/*
 * Copyright 2015-2018 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include "./bits.h"
#include "./debug.h"
#include "./proto.h"
#include "./ualb.h"

#define page_extra(fmt, ...) log_extra(MDBX_LOG_PAGE, fmt, ##__VA_ARGS__)
#define page_trace(fmt, ...) log_trace(MDBX_LOG_PAGE, fmt, ##__VA_ARGS__)
#define page_verbose(fmt, ...) log_verbose(MDBX_LOG_PAGE, fmt, ##__VA_ARGS__)
#define page_info(fmt, ...) log_info(MDBX_LOG_PAGE, fmt, ##__VA_ARGS__)
#define page_notice(fmt, ...) log_notice(MDBX_LOG_PAGE, fmt, ##__VA_ARGS__)
#define page_warning(fmt, ...) log_warning(MDBX_LOG_PAGE, fmt, ##__VA_ARGS__)
#define page_error(fmt, ...) log_error(MDBX_LOG_PAGE, fmt, ##__VA_ARGS__)
#define page_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_PAGE, __func__, __LINE__, "%s, error %d", msg, err)

/*----------------------------------------------------------------------------*/

/* Allocate memory for a page.
 * Re-use old malloc'd pages first for singletons, otherwise just malloc.
 * Set MDBX_TXN_ERROR on failure. */
static page_t *page_malloc(MDBX_txn_t *txn, unsigned num) {
  MDBX_env_t *env = txn->mt_env;
  page_t *np = env->me_dpages;
  size_t size = env->me_psize;
  if (likely(num == 1 && np)) {
    ASAN_UNPOISON_MEMORY_REGION(np, size);
    VALGRIND_MEMPOOL_ALLOC(env, np, size);
    VALGRIND_MAKE_MEM_DEFINED(&np->mp_next, sizeof(np->mp_next));
    env->me_dpages = np->mp_next;
  } else {
    size = pgno2bytes(env, num);
    np = malloc(size);
    if (unlikely(!np)) {
      txn->mt_flags |= MDBX_TXN_ERROR;
      return np;
    }
    VALGRIND_MEMPOOL_ALLOC(env, np, size);
  }

  if ((env->me_flags32 & MDBX_NOMEMINIT) == 0) {
    /* For a single page alloc, we init everything after the page header.
     * For multi-page, we init the final page; if the caller needed that
     * many pages they will be filling in at least up to the last page. */
    size_t skip = PAGEHDRSZ;
    if (num > 1)
      skip += pgno2bytes(env, num - 1);
    memset((char *)np + skip, 0, size - skip);
  }
#if MDBX_DEBUG
  np->mp_pgno = 0;
#endif
  VALGRIND_MAKE_MEM_UNDEFINED_ERASE(np, size);
  np->mp_flags16 = 0;
  np->mp_pages = num;
  return np;
}

/* Free a dirty page.
 * Saves singlev (not multi-page overflow) pages to a list, for future reuse. */
static void dpage_free(MDBX_env_t *env, page_t *dp) {
#if MDBX_DEBUG
  dp->mp_pgno = MAX_PAGENO;
#endif
  if (!IS_OVERFLOW(dp) || dp->mp_pages == 1) {
    dp->mp_next = env->me_dpages;
    VALGRIND_MEMPOOL_FREE(env, dp);
    env->me_dpages = dp;
  } else {
    /* large pages just get freed directly */
    VALGRIND_MEMPOOL_FREE(env, dp);
    free(dp);
  }
}

/* Return all dirty pages to dpage list */
static void dlist_free(MDBX_txn_t *txn) {
  MDBX_env_t *env = txn->mt_env;
  MDBX_ID2L dl = txn->mt_rw_dirtylist;
  size_t i, n = dl[0].mid;

  for (i = 1; i <= n; i++)
    dpage_free(env, dl[i].mptr);

  dl[0].mid = 0;
}

static void __cold kill_page(MDBX_env_t *env, pgno_t pgno) {
  const size_t offs = pgno2bytes(env, pgno);
  const size_t shift = offsetof(page_t, mp_pages);

  if (env->me_flags32 & MDBX_WRITEMAP) {
    page_t *mp = (page_t *)(env->me_map + offs);
    memset(&mp->mp_pages, 0x6F /* 'o', 111 */, env->me_psize - shift);
    VALGRIND_MAKE_MEM_NOACCESS(&mp->mp_pages, env->me_psize - shift);
    ASAN_POISON_MEMORY_REGION(&mp->mp_pages, env->me_psize - shift);
  } else {
    intptr_t len = env->me_psize - shift;
    void *buf = alloca(len);
    memset(buf, 0x6F /* 'o', 111 */, len);
    (void)mdbx_pwrite(env->me_dxb_fd, buf, len, offs + shift);
  }
}

/* Loosen or free a single page.
 *
 * Saves single pages to a list for future reuse
 * in this same txn. It has been pulled from the GACO
 * and already resides on the dirty list, but has been
 * deleted. Use these pages first before pulling again
 * from the GACO.
 *
 * If the page wasn't dirtied in this txn, just add it
 * to this txn's free list. */
static int page_loose(cursor_t *mc, page_t *mp) {
  int loose = 0;
  const pgno_t pgno = mp->mp_pgno;
  MDBX_txn_t *txn = mc->mc_txn;

  if ((mp->mp_flags16 & P_DIRTY) && mc->mc_aht != mc->mc_txn->txn_aht_array) {
    if (txn->mt_parent) {
      mdbx_assert(txn->mt_env, (txn->mt_env->me_flags32 & MDBX_WRITEMAP) == 0);
      MDBX_ID2 *dl = txn->mt_rw_dirtylist;
      /* If txn has a parent,
       * make sure the page is in our dirty list. */
      if (dl[0].mid) {
        unsigned x = mdbx_mid2l_search(dl, pgno);
        if (x <= dl[0].mid && dl[x].mid == pgno) {
          if (unlikely(mp != dl[x].mptr)) { /* bad cursor? */
            mdbx_error("wrong page 0x%p #%" PRIaPGNO " in the dirtylist[%d], expecting %p", dl[x].mptr, pgno,
                       x, mp);
            mc->mc_state8 &= ~(C_INITIALIZED | C_EOF);
            txn->mt_flags |= MDBX_TXN_ERROR;
            return MDBX_PROBLEM;
          }
          /* ok, it's ours */
          loose = 1;
        }
      }
    } else {
      /* no parent txn, so it's just ours */
      loose = 1;
    }
  }
  if (loose) {
    mdbx_debug("loosen db %d page %" PRIaPGNO, DAAH(mc), mp->mp_pgno);
    page_t **link = &NEXT_LOOSE_PAGE(mp);
    if (unlikely(txn->mt_env->me_flags32 & MDBX_PAGEPERTURB)) {
      kill_page(txn->mt_env, pgno);
      ASAN_UNPOISON_MEMORY_REGION(link, sizeof(page_t *));
      VALGRIND_MAKE_MEM_UNDEFINED_ERASE(link, sizeof(page_t *));
    }
    *link = txn->mt_loose_pages;
    txn->mt_loose_pages = mp;
    txn->mt_loose_count++;
    mp->mp_flags16 |= P_LOOSE;
  } else {
    int rc = mdbx_pnl_append(&txn->mt_befree_pages, pgno);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  return MDBX_SUCCESS;
}

/* Set or clear P_KEEP in dirty, non-overflow, non-sub pages watched by txn.
 *
 * [in] pflags  Flags of the pages to update:
 *                - P_DIRTY to set P_KEEP,
 *                - P_DIRTY|P_KEEP to clear it.
 * [in] all     No shortcuts. Needed except after a full page_flush().
 *
 * Returns 0 on success, non-zero on failure. */

static inline void page_toogle_keep(page_t *page, const unsigned match_flags) {
  const unsigned MatchMask = P_SUBP | P_DIRTY | P_LOOSE | P_KEEP;
  if ((page->mp_flags16 & MatchMask) == match_flags)
    page->mp_flags16 ^= P_KEEP;
}

static void cursor_toogle_keep(cursor_t *cursor, unsigned pageflags_match) {
tail_recursion:
  if (cursor->mc_state8 & C_INITIALIZED) {
    page_t *page = nullptr;
    for (unsigned i = 0; i < cursor->mc_snum; ++i) {
      page = cursor->mc_pg[i];
      page_toogle_keep(page, pageflags_match);
    }

    if ((cursor->mc_kind8 & S_HAVESUB) && page && (page->mp_flags16 & P_LEAF)) {
      node_t *leaf = node_ptr(page, cursor->mc_ki[cursor->mc_snum - 1]);
      /* Proceed to nested if it is at a sub-database */
      if (leaf->node_flags8 & NODE_SUBTREE) {
        cursor = cursor_nested(cursor);
        goto tail_recursion;
      }
    }
  }
}

static int pages_xkeep(MDBX_txn_t *txn, unsigned pageflags_match, bool all) {
  for (unsigned i = 0; i < txn->txn_ah_num; ++i) {
    for (MDBX_cursor_t *mc = txn->mt_cursors[i]; mc != nullptr; mc = mc->mc_next)
      cursor_toogle_keep(&mc->primal, pageflags_match);
  }

  if (all) {
    /* Mark dirty root pages */
    aht_t *const end = &txn->txn_aht_array[txn->txn_ah_num];
    for (aht_t *aht = txn->txn_aht_array; aht < end; ++aht) {
      if ((aht->ah.state8 & MDBX_AAH_DIRTY) && aht->aa.root != P_INVALID) {
        int level;
        page_t *dp;
        int rc = page_get(txn, aht->aa.root, &dp, &level);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
        if (level <= 1)
          page_toogle_keep(dp, pageflags_match);
      }
    }
  }

  return MDBX_SUCCESS;
}

/* Add a page to the txn's dirty list */
static void mdbx_page_dirty(MDBX_txn_t *txn, page_t *mp) {
  MDBX_ID2 mid;
  int rc, (*insert)(MDBX_ID2L, MDBX_ID2 *);

  if (txn->mt_flags & MDBX_WRITEMAP) {
    insert = mdbx_mid2l_append;
  } else {
    insert = mdbx_mid2l_insert;
  }
  mid.mid = mp->mp_pgno;
  mid.mptr = mp;
  rc = insert(txn->mt_rw_dirtylist, &mid);
  assert(rc == 0);
  (void)rc;
  txn->mt_dirtyroom--;
}

/* Spill pages from the dirty list back to disk.
 * This is intended to prevent running into MDBX_TXN_FULL situations,
 * but note that they may still occur in a few cases:
 *
 * 1) our estimate of the txn size could be too small. Currently this
 *  seems unlikely, except with a large number of MDBX_IUD_MULTIPLE items.
 *
 * 2) child txns may run out of space if their parents dirtied a
 *  lot of pages and never spilled them. TODO: we probably should do
 *  a preemptive spill during mdbx_begin() of a child txn, if
 *  the parent's dirtyroom is below a given threshold.
 *
 * Otherwise, if not using nested txns, it is expected that apps will
 * not run into MDBX_TXN_FULL any more. The pages are flushed to disk
 * the same way as for a txn commit, e.g. their P_DIRTY flag is cleared.
 * If the txn never references them again, they can be left alone.
 * If the txn only reads them, they can be used without any fuss.
 * If the txn writes them again, they can be dirtied immediately without
 * going thru all of the work of page_touch(). Such references are
 * handled by page_unspill().
 *
 * Also note, we never spill AA root pages, nor pages of active cursors,
 * because we'll need these back again soon anyway. And in nested txns,
 * we can't spill a page in a child txn if it was already spilled in a
 * parent txn. That would alter the parent txns' data even though
 * the child hasn't committed yet, and we'd have no way to undo it if
 * the child aborted.
 *
 * [in] m0    cursor A cursor handle identifying the transaction and
 *            database for which we are checking space.
 * [in] key   For a put operation, the key being stored.
 * [in] data  For a put operation, the data being stored.
 *
 * Returns 0 on success, non-zero on failure. */
static int page_spill(cursor_t *cursor, MDBX_iov_t *key, MDBX_iov_t *data) {
  if (cursor->mc_kind8 & S_SUBCURSOR)
    return MDBX_SUCCESS;

  MDBX_txn_t *txn = cursor->mc_txn;
  /* Estimate how much space this op will take */
  pgno_t need = cursor->mc_aht->aa.depth16 + aht_gaco(txn)->aa.depth16;
  /* Named AAs also dirty the main AA */
  if ((cursor->mc_aht->aa.flags16 & (MDBX_AAH_GACO | MDBX_AAH_MAIN)) == 0)
    need += aht_main(txn)->aa.depth16;
  /* For puts, roughly factor in the key+data size */
  if (key) {
    size_t sz = LEAFSIZE(key, data);
    if (sz > txn->mt_env->me_nodemax)
      /* put requires an overflow page */
      sz += sizeof(pgno_t) + PAGEHDRSZ + txn->mt_env->me_psize - 1;
    need += bytes2pgno(txn->mt_env, sz);
  }

  if (likely(txn->mt_dirtyroom > need))
    return MDBX_SUCCESS;

  if (!txn->mt_spill_pages) {
    txn->mt_spill_pages = mdbx_pnl_alloc(MDBX_PNL_UM_MAX);
    if (unlikely(!txn->mt_spill_pages))
      return MDBX_ENOMEM;
  } else {
    /* purge deleted slots */
    MDBX_PNL sl = txn->mt_spill_pages;
    pgno_t num = sl[0], j = 0;
    for (unsigned i = 1; i <= num; i++) {
      if (!(sl[i] & 1))
        sl[++j] = sl[i];
    }
    sl[0] = j;
  }

  /* Preserve pages which may soon be dirtied again */
  int rc = pages_xkeep(txn, P_DIRTY, true);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  /* Less aggressive spill - we originally spilled the entire dirty list,
   * with a few exceptions for cursor pages and AA root pages. But this
   * turns out to be a lot of wasted effort because in a large txn many
   * of those pages will need to be used again. So now we spill only 1/8th
   * of the dirty pages. Testing revealed this to be a good tradeoff,
   * better than 1/2, 1/4, or 1/10. */
  need += need; /* double it for good measure */
  if (need < MDBX_PNL_UM_MAX / 8)
    need = MDBX_PNL_UM_MAX / 8;

  /* Save the page IDs of all the pages we're flushing */
  /* flush from the tail forward, this saves a lot of shifting later on. */
  MDBX_ID2L dl = txn->mt_rw_dirtylist;
  unsigned left_dirty;
  for (left_dirty = dl[0].mid; left_dirty && need; left_dirty--) {
    const pgno_t pgno2 = dl[left_dirty].mid << 1;
    page_t *dp = dl[left_dirty].mptr;
    if (dp->mp_flags16 & (P_LOOSE | P_KEEP))
      continue;
    /* Can't spill twice,
     * make sure it's not already in a parent's spill list. */
    if (txn->mt_parent) {
      MDBX_txn_t *tx2;
      for (tx2 = txn->mt_parent; tx2; tx2 = tx2->mt_parent) {
        if (tx2->mt_spill_pages) {
          size_t pos = mdbx_pnl_search(tx2->mt_spill_pages, pgno2);
          if (pos <= tx2->mt_spill_pages[0] && tx2->mt_spill_pages[pos] == pgno2) {
            dp->mp_flags16 |= P_KEEP;
            break;
          }
        }
      }
      if (tx2)
        continue;
    }
    rc = mdbx_pnl_append(&txn->mt_spill_pages, pgno2);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
    need--;
  }
  mdbx_pnl_sort(txn->mt_spill_pages);

  /* Flush the spilled part of dirty list */
  rc = page_flush(txn, left_dirty);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  /* Reset any dirty pages we kept that page_flush didn't see */
  rc = pages_xkeep(txn, P_DIRTY | P_KEEP, left_dirty != 0);

bailout:
  txn->mt_flags |= rc ? MDBX_TXN_ERROR : MDBX_TXN_SPILLS;
  return rc;
}

/* Copy the used portions of a non-overflow page.
 * [in] dst page to copy into
 * [in] src page to copy from
 * [in] psize size of a page */
static void page_copy(page_t *dst, page_t *src, unsigned psize) {
  STATIC_ASSERT(UINT16_MAX > MAX_PAGESIZE - PAGEHDRSZ);
  STATIC_ASSERT(MIN_PAGESIZE > PAGEHDRSZ + NODESIZE * 42);
  enum { Align = sizeof(pgno_t) };
  indx_t upper = src->mp_upper, lower = src->mp_lower, unused = upper - lower;

  /* If page isn't full, just copy the used portion. Adjust
   * alignment so memcpy may copy words instead of bytes. */
  if ((unused &= -Align) && !IS_DFL(src)) {
    upper = (upper + PAGEHDRSZ) & -Align;
    memcpy(dst, src, (lower + PAGEHDRSZ + (Align - 1)) & -Align);
    memcpy((pgno_t *)((char *)dst + upper), (pgno_t *)((char *)src + upper), psize - upper);
  } else {
    memcpy(dst, src, psize - unused);
  }
}

/* Pull a page off the txn's spill list, if present.
 *
 * If a page being referenced was spilled to disk in this txn, bring
 * it back and make it dirty/writable again.
 *
 * [in] txn   the transaction handle.
 * [in] mp    the page being referenced. It must not be dirty.
 * [out] ret  the writable page, if any.
 *            ret is unchanged if mp wasn't spilled. */
static int page_unspill(MDBX_txn_t *txn, page_t *mp, page_t **ret) {
  MDBX_env_t *env = txn->mt_env;
  const pgno_t pgno = mp->mp_pgno, pgno2 = pgno << 1;

  for (const MDBX_txn_t *tx2 = txn; tx2; tx2 = tx2->mt_parent) {
    if (!tx2->mt_spill_pages)
      continue;
    size_t pos = mdbx_pnl_search(tx2->mt_spill_pages, pgno2);
    if (pos <= tx2->mt_spill_pages[0] && tx2->mt_spill_pages[pos] == pgno2) {
      if (txn->mt_dirtyroom == 0)
        return MDBX_TXN_FULL;
      int num = IS_OVERFLOW(mp) ? mp->mp_pages : 1;
      page_t *np;
      if (env->me_flags32 & MDBX_WRITEMAP) {
        np = mp;
      } else {
        np = page_malloc(txn, num);
        if (unlikely(!np))
          return MDBX_ENOMEM;
        if (unlikely(num > 1))
          memcpy(np, mp, pgno2bytes(env, num));
        else
          page_copy(np, mp, env->me_psize);
      }
      mdbx_debug("unspill page %" PRIaPGNO, mp->mp_pgno);
      if (tx2 == txn) {
        /* If in current txn, this page is no longer spilled.
         * If it happens to be the last page, truncate the spill list.
         * Otherwise mark it as deleted by setting the LSB. */
        if (pos == txn->mt_spill_pages[0])
          txn->mt_spill_pages[0]--;
        else
          txn->mt_spill_pages[pos] |= 1;
      } /* otherwise, if belonging to a parent txn, the
         * page remains spilled until child commits */

      mdbx_page_dirty(txn, np);
      np->mp_flags16 |= P_DIRTY;
      *ret = np;
      break;
    }
  }
  return MDBX_SUCCESS;
}

/* Flush (some) dirty pages to the map, after clearing their dirty flag.
 * [in] txn   the transaction that's being committed
 * [in] keep  number of initial pages in dirtylist to keep dirty.
 * Returns 0 on success, non-zero on failure. */
static int page_flush(MDBX_txn_t *txn, pgno_t keep) {
  MDBX_env_t *env = txn->mt_env;
  MDBX_ID2L dl = txn->mt_rw_dirtylist;
  unsigned i, j, pagecount = dl[0].mid;
  int rc;
  size_t size = 0, pos = 0;
  pgno_t pgno = 0;
  page_t *dp = nullptr;
  struct iovec iov[MDBX_COMMIT_PAGES];
  intptr_t wpos = 0, wsize = 0;
  size_t next_pos = 1; /* impossible pos, so pos != next_pos */
  int n = 0;

  j = i = keep;

  if (env->me_flags32 & MDBX_WRITEMAP) {
    /* Clear dirty flags */
    while (++i <= pagecount) {
      dp = dl[i].mptr;
      /* Don't flush this page yet */
      if (dp->mp_flags16 & (P_LOOSE | P_KEEP)) {
        dp->mp_flags16 &= ~P_KEEP;
        dl[++j] = dl[i];
        continue;
      }
      dp->mp_flags16 &= ~P_DIRTY;
      dp->page_checksum = 0 /* TODO */;
      env->me_lck->li_dirty_volume += IS_OVERFLOW(dp) ? pgno2bytes(env, dp->mp_pages) : env->me_psize;
    }
    goto done;
  }

  /* Write the pages */
  for (;;) {
    if (++i <= pagecount) {
      dp = dl[i].mptr;
      /* Don't flush this page yet */
      if (dp->mp_flags16 & (P_LOOSE | P_KEEP)) {
        dp->mp_flags16 &= ~P_KEEP;
        dl[i].mid = 0;
        continue;
      }
      pgno = dl[i].mid;
      /* clear dirty flag */
      dp->mp_flags16 &= ~P_DIRTY;
      dp->page_checksum = 0 /* TODO */;
      pos = pgno2bytes(env, pgno);
      size = IS_OVERFLOW(dp) ? pgno2bytes(env, dp->mp_pages) : env->me_psize;
      env->me_lck->li_dirty_volume += size;
    }
    /* Write up to MDBX_COMMIT_PAGES dirty pages at a time. */
    if (pos != next_pos || n == MDBX_COMMIT_PAGES || wsize + size > MAX_WRITE) {
      if (n) {
        /* Write previous page(s) */
        rc = mdbx_pwritev(env->me_dxb_fd, iov, n, wpos, wsize);
        if (unlikely(rc != MDBX_SUCCESS)) {
          mdbx_debug("Write error: %s", strerror(rc));
          return rc;
        }
        n = 0;
      }
      if (i > pagecount)
        break;
      wpos = pos;
      wsize = 0;
    }
    mdbx_debug("committing page %" PRIaPGNO "", pgno);
    next_pos = pos + size;
    iov[n].iov_len = size;
    iov[n].iov_base = (char *)dp;
    wsize += size;
    n++;
  }

  mdbx_invalidate_cache(env->me_map, pgno2bytes(env, txn->mt_next_pgno));

  for (i = keep; ++i <= pagecount;) {
    dp = dl[i].mptr;
    /* This is a page we skipped above */
    if (!dl[i].mid) {
      dl[++j] = dl[i];
      dl[j].mid = dp->mp_pgno;
      continue;
    }
    dpage_free(env, dp);
  }

done:
  i--;
  txn->mt_dirtyroom += i - j;
  dl[0].mid = j;
  return MDBX_SUCCESS;
}
