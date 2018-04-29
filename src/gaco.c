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

#define gaco_extra(fmt, ...) log_extra(MDBX_LOG_GACO, fmt, ##__VA_ARGS__)
#define gaco_trace(fmt, ...) log_trace(MDBX_LOG_GACO, fmt, ##__VA_ARGS__)
#define gaco_verbose(fmt, ...) log_verbose(MDBX_LOG_GACO, fmt, ##__VA_ARGS__)
#define gaco_info(fmt, ...) log_info(MDBX_LOG_GACO, fmt, ##__VA_ARGS__)
#define gaco_notice(fmt, ...) log_notice(MDBX_LOG_GACO, fmt, ##__VA_ARGS__)
#define gaco_warning(fmt, ...) log_warning(MDBX_LOG_GACO, fmt, ##__VA_ARGS__)
#define gaco_error(fmt, ...) log_error(MDBX_LOG_GACO, fmt, ##__VA_ARGS__)
#define gaco_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_GACO, __func__, __LINE__, "%s, error %d", msg, err)

/*----------------------------------------------------------------------------*/

/* Allocate page numbers and memory for writing.  Maintain me_last_reclaimed,
 * me_reclaimed_pglist and mt_next_pgno.  Set MDBX_TXN_ERROR on failure.
 *
 * If there are free pages available from older transactions, they
 * are re-used first. Otherwise allocate a new page at mt_next_pgno.
 * Do not modify the GACO, just merge GACO records into me_reclaimed_pglist
 * and move me_last_reclaimed to say which records were consumed.  Only this
 * function can create me_reclaimed_pglist and move
 * me_last_reclaimed/mt_next_pgno.
 *
 * [in] mc    cursor A cursor handle identifying the transaction and
 *            database for which we are allocating.
 * [in] num   the number of pages to allocate.
 * [out] mp   Address of the allocated page(s). Requests for multiple pages
 *            will always be satisfied by a single contiguous chunk of memory.
 *
 * Returns 0 on success, non-zero on failure.*/

static int page_alloc(cursor_t *mc, unsigned num, page_t **mp, int flags) {
  int rc;
  MDBX_txn_t *txn = mc->mc_txn;
  MDBX_env_t *env = txn->mt_env;
  page_t *np;

  if (likely(flags & MDBX_ALLOC_GC)) {
    STATIC_ASSERT((MDBX_ALLOC_ALL & (MDBX_COALESCE | MDBX_LIFORECLAIM)) == 0);
    flags |= env->me_flags32 & (MDBX_COALESCE | MDBX_LIFORECLAIM);
    if (unlikely(mc->mc_state8 & C_RECLAIMING)) {
      /* If mc is updating the GACO, then the freelist cannot play
       * catch-up with itself by growing while trying to save it. */
      flags &= ~(MDBX_ALLOC_GC | MDBX_ALLOC_KICK | MDBX_COALESCE | MDBX_LIFORECLAIM);
    } else if (unlikely(aht_gaco(txn)->aa.entries == 0)) {
      /* avoid (recursive) search inside empty tree and while tree is updating,
       * https://github.com/leo-yuriev/libmdbx/issues/31 */
      flags &= ~MDBX_ALLOC_GC;
    }
  }

  if (likely(flags & MDBX_ALLOC_CACHE)) {
    /* If there are any loose pages, just use them */
    assert(mp && num);
    if (likely(num == 1 && txn->mt_loose_pages)) {
      np = txn->mt_loose_pages;
      txn->mt_loose_pages = NEXT_LOOSE_PAGE(np);
      txn->mt_loose_count--;
      mdbx_debug("db %d use loose page %" PRIaPGNO, DAAH(mc), np->mp_pgno);
      ASAN_UNPOISON_MEMORY_REGION(np, env->me_psize);
      assert(np->mp_pgno < txn->mt_next_pgno);
      mdbx_ensure(env, np->mp_pgno >= NUM_METAS);
      *mp = np;
      return MDBX_SUCCESS;
    }
  }

  assert(mdbx_pnl_check(env->me_reclaimed_pglist));
  pgno_t pgno, *repg_list = env->me_reclaimed_pglist;
  unsigned repg_pos = 0, repg_len = repg_list ? repg_list[0] : 0;
  txnid_t oldest = 0, last = 0;
  const unsigned wanna_range = num - 1;

  while (1) { /* RBR-kick retry loop */
    /* If our dirty list is already full, we can't do anything */
    if (unlikely(txn->mt_dirtyroom == 0)) {
      rc = MDBX_TXN_FULL;
      goto fail;
    }

    MDBX_cursor_t recur;
    for (MDBX_cursor_op_t op = MDBX_FIRST;; op = (flags & MDBX_LIFORECLAIM) ? MDBX_PREV : MDBX_NEXT) {
      MDBX_iov_t key, data;

      /* Seek a big enough contiguous page range.
       * Prefer pages with lower pgno. */
      assert(mdbx_pnl_check(env->me_reclaimed_pglist));
      if (likely(flags & MDBX_ALLOC_CACHE) && repg_len > wanna_range &&
          (!(flags & MDBX_COALESCE) || op == MDBX_FIRST)) {
#if MDBX_PNL_ASCENDING
        for (repg_pos = 1; repg_pos <= repg_len - wanna_range; ++repg_pos) {
          pgno = repg_list[repg_pos];
          if (likely(repg_list[repg_pos + wanna_range - 1] == pgno + wanna_range - 1))
            goto done;
        }
#else
        repg_pos = repg_len;
        do {
          pgno = repg_list[repg_pos];
          if (likely(repg_list[repg_pos - wanna_range] == pgno + wanna_range))
            goto done;
        } while (--repg_pos > wanna_range);
#endif /* MDBX_PNL sort-order */
      }

      if (op == MDBX_FIRST) { /* 1st iteration, setup cursor, etc */
        if (unlikely(!(flags & MDBX_ALLOC_GC)))
          break /* reclaiming is prohibited for now */;

        /* Prepare to fetch more and coalesce */
        oldest = (flags & MDBX_LIFORECLAIM) ? find_oldest(txn) : env->me_oldest[0];
        rc = cursor_init(&recur, txn, aht_gaco(txn));
        if (unlikely(rc != MDBX_SUCCESS))
          break;
        if (flags & MDBX_LIFORECLAIM) {
          /* Begin from oldest reader if any */
          if (oldest > 2) {
            last = oldest - 1;
            op = MDBX_SET_RANGE;
          }
        } else if (env->me_last_reclaimed) {
          /* Continue lookup from env->me_last_reclaimed to oldest reader */
          last = env->me_last_reclaimed;
          op = MDBX_SET_RANGE;
        }

        key.iov_base = &last;
        key.iov_len = sizeof(last);
      }

      if (!(flags & MDBX_LIFORECLAIM)) {
        /* Do not try fetch more if the record will be too recent */
        if (op != MDBX_FIRST && ++last >= oldest) {
          oldest = find_oldest(txn);
          if (oldest <= last)
            break;
        }
      }

      rc = mdbx_cursor_get(&recur, &key, nullptr, op);
      if (rc == MDBX_NOTFOUND && (flags & MDBX_LIFORECLAIM)) {
        if (op == MDBX_SET_RANGE)
          continue;
        if (oldest < find_oldest(txn)) {
          oldest = *env->me_oldest;
          last = oldest - 1;
          key.iov_base = &last;
          key.iov_len = sizeof(last);
          op = MDBX_SET_RANGE;
          rc = mdbx_cursor_get(&recur, &key, nullptr, op);
        }
      }
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (rc == MDBX_NOTFOUND)
          break;
        goto fail;
      }

      last = *(txnid_t *)key.iov_base;
      if (oldest <= last) {
        oldest = find_oldest(txn);
        if (oldest <= last) {
          if (flags & MDBX_LIFORECLAIM)
            continue;
          break;
        }
      }

      if (flags & MDBX_LIFORECLAIM) {
        /* skip IDs of records that already reclaimed */
        if (txn->mt_lifo_reclaimed) {
          unsigned i;
          for (i = (unsigned)txn->mt_lifo_reclaimed[0]; i > 0; --i)
            if (txn->mt_lifo_reclaimed[i] == last)
              break;
          if (i)
            continue;
        }
      }

      /* Reading next GACO record */
      np = recur.primal.mc_pg[recur.primal.mc_top];
      node_t *leaf = node_ptr(np, recur.primal.mc_ki[recur.primal.mc_top]);
      if (unlikely((rc = node_read(&recur.primal, leaf, &data)) != MDBX_SUCCESS))
        goto fail;

      if ((flags & MDBX_LIFORECLAIM) && !txn->mt_lifo_reclaimed) {
        txn->mt_lifo_reclaimed = mdbx_txl_alloc();
        if (unlikely(!txn->mt_lifo_reclaimed)) {
          rc = MDBX_ENOMEM;
          goto fail;
        }
      }

      /* Append PNL from GACO record to me_reclaimed_pglist */
      pgno_t *re_pnl = (pgno_t *)data.iov_base;
      assert(re_pnl[0] == 0 || data.iov_len == (re_pnl[0] + 1) * sizeof(pgno_t));
      assert(mdbx_pnl_check(re_pnl));
      repg_pos = re_pnl[0];
      if (!repg_list) {
        repg_list = env->me_reclaimed_pglist = mdbx_pnl_alloc(repg_pos);
        if (unlikely(!repg_list)) {
          rc = MDBX_ENOMEM;
          goto fail;
        }
      } else {
        rc = mdbx_pnl_need(&env->me_reclaimed_pglist, repg_pos);
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
        repg_list = env->me_reclaimed_pglist;
      }

      /* Remember ID of GACO record */
      if (flags & MDBX_LIFORECLAIM) {
        rc = mdbx_txl_append(&txn->mt_lifo_reclaimed, last);
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
      }
      env->me_last_reclaimed = last;

      if (mdbx_dbglog_enabled(MDBX_LOG_GACO, MDBX_LOGLEVEL_VERBOSE)) {
        dbglog_begin(MDBX_LOG_GACO, MDBX_LOGLEVEL_VERBOSE);

        dbglog_continue("PNL read txn %" PRIaTXN " root %" PRIaPGNO " num %u [", last, aht_gaco(txn)->aa.root,
                        repg_pos);
        for (unsigned i = repg_pos; i; i--)
          dbglog_continue(" %" PRIaPGNO, re_pnl[i]);
        dbglog_end(" ]\n");
      }

      /* Merge in descending sorted order */
      mdbx_pnl_xmerge(repg_list, re_pnl);
      repg_len = repg_list[0];
      if (unlikely((flags & MDBX_ALLOC_CACHE) == 0)) {
        /* Done for a kick-reclaim mode, actually no page needed */
        return MDBX_SUCCESS;
      }

      assert(repg_len == 0 || repg_list[repg_len] < txn->mt_next_pgno);
      if (repg_len) {
        /* Refund suitable pages into "unallocated" space */
        pgno_t tail = txn->mt_next_pgno;
        pgno_t *const begin = repg_list + 1;
        pgno_t *const end = begin + repg_len;
        pgno_t *higest;
#if MDBX_PNL_ASCENDING
        for (higest = end; --higest >= begin;) {
#else
        for (higest = begin; higest < end; ++higest) {
#endif /* MDBX_PNL sort-order */
          assert(*higest >= NUM_METAS && *higest < tail);
          if (*higest != tail - 1)
            break;
          tail -= 1;
        }
        if (tail != txn->mt_next_pgno) {
#if MDBX_PNL_ASCENDING
          repg_len = (unsigned)(higest + 1 - begin);
#else
          repg_len -= (unsigned)(higest - begin);
          for (pgno_t *move = begin; higest < end; ++move, ++higest)
            *move = *higest;
#endif /* MDBX_PNL sort-order */
          repg_list[0] = repg_len;
          gaco_info("refunded %" PRIaPGNO " pages: %" PRIaPGNO " -> %" PRIaPGNO, tail - txn->mt_next_pgno,
                    tail, txn->mt_next_pgno);
          txn->mt_next_pgno = tail;
          assert(mdbx_pnl_check(env->me_reclaimed_pglist));
        }
      }

      /* Don't try to coalesce too much. */
      if (repg_len > MDBX_PNL_UM_SIZE / 2)
        break;
      if (flags & MDBX_COALESCE) {
        if (repg_len /* current size */ >= env->me_maxfree_1pg / 2 ||
            repg_pos /* prev size */ >= env->me_maxfree_1pg / 4)
          flags &= ~MDBX_COALESCE;
      }
    }

    if ((flags & (MDBX_COALESCE | MDBX_ALLOC_CACHE)) == (MDBX_COALESCE | MDBX_ALLOC_CACHE) &&
        repg_len > wanna_range) {
#if MDBX_PNL_ASCENDING
      for (repg_pos = 1; repg_pos <= repg_len - wanna_range; ++repg_pos) {
        pgno = repg_list[repg_pos];
        if (likely(repg_list[repg_pos + wanna_range - 1] == pgno + wanna_range - 1))
          goto done;
      }
#else
      repg_pos = repg_len;
      do {
        pgno = repg_list[repg_pos];
        if (likely(repg_list[repg_pos - wanna_range] == pgno + wanna_range))
          goto done;
      } while (--repg_pos > wanna_range);
#endif /* MDBX_PNL sort-order */
    }

    /* Use new pages from the map when nothing suitable in the GACO */
    repg_pos = 0;
    pgno = txn->mt_next_pgno;
    rc = MDBX_MAP_FULL;
    const pgno_t next = pgno_add(pgno, num);
    if (likely(next <= txn->mt_end_pgno)) {
      rc = MDBX_NOTFOUND;
      if (likely(flags & MDBX_ALLOC_NEW))
        goto done;
    }

    const meta_t *head = meta_head(env);
    if ((flags & MDBX_ALLOC_GC) && ((flags & MDBX_ALLOC_KICK) || rc == MDBX_MAP_FULL)) {
      meta_t *steady = meta_steady(env);

      if (oldest == meta_txnid_stable(env, steady) && !META_IS_STEADY(head) && META_IS_STEADY(steady)) {
        /* LY: Here an RBR was happened:
         *  - all pages had allocated;
         *  - reclaiming was stopped at the last steady-sync;
         *  - the head-sync is weak.
         * Now we need make a sync to resume reclaiming. If both
         * MDBX_NOSYNC and MDBX_MAPASYNC flags are set, then assume that
         * utterly no-sync write mode was requested. In such case
         * don't make a steady-sync, but only a legacy-mode checkpoint,
         * just for resume reclaiming only, not for data consistency. */

        mdbx_debug("kick-gc: head %" PRIaTXN "-%s, tail %" PRIaTXN "-%s, oldest %" PRIaTXN "",
                   meta_txnid_stable(env, head), durable_str(head), meta_txnid_stable(env, steady),
                   durable_str(steady), oldest);

        const unsigned syncflags =
            F_ISSET(env->me_flags32, MDBX_UTTERLY_NOSYNC) ? env->me_flags32 : env->me_flags32 & MDBX_WRITEMAP;
        meta_t meta = *head;
        if (mdbx_sync_locked(env, syncflags, &meta) == MDBX_SUCCESS) {
          txnid_t snap = find_oldest(txn);
          if (snap > oldest)
            continue;
        }
      }

      if (rc == MDBX_MAP_FULL && oldest < txn->mt_txnid - 1) {
        if (rbr(env, oldest) > oldest)
          continue;
      }
    }

    if (rc == MDBX_MAP_FULL && next < head->mm_geo.upper) {
      mdbx_assert(env, next > txn->mt_end_pgno);
      pgno_t aligned =
          pgno_align2os_pgno(env, pgno_add(next, head->mm_geo.grow16 - next % head->mm_geo.grow16));

      if (aligned > head->mm_geo.upper)
        aligned = head->mm_geo.upper;
      mdbx_assert(env, aligned > txn->mt_end_pgno);

      gaco_info("try growth datafile to %" PRIaPGNO " pages (+%" PRIaPGNO ")", aligned,
                aligned - txn->mt_end_pgno);
      rc = mdbx_mapresize(env, aligned, head->mm_geo.upper);
      if (rc == MDBX_SUCCESS) {
        assert(txn->mt_end_pgno >= next);
        if (!mp)
          return rc;
        goto done;
      }

      gaco_warning("unable growth datafile to %" PRIaPGNO "pages (+%" PRIaPGNO "), errcode %d", aligned,
                   aligned - txn->mt_end_pgno, rc);
    }

  fail:
    assert(mdbx_pnl_check(env->me_reclaimed_pglist));
    if (mp) {
      *mp = nullptr;
      txn->mt_flags |= MDBX_TXN_ERROR;
    }
    assert(rc);
    return rc;
  }

done:
  assert(mp && num);
  assert(pgno >= NUM_METAS);
  mdbx_ensure(env, pgno >= NUM_METAS);
  if (unlikely(pgno < NUM_METAS)) {
    rc = MDBX_PANIC;
    goto fail;
  }

  if (env->me_flags32 & MDBX_WRITEMAP) {
    np = pgno2page(env, pgno);
    /* LY: reset no-access flag from kill_page() */
    ASAN_UNPOISON_MEMORY_REGION(np, pgno2bytes(env, num));
    VALGRIND_MAKE_MEM_UNDEFINED_ERASE(np, pgno2bytes(env, num));
  } else {
    np = page_malloc(txn, num);
    if (unlikely(!np)) {
      rc = MDBX_ENOMEM;
      goto fail;
    }
  }

  if (repg_pos) {
    assert(pgno < txn->mt_next_pgno);
    assert(pgno == repg_list[repg_pos]);
    /* Cutoff allocated pages from me_reclaimed_pglist */
    repg_list[0] = repg_len -= num;
    for (unsigned i = repg_pos - num; i < repg_len;)
      repg_list[++i] = repg_list[++repg_pos];
    assert(mdbx_pnl_check(env->me_reclaimed_pglist));
  } else {
    txn->mt_next_pgno = pgno + num;
    assert(txn->mt_next_pgno <= txn->mt_end_pgno);
  }

  if (env->me_flags32 & MDBX_PAGEPERTURB)
    memset(np, 0x71 /* 'q', 113 */, pgno2bytes(env, num));
  VALGRIND_MAKE_MEM_UNDEFINED_ERASE(np, pgno2bytes(env, num));

  np->mp_pgno = pgno;
  np->mp_leaf2_ksize16 = 0;
  np->mp_flags16 = 0;
  np->mp_pages = num;
  mdbx_page_dirty(txn, np);
  *mp = np;

  assert(mdbx_pnl_check(env->me_reclaimed_pglist));
  return MDBX_SUCCESS;
}

static inline int backlog_size(MDBX_txn_t *txn) {
  int reclaimed = txn->mt_env->me_reclaimed_pglist ? txn->mt_env->me_reclaimed_pglist[0] : 0;
  return reclaimed + txn->mt_loose_count + txn->mt_end_pgno - txn->mt_next_pgno;
}

static inline int backlog_extragap(MDBX_env_t *env) {
  /* LY: extra page(s) for b-tree rebalancing */
  return (env->me_flags32 & MDBX_LIFORECLAIM) ? 2 : 1;
}

/* LY: Prepare a backlog of pages to modify GACO itself,
 * while reclaiming is prohibited. It should be enough to prevent search
 * in page_alloc() during a deleting, when GACO tree is unbalanced. */
static MDBX_error_t prep_backlog(MDBX_txn_t *txn, cursor_t *mc) {
  /* LY: extra page(s) for b-tree rebalancing */
  const int extra = backlog_extragap(txn->mt_env);

  if (backlog_size(txn) < mc->mc_aht->aa.depth16 + extra) {
    MDBX_error_t rc = cursor_touch(mc);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    int backlog;
    while (unlikely((backlog = backlog_size(txn)) < extra)) {
      rc = page_alloc(mc, 1, nullptr, MDBX_ALLOC_GC);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (unlikely(rc != MDBX_NOTFOUND))
          return rc;
        break;
      }
    }
  }

  return MDBX_SUCCESS;
}

/* Save the freelist as of this transaction to the GACO.
 * This changes the freelist. Keep trying until it stabilizes. */
static MDBX_error_t freelist_save(MDBX_txn_t *txn) {
  /* env->me_reclaimed_pglist[] can grow and shrink during this call.
   * env->me_last_reclaimed and txn->mt_free_pages[] can only grow.
   * Page numbers cannot disappear from txn->mt_free_pages[]. */
  MDBX_cursor_t mc;
  MDBX_env_t *env = txn->mt_env;
  int rc, more = 1;
  txnid_t cleanup_reclaimed_id = 0, head_id = 0;
  pgno_t befree_count = 0;
  intptr_t head_room = 0, total_room = 0;
  unsigned cleanup_reclaimed_pos = 0, refill_reclaimed_pos = 0;
  const bool lifo = (env->me_flags32 & MDBX_LIFORECLAIM) != 0;

  rc = cursor_init(&mc, txn, aht_gaco(txn));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* MDBX_IUD_RESERVE cancels meminit in ovpage malloc (when no WRITEMAP) */
  const intptr_t clean_limit =
      (env->me_flags32 & (MDBX_NOMEMINIT | MDBX_WRITEMAP)) ? SSIZE_MAX : env->me_maxfree_1pg;

  assert(mdbx_pnl_check(env->me_reclaimed_pglist));
again_on_freelist_change:
  assert(mdbx_pnl_check(env->me_reclaimed_pglist));
  while (1) {
    /* Come back here after each Put() in case freelist changed */
    MDBX_iov_t key, data;

    assert(mdbx_pnl_check(env->me_reclaimed_pglist));
    if (!lifo) {
      /* If using records from GACO which we have not yet deleted,
       * now delete them and any we reserved for me_reclaimed_pglist. */
      while (cleanup_reclaimed_id < env->me_last_reclaimed) {
        rc = cursor_first(&mc.primal, &key, nullptr);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        rc = prep_backlog(txn, &mc.primal);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        cleanup_reclaimed_id = head_id = *(txnid_t *)key.iov_base;
        total_room = head_room = 0;
        more = 1;
        assert(cleanup_reclaimed_id <= env->me_last_reclaimed);
        mc.primal.mc_state8 |= C_RECLAIMING;
        rc = mdbx_cursor_delete(&mc, 0);
        mc.primal.mc_state8 ^= C_RECLAIMING;
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
    } else if (txn->mt_lifo_reclaimed) {
      /* LY: cleanup reclaimed records. */
      while (cleanup_reclaimed_pos < txn->mt_lifo_reclaimed[0]) {
        cleanup_reclaimed_id = txn->mt_lifo_reclaimed[++cleanup_reclaimed_pos];
        key.iov_base = &cleanup_reclaimed_id;
        key.iov_len = sizeof(cleanup_reclaimed_id);
        rc = mdbx_cursor_get(&mc, &key, nullptr, MDBX_SET);
        if (likely(rc != MDBX_NOTFOUND)) {
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          rc = prep_backlog(txn, &mc.primal);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          mc.primal.mc_state8 |= C_RECLAIMING;
          rc = mdbx_cursor_delete(&mc, 0);
          mc.primal.mc_state8 ^= C_RECLAIMING;
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        }
      }
    }

    assert(mdbx_pnl_check(env->me_reclaimed_pglist));
    if (txn->mt_loose_pages) {
      /* Return loose page numbers to me_reclaimed_pglist,
       * though usually none are left at this point.
       * The pages themselves remain in dirtylist. */
      if (unlikely(!env->me_reclaimed_pglist) && !(lifo && env->me_last_reclaimed > 1)) {
        /* Put loose page numbers in mt_free_pages,
         * since unable to return them to me_reclaimed_pglist. */
        if (unlikely((rc = mdbx_pnl_need(&txn->mt_befree_pages, txn->mt_loose_count)) != 0))
          return rc;
        for (page_t *mp = txn->mt_loose_pages; mp; mp = NEXT_LOOSE_PAGE(mp))
          mdbx_pnl_xappend(txn->mt_befree_pages, mp->mp_pgno);
      } else {
        /* RRBR for loose pages + temp PNL with same */
        if ((rc = mdbx_pnl_need(&env->me_reclaimed_pglist, 2 * txn->mt_loose_count + 1)) != 0)
          goto bailout;
        MDBX_PNL loose =
            env->me_reclaimed_pglist + MDBX_PNL_ALLOCLEN(env->me_reclaimed_pglist) - txn->mt_loose_count;
        unsigned count = 0;
        for (page_t *mp = txn->mt_loose_pages; mp; mp = NEXT_LOOSE_PAGE(mp))
          loose[++count] = mp->mp_pgno;
        loose[0] = count;
        mdbx_pnl_sort(loose);
        mdbx_pnl_xmerge(env->me_reclaimed_pglist, loose);
      }

      MDBX_ID2L dl = txn->mt_rw_dirtylist;
      for (page_t *mp = txn->mt_loose_pages; mp;) {
        mdbx_assert(env, mp->mp_pgno < txn->mt_next_pgno);
        mdbx_ensure(env, mp->mp_pgno >= NUM_METAS);

        unsigned s, d;
        for (s = d = 0; ++s <= dl[0].mid;)
          if (dl[s].mid != mp->mp_pgno)
            dl[++d] = dl[s];

        dl[0].mid -= 1;
        mdbx_assert(env, dl[0].mid == d);

        page_t *dp = mp;
        mp = NEXT_LOOSE_PAGE(mp);
        if ((env->me_flags32 & MDBX_WRITEMAP) == 0)
          dpage_free(env, dp);
      }

      txn->mt_loose_pages = nullptr;
      txn->mt_loose_count = 0;
    }

    assert(mdbx_pnl_check(env->me_reclaimed_pglist));
    if (env->me_reclaimed_pglist) {
      /* Refund suitable pages into "unallocated" space */
      pgno_t tail = txn->mt_next_pgno;
      pgno_t *const begin = env->me_reclaimed_pglist + 1;
      pgno_t *const end = begin + env->me_reclaimed_pglist[0];
      pgno_t *higest;
#if MDBX_PNL_ASCENDING
      for (higest = end; --higest >= begin;) {
#else
      for (higest = begin; higest < end; ++higest) {
#endif /* MDBX_PNL sort-order */
        assert(*higest >= NUM_METAS && *higest < tail);
        if (*higest != tail - 1)
          break;
        tail -= 1;
      }
      if (tail != txn->mt_next_pgno) {
#if MDBX_PNL_ASCENDING
        env->me_reclaimed_pglist[0] = (unsigned)(higest + 1 - begin);
#else
        env->me_reclaimed_pglist[0] -= (unsigned)(higest - begin);
        for (pgno_t *move = begin; higest < end; ++move, ++higest)
          *move = *higest;
#endif /* MDBX_PNL sort-order */
        env_info("refunded %" PRIaPGNO " pages: %" PRIaPGNO " -> %" PRIaPGNO, tail - txn->mt_next_pgno, tail,
                 txn->mt_next_pgno);
        txn->mt_next_pgno = tail;
        assert(mdbx_pnl_check(env->me_reclaimed_pglist));
      }
    }

    /* Save the PNL of pages freed by this txn, to a single record */
    if (befree_count < txn->mt_befree_pages[0]) {
      if (unlikely(!befree_count)) {
        /* Make sure last page of GACO is touched and on freelist */
        rc = page_search(&mc.primal, nullptr, MDBX_PS_LAST | MDBX_PS_MODIFY);
        if (unlikely(rc && rc != MDBX_NOTFOUND))
          goto bailout;
      }
      pgno_t *befree_pages = txn->mt_befree_pages;
      /* Write to last page of GACO */
      key.iov_len = sizeof(txn->mt_txnid);
      key.iov_base = (void *)&txn->mt_txnid;
      do {
        befree_count = befree_pages[0];
        data.iov_len = MDBX_PNL_SIZEOF(befree_pages);
        rc = cursor_put(&mc.primal, &key, &data, MDBX_IUD_RESERVE);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        /* Retry if mt_free_pages[] grew during the Put() */
        befree_pages = txn->mt_befree_pages;
      } while (befree_count < befree_pages[0]);

      mdbx_pnl_sort(befree_pages);
      memcpy(data.iov_base, befree_pages, data.iov_len);

      if (mdbx_dbglog_enabled(MDBX_LOG_GACO, MDBX_LOGLEVEL_VERBOSE)) {
        dbglog_begin(MDBX_LOG_GACO, MDBX_LOGLEVEL_VERBOSE);

        unsigned i = (unsigned)befree_pages[0];
        dbglog_continue("PNL write txn %" PRIaTXN " root %" PRIaPGNO " num %u, PNL", txn->mt_txnid,
                        aht_gaco(txn)->aa.root, i);
        for (; i; i--)
          dbglog_continue(" %" PRIaPGNO "", befree_pages[i]);
        dbglog_end(" ]\n");
      }

      continue;
    }

    assert(mdbx_pnl_check(env->me_reclaimed_pglist));
    const intptr_t rpl_len =
        (env->me_reclaimed_pglist ? env->me_reclaimed_pglist[0] : 0) + txn->mt_loose_count;
    if (rpl_len && refill_reclaimed_pos == 0)
      refill_reclaimed_pos = 1;

    /* Reserve records for me_reclaimed_pglist[]. Split it if multi-page,
     * to avoid searching GACO for a page range. Use keys in
     * range [1,me_last_reclaimed]: Smaller than txnid of oldest reader. */
    if (total_room >= rpl_len) {
      if (total_room == rpl_len || --more < 0)
        break;
    } else if (head_room >= (intptr_t)env->me_maxfree_1pg && head_id > 1) {
      /* Keep current record (overflow page), add a new one */
      head_id--;
      refill_reclaimed_pos++;
      head_room = 0;
    }

    if (lifo) {
      if (refill_reclaimed_pos > (txn->mt_lifo_reclaimed ? txn->mt_lifo_reclaimed[0] : 0)) {
        /* LY: need just a txn-id for save page list. */
        rc = page_alloc(&mc.primal, 0, nullptr, MDBX_ALLOC_GC | MDBX_ALLOC_KICK);
        if (likely(rc == 0))
          /* LY: ok, reclaimed from GACO. */
          continue;
        if (unlikely(rc != MDBX_NOTFOUND))
          /* LY: other troubles... */
          goto bailout;

        /* LY: GACO is empty, will look any free txn-id in high2low order. */
        if (unlikely(env->me_last_reclaimed < 1)) {
          /* LY: not any txn in the past of GACO. */
          rc = MDBX_MAP_FULL;
          goto bailout;
        }

        if (unlikely(!txn->mt_lifo_reclaimed)) {
          txn->mt_lifo_reclaimed = mdbx_txl_alloc();
          if (unlikely(!txn->mt_lifo_reclaimed)) {
            rc = MDBX_ENOMEM;
            goto bailout;
          }
        }
        /* LY: append the list. */
        rc = mdbx_txl_append(&txn->mt_lifo_reclaimed, env->me_last_reclaimed - 1);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        --env->me_last_reclaimed;
        /* LY: note that GACO cleanup is not needed. */
        ++cleanup_reclaimed_pos;
      }
      assert(txn->mt_lifo_reclaimed != nullptr);
      head_id = txn->mt_lifo_reclaimed[refill_reclaimed_pos];
    } else {
      assert(txn->mt_lifo_reclaimed == nullptr);
    }

    /* (Re)write {key = head_id, PNL length = head_room} */
    total_room -= head_room;
    head_room = rpl_len - total_room;
    if (head_room > (intptr_t)env->me_maxfree_1pg && head_id > 1) {
      /* Overflow multi-page for part of me_reclaimed_pglist */
      head_room /= (head_id < INT16_MAX) ? (pgno_t)head_id : INT16_MAX; /* amortize page sizes */
      head_room += env->me_maxfree_1pg - head_room % (env->me_maxfree_1pg + 1);
    } else if (head_room < 0) {
      /* Rare case, not bothering to delete this record */
      head_room = 0;
      continue;
    }
    key.iov_len = sizeof(head_id);
    key.iov_base = &head_id;
    data.iov_len = (head_room + 1) * sizeof(pgno_t);
    rc = cursor_put(&mc.primal, &key, &data, MDBX_IUD_RESERVE);
    assert(mdbx_pnl_check(env->me_reclaimed_pglist));
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    /* PNL is initially empty, zero out at least the length */
    pgno_t *pgs = (pgno_t *)data.iov_base;
    intptr_t i = head_room > clean_limit ? head_room : 0;
    do {
      pgs[i] = 0;
    } while (--i >= 0);
    total_room += head_room;
    continue;
  }

  assert(cleanup_reclaimed_pos == (txn->mt_lifo_reclaimed ? txn->mt_lifo_reclaimed[0] : 0));

  /* Fill in the reserved me_reclaimed_pglist records */
  rc = MDBX_SUCCESS;
  assert(mdbx_pnl_check(env->me_reclaimed_pglist));
  if (env->me_reclaimed_pglist && env->me_reclaimed_pglist[0]) {
    MDBX_iov_t key, data;
    key.iov_len = data.iov_len = 0; /* avoid MSVC warning */
    key.iov_base = data.iov_base = nullptr;

    size_t rpl_left = env->me_reclaimed_pglist[0];
    pgno_t *rpl_end = env->me_reclaimed_pglist + rpl_left;
    if (txn->mt_lifo_reclaimed == 0) {
      assert(lifo == 0);
      rc = cursor_first(&mc.primal, &key, &data);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    } else {
      assert(lifo != 0);
    }

    while (1) {
      txnid_t id;
      if (txn->mt_lifo_reclaimed == 0) {
        assert(lifo == 0);
        id = *(txnid_t *)key.iov_base;
        assert(id <= env->me_last_reclaimed);
      } else {
        assert(lifo != 0);
        assert(refill_reclaimed_pos > 0 && refill_reclaimed_pos <= txn->mt_lifo_reclaimed[0]);
        id = txn->mt_lifo_reclaimed[refill_reclaimed_pos--];
        key.iov_base = &id;
        key.iov_len = sizeof(id);
        rc = mdbx_cursor_get(&mc, &key, &data, MDBX_SET);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      assert(cleanup_reclaimed_pos == (txn->mt_lifo_reclaimed ? txn->mt_lifo_reclaimed[0] : 0));

      assert(data.iov_len >= sizeof(pgno_t) * 2);
      size_t chunk_len = (data.iov_len / sizeof(pgno_t)) - 1;
      if (chunk_len > rpl_left)
        chunk_len = rpl_left;
      data.iov_len = (chunk_len + 1) * sizeof(pgno_t);
      key.iov_base = &id;
      key.iov_len = sizeof(id);

      rpl_end -= chunk_len;
      data.iov_base = rpl_end;
      pgno_t save = rpl_end[0];
      rpl_end[0] = (pgno_t)chunk_len;
      assert(mdbx_pnl_check(rpl_end));
      mc.primal.mc_state8 |= C_RECLAIMING;
      rc = cursor_put(&mc.primal, &key, &data, MDBX_IUD_CURRENT);
      mc.primal.mc_state8 ^= C_RECLAIMING;
      mdbx_assert(env, mdbx_pnl_check(rpl_end));
      assert(cleanup_reclaimed_pos == (txn->mt_lifo_reclaimed ? txn->mt_lifo_reclaimed[0] : 0));
      rpl_end[0] = save;
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;

      rpl_left -= chunk_len;
      if (rpl_left == 0)
        break;

      if (!lifo) {
        rc = cursor_next(&mc.primal, &key, &data, MDBX_NEXT);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
    }
  }

bailout:
  if (txn->mt_lifo_reclaimed) {
    assert(rc || cleanup_reclaimed_pos == txn->mt_lifo_reclaimed[0]);
    if (rc == MDBX_SUCCESS && cleanup_reclaimed_pos != txn->mt_lifo_reclaimed[0]) {
      assert(cleanup_reclaimed_pos < txn->mt_lifo_reclaimed[0]);
      /* LY: zeroed cleanup_idx to force cleanup
       * and refill created GACO records. */
      cleanup_reclaimed_pos = 0;
      /* LY: restart filling */
      total_room = head_room = refill_reclaimed_pos = 0;
      more = 1;
      goto again_on_freelist_change;
    }
    txn->mt_lifo_reclaimed[0] = 0;
    if (txn != env->me_wpa_txn) {
      mdbx_txl_free(txn->mt_lifo_reclaimed);
      txn->mt_lifo_reclaimed = nullptr;
    }
  }

  return rc;
}
