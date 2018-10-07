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
      /* If mc is updating the GC, then the it cannot play
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
      txn->mt_loose_pages = np->mp_next;
      txn->mt_loose_count--;
      mdbx_debug("db %d use loose page %" PRIaPGNO, DAAH(mc), np->mp_pgno);
      ASAN_UNPOISON_MEMORY_REGION(np, env->me_psize);
      assert(np->mp_pgno < txn->mt_next_pgno);
      mdbx_ensure(env, np->mp_pgno >= MDBX_NUM_METAS);
      *mp = np;
      return MDBX_SUCCESS;
    }
  }

  assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
  pgno_t pgno, *repg_list = env->me_reclaimed_pglist;
  unsigned repg_pos = 0, repg_len = repg_list ? MDBX_PNL_SIZE(repg_list) : 0;
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
      assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
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
        oldest = (flags & MDBX_LIFORECLAIM) ? find_oldest(txn) : *env->me_oldest;
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
          for (i = (unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed); i > 0; --i)
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
      mdbx_assert(env, (mc->mc_state8 & C_GCFREEZE) == 0);
      pgno_t *re_pnl = (pgno_t *)data.iov_base;
      assert(data.iov_len >= MDBX_PNL_SIZEOF(re_pnl));
      assert(mdbx_pnl_check(re_pnl, false));
      repg_pos = MDBX_PNL_SIZE(re_pnl);
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
      repg_len = MDBX_PNL_SIZE(repg_list);
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
          assert(*higest >= MDBX_NUM_METAS && *higest < tail);
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
          MDBX_PNL_SIZE(repg_list) = repg_len;
          gaco_info("refunded %" PRIaPGNO " pages: %" PRIaPGNO " -> %" PRIaPGNO, tail - txn->mt_next_pgno,
                    tail, txn->mt_next_pgno);
          txn->mt_next_pgno = tail;
          assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
        }
      }

      /* Don't try to coalesce too much. */
      if (unlikely(repg_len > MDBX_DPL_TXNFULL / 16))
        break;
      if (repg_len /* current size */ >= env->me_maxfree_1pg ||
          repg_pos /* prev size */ >= env->me_maxfree_1pg / 2)
        flags &= ~MDBX_COALESCE;
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

        mdbx_debug("kick-gc: head %" PRIaTXN "-%s, tail %" PRIaTXN "-%s, oldest %" PRIaTXN,
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

    if (rc == MDBX_MAP_FULL && next < head->mm_dxb_geo.upper) {
      mdbx_assert(env, next > txn->mt_end_pgno);
      pgno_t aligned =
          pgno_align2os_pgno(env, pgno_add(next, head->mm_dxb_geo.grow16 - next % head->mm_dxb_geo.grow16));

      if (aligned > head->mm_dxb_geo.upper)
        aligned = head->mm_dxb_geo.upper;
      mdbx_assert(env, aligned > txn->mt_end_pgno);

      gaco_info("try growth datafile to %" PRIaPGNO " pages (+%" PRIaPGNO ")", aligned,
                aligned - txn->mt_end_pgno);
      rc = mdbx_mapresize(env, aligned, head->mm_dxb_geo.upper);
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
    assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
    if (mp) {
      *mp = nullptr;
      txn->mt_flags |= MDBX_TXN_ERROR;
    }
    assert(rc);
    return rc;
  }

done:
  assert(mp && num);
  assert(pgno >= MDBX_NUM_METAS);
  mdbx_ensure(env, pgno >= MDBX_NUM_METAS);
  if (unlikely(pgno < MDBX_NUM_METAS)) {
    rc = MDBX_PANIC;
    goto fail;
  }

  if (env->me_flags32 & MDBX_WRITEMAP) {
    np = pgno2page(env, pgno);
    /* LY: reset no-access flag */
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
    mdbx_assert(env, (mc->mc_state8 & C_GCFREEZE) == 0);
    assert(pgno < txn->mt_next_pgno);
    assert(pgno == repg_list[repg_pos]);
    /* Cutoff allocated pages from me_reclaimed_pglist */
    MDBX_PNL_SIZE(repg_list) = repg_len -= num;
    for (unsigned i = repg_pos - num; i < repg_len;)
      repg_list[++i] = repg_list[++repg_pos];
    assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
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
  rc = mdbx_page_dirty(txn, np);
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;
  *mp = np;

  mdbx_debug("page %" PRIaPGNO, pgno);
  assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
  return MDBX_SUCCESS;
}

static inline int backlog_size(MDBX_txn_t *txn) {
  int reclaimed_and_loose = txn->mt_env->me_reclaimed_pglist
                                ? MDBX_PNL_SIZE(txn->mt_env->me_reclaimed_pglist) + txn->mt_loose_count
                                : 0;
  return reclaimed_and_loose + txn->mt_end_pgno - txn->mt_next_pgno;
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

/* Cleanup reclaimed GC records, than save the befree-list as of this
 * transaction to GC (aka freeDB). This recursive changes the reclaimed-list
 * loose-list and befree-list. Keep trying until it stabilizes. */
static MDBX_error_t update_gc(MDBX_txn_t *txn) {
  /* env->me_reclaimed_pglist[] can grow and shrink during this call.
   * env->me_last_reclaimed and txn->mt_befree_pages[] can only grow.
   * Page numbers cannot disappear from txn->mt_befree_pages[]. */
  MDBX_env_t *const env = txn->mt_env;
  const bool lifo = (env->me_flags32 & MDBX_LIFORECLAIM) != 0;
  const char *dbg_prefix_mode = lifo ? "    lifo" : "    fifo";
  (void)dbg_prefix_mode;
  mdbx_trace("\n>>> @%" PRIaTXN, txn->mt_txnid);

  unsigned befree_stored = 0, loop = 0;
  MDBX_cursor_t mc;
  int rc = cursor_init(&mc, txn, aht_gaco(txn));
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout_notracking;

  MDBX_cursor_t **const tracking_head = cursor_tracking_head(&mc);
  mc.mc_next = *tracking_head;
  *tracking_head = &mc;

retry:
  mdbx_trace(" >> restart");
  assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
  if (unlikely(/* paranoia */ ++loop > 42)) {
    mdbx_error("too more loops %u, bailout", loop);
    rc = MDBX_PROBLEM;
    goto bailout;
  }

  unsigned settled = 0, cleaned_gc_slot = 0, reused_gc_slot = 0, filled_gc_slot = ~0u;
  txnid_t cleaned_gc_id = 0, head_gc_id = env->me_last_reclaimed;
  while (1) {
    /* Come back here after each Put() in case befree-list changed */
    MDBX_iov_t key, data;
    mdbx_trace(" >> continue");

    assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
    if (txn->mt_lifo_reclaimed) {
      if (cleaned_gc_slot < MDBX_PNL_SIZE(txn->mt_lifo_reclaimed)) {
        settled = 0;
        cleaned_gc_slot = 0;
        reused_gc_slot = 0;
        filled_gc_slot = ~0u;
        /* LY: cleanup reclaimed records. */
        do {
          cleaned_gc_id = txn->mt_lifo_reclaimed[++cleaned_gc_slot];
          assert(cleaned_gc_slot > 0 && cleaned_gc_id < *env->me_oldest);
          key.iov_base = &cleaned_gc_id;
          key.iov_len = sizeof(cleaned_gc_id);
          rc = cursor_get(&mc.primal, &key, NULL, MDBX_SET);
          if (rc == MDBX_NOTFOUND)
            continue;
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          rc = prep_backlog(txn, &mc.primal);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          assert(cleaned_gc_id < *env->me_oldest);
          mdbx_trace("%s.cleanup-reclaimed-id [%u]%" PRIaTXN, dbg_prefix_mode, cleaned_gc_slot, cleaned_gc_id);
          mc.primal.mc_state8 |= C_RECLAIMING;
          rc = cursor_delete(&mc.primal, 0);
          mc.primal.mc_state8 -= C_RECLAIMING;
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        } while (cleaned_gc_slot < MDBX_PNL_SIZE(txn->mt_lifo_reclaimed));
        mdbx_txl_sort(txn->mt_lifo_reclaimed);
        head_gc_id = MDBX_PNL_LAST(txn->mt_lifo_reclaimed);
      }
    } else {
      /* If using records from freeDB which we have not yet deleted,
       * now delete them and any we reserved for me_reclaimed_pglist. */
      while (cleaned_gc_id < env->me_last_reclaimed) {
        rc = cursor_first(&mc.primal, &key, NULL);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        rc = prep_backlog(txn, &mc.primal);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        cleaned_gc_id = head_gc_id = *(txnid_t *)key.iov_base;
        assert(cleaned_gc_id <= env->me_last_reclaimed);
        assert(cleaned_gc_id < *env->me_oldest);
        mdbx_trace("%s.cleanup-reclaimed-id %" PRIaTXN, dbg_prefix_mode, cleaned_gc_id);
        mc.primal.mc_state8 |= C_RECLAIMING;
        rc = cursor_delete(&mc.primal, 0);
        mc.primal.mc_state8 -= C_RECLAIMING;
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        settled = 0;
      }
    }

    // handle loose pages - put ones into the reclaimed- or befree-list
    assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
    if (MDBX_AUDIT_ENABLED()) {
      rc = audit(txn, befree_stored);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
    if (txn->mt_loose_pages) {
      /* Return loose page numbers to me_reclaimed_pglist,
       * though usually none are left at this point.
       * The pages themselves remain in dirtylist. */
      if (unlikely(!env->me_reclaimed_pglist) && !txn->mt_lifo_reclaimed && env->me_last_reclaimed < 1) {
        /* Put loose page numbers in mt_befree_pages,
         * since unable to return them to me_reclaimed_pglist. */
        if (unlikely((rc = mdbx_pnl_need(&txn->mt_befree_pages, txn->mt_loose_count)) != 0))
          goto bailout;
        for (page_t *mp = txn->mt_loose_pages; mp; mp = mp->mp_next)
          mdbx_pnl_xappend(txn->mt_befree_pages, mp->mp_pgno);
        mdbx_trace("%s: append %u loose-pages to befree-pages", dbg_prefix_mode, txn->mt_loose_count);
      } else {
        /* Room for loose pages + temp PNL with same */
        if (likely(env->me_reclaimed_pglist != NULL)) {
          rc = mdbx_pnl_need(&env->me_reclaimed_pglist, 2 * txn->mt_loose_count + 2);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          MDBX_PNL loose =
              env->me_reclaimed_pglist + MDBX_PNL_ALLOCLEN(env->me_reclaimed_pglist) - txn->mt_loose_count - 1;
          unsigned count = 0;
          for (page_t *mp = txn->mt_loose_pages; mp; mp = mp->mp_next)
            loose[++count] = mp->mp_pgno;
          MDBX_PNL_SIZE(loose) = count;
          mdbx_pnl_sort(loose);
          mdbx_pnl_xmerge(env->me_reclaimed_pglist, loose);
        } else {
          env->me_reclaimed_pglist = mdbx_pnl_alloc(txn->mt_loose_count);
          if (unlikely(env->me_reclaimed_pglist == NULL)) {
            rc = MDBX_ENOMEM;
            goto bailout;
          }
          for (page_t *mp = txn->mt_loose_pages; mp; mp = mp->mp_next)
            mdbx_pnl_xappend(env->me_reclaimed_pglist, mp->mp_pgno);
          mdbx_pnl_sort(env->me_reclaimed_pglist);
        }
        mdbx_trace("%s: append %u loose-pages to reclaimed-pages", dbg_prefix_mode, txn->mt_loose_count);
      }

      // filter-out list of dirty-pages from loose-pages
      MDBX_DPL dl = txn->mt_rw_dirtylist;
      mdbx_dpl_sort(dl);
      unsigned left = dl->length;
      for (page_t *mp = txn->mt_loose_pages; mp;) {
        assert(mp->mp_pgno < txn->mt_next_pgno);
        mdbx_ensure(env, mp->mp_pgno >= MDBX_NUM_METAS);

        if (left > 0) {
          const unsigned i = mdbx_dpl_search(dl, mp->mp_pgno);
          if (i <= dl->length && dl[i].pgno == mp->mp_pgno) {
            assert(i > 0 && dl[i].ptr != (void *)dl);
            dl[i].ptr = (void *)dl /* mark for deletion */;
          }
          left -= 1;
        }

        page_t *dp = mp;
        mp = mp->mp_next;
        if ((env->me_flags32 & MDBX_WRITEMAP) == 0)
          dpage_free(env, dp /*, 1*/);
      }

      if (left > 0) {
        MDBX_DPL r, w, end = dl + dl->length;
        for (r = w = dl + 1; r <= end; r++) {
          if (r->ptr != (void *)dl) {
            if (r != w)
              *w = *r;
            ++w;
          }
        }
        assert(w - dl == (int)left + 1);
      }

      if (left != dl->length)
        mdbx_trace("%s: filtered-out loose-pages from %u -> %u dirty-pages", dbg_prefix_mode, dl->length,
                   left);
      dl->length = left;

      txn->mt_loose_pages = NULL;
      txn->mt_loose_count = 0;
      if (MDBX_AUDIT_ENABLED()) {
        rc = audit(txn, befree_stored);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
    }

    // handle reclaimed pages - return suitable into unallocated space
    assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
    if (env->me_reclaimed_pglist) {
      pgno_t tail = txn->mt_next_pgno;
      pgno_t *const begin = MDBX_PNL_BEGIN(env->me_reclaimed_pglist);
      pgno_t *const end = MDBX_PNL_END(env->me_reclaimed_pglist);
      pgno_t *higest;
#if MDBX_PNL_ASCENDING
      for (higest = end; --higest >= begin;) {
#else
      for (higest = begin; higest < end; ++higest) {
#endif /* MDBX_PNL sort-order */
        assert(*higest >= MDBX_NUM_METAS && *higest < tail);
        if (*higest != tail - 1)
          break;
        tail -= 1;
      }
      if (tail != txn->mt_next_pgno) {
#if MDBX_PNL_ASCENDING
        MDBX_PNL_SIZE(env->me_reclaimed_pglist) = (unsigned)(higest + 1 - begin);
#else
        MDBX_PNL_SIZE(env->me_reclaimed_pglist) -= (unsigned)(higest - begin);
        for (pgno_t *move = begin; higest < end; ++move, ++higest)
          *move = *higest;
#endif /* MDBX_PNL sort-order */
        mdbx_verbose("%s.refunded %" PRIaPGNO " pages: %" PRIaPGNO " -> %" PRIaPGNO, dbg_prefix_mode,
                     txn->mt_next_pgno - tail, tail, txn->mt_next_pgno);
        txn->mt_next_pgno = tail;
        assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
        if (MDBX_AUDIT_ENABLED()) {
          rc = audit(txn, befree_stored);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        }
      }
    }

    // handle befree-list - store ones into singe gc-record
    if (befree_stored < MDBX_PNL_SIZE(txn->mt_befree_pages)) {
      if (unlikely(!befree_stored)) {
        /* Make sure last page of freeDB is touched and on befree-list */
        rc = page_search(&mc.primal, NULL, MDBX_PS_LAST | MDBX_PS_MODIFY);
        if (unlikely(rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND))
          goto bailout;
      }
      /* Write to last page of freeDB */
      key.iov_len = sizeof(txn->mt_txnid);
      key.iov_base = (void *)&txn->mt_txnid;
      do {
        data.iov_len = MDBX_PNL_SIZEOF(txn->mt_befree_pages);
        rc = cursor_put(&mc.primal, &key, &data, MDBX_IUD_RESERVE);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        /* Retry if mt_befree_pages[] grew during the Put() */
      } while (data.iov_len < MDBX_PNL_SIZEOF(txn->mt_befree_pages));

      befree_stored = (unsigned)MDBX_PNL_SIZE(txn->mt_befree_pages);
      mdbx_pnl_sort(txn->mt_befree_pages);
      memcpy(data.iov_base, txn->mt_befree_pages, data.iov_len);

      mdbx_trace("%s.put-befree #%u @ %" PRIaTXN, dbg_prefix_mode, befree_stored, txn->mt_txnid);
      continue;
    }

    // handle reclaimed and loost pages - merge and store both into gc
    assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
    assert(txn->mt_loose_count == 0);

    mdbx_trace(" >> reserving");
    if (MDBX_AUDIT_ENABLED()) {
      rc = audit(txn, befree_stored);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
    const unsigned amount = env->me_reclaimed_pglist ? MDBX_PNL_SIZE(env->me_reclaimed_pglist) : 0;
    const unsigned left = amount - settled;
    mdbx_trace("%s: amount %u, settled %d, left %d, lifo-reclaimed-slots %u, "
               "reused-gc-slots %u",
               dbg_prefix_mode, amount, settled, (int)left,
               txn->mt_lifo_reclaimed ? (unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) : 0, reused_gc_slot);
    if (0 >= (int)left)
      break;

    if (unlikely(head_gc_id == 0)) {
      head_gc_id = find_oldest(txn) - 1;
      if (txn->mt_lifo_reclaimed == NULL) {
        rc = cursor_get(&mc.primal, &key, NULL, MDBX_FIRST);
        if (unlikely(rc != MDBX_SUCCESS)) {
          if (rc != MDBX_NOTFOUND)
            goto bailout;
        } else if (unlikely(key.iov_len != sizeof(txnid_t))) {
          rc = MDBX_CORRUPTED;
          goto bailout;
        } else {
          txnid_t first_txn;
          memcpy(&first_txn, key.iov_base, sizeof(txnid_t));
          if (head_gc_id >= first_txn)
            head_gc_id = first_txn - 1;
        }
      }
    }

    const unsigned prefer_max_scatter = 257;
    txnid_t reservation_gc_id;
    if (lifo) {
      assert(txn->mt_lifo_reclaimed != NULL);
      if (unlikely(!txn->mt_lifo_reclaimed)) {
        txn->mt_lifo_reclaimed = mdbx_txl_alloc();
        if (unlikely(!txn->mt_lifo_reclaimed)) {
          rc = MDBX_ENOMEM;
          goto bailout;
        }
      }

      if (head_gc_id > 1 && MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) < prefer_max_scatter &&
          left > ((unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) - reused_gc_slot) * env->me_maxfree_1pg) {
        /* LY: need just a txn-id for save page list. */
        rc = page_alloc(&mc.primal, 0, NULL, MDBX_ALLOC_GC | MDBX_ALLOC_KICK);
        if (likely(rc == MDBX_SUCCESS)) {
          /* LY: ok, reclaimed from freedb. */
          mdbx_trace("%s: took @%" PRIaTXN " from GC, continue", dbg_prefix_mode,
                     MDBX_PNL_LAST(txn->mt_lifo_reclaimed));
          continue;
        }
        if (unlikely(rc != MDBX_NOTFOUND))
          /* LY: other troubles... */
          goto bailout;

        /* LY: freedb is empty, will look any free txn-id in high2low order. */
        do {
          --head_gc_id;
          mdbx_assert(env, MDBX_PNL_IS_EMPTY(txn->mt_lifo_reclaimed) ||
                               MDBX_PNL_LAST(txn->mt_lifo_reclaimed) > head_gc_id);
          rc = mdbx_txl_append(&txn->mt_lifo_reclaimed, head_gc_id);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          cleaned_gc_slot += 1 /* mark GC cleanup is not needed. */;

          mdbx_trace("%s: append @%" PRIaTXN " to lifo-reclaimed, cleaned-gc-slot = %u", dbg_prefix_mode,
                     head_gc_id, cleaned_gc_slot);
        } while (head_gc_id > 1 && MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) < prefer_max_scatter &&
                 left >
                     ((unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) - reused_gc_slot) * env->me_maxfree_1pg);
      }

      if ((unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) <= reused_gc_slot) {
        mdbx_notice("** restart: reserve depleted (reused_gc_slot %u >= "
                    "lifo_reclaimed %u" PRIaTXN,
                    reused_gc_slot, (unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed));
        goto retry;
      }
      const unsigned i = (unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) - reused_gc_slot;
      assert(i > 0 && i <= MDBX_PNL_SIZE(txn->mt_lifo_reclaimed));
      reservation_gc_id = txn->mt_lifo_reclaimed[i];
      mdbx_trace("%s: take @%" PRIaTXN " from lifo-reclaimed[%u]", dbg_prefix_mode, reservation_gc_id, i);
    } else {
      assert(txn->mt_lifo_reclaimed == NULL);
      reservation_gc_id = head_gc_id--;
      mdbx_trace("%s: take @%" PRIaTXN " from head-gc-id", dbg_prefix_mode, reservation_gc_id);
    }
    ++reused_gc_slot;

    unsigned chunk = left;
    if (unlikely(chunk > env->me_maxfree_1pg)) {
      const unsigned avail_gs_slots =
          txn->mt_lifo_reclaimed ? (unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) - reused_gc_slot + 1
                                 : (head_gc_id < INT16_MAX) ? (unsigned)head_gc_id : INT16_MAX;
      if (avail_gs_slots > 1) {
        if (chunk < env->me_maxfree_1pg * 2)
          chunk /= 2;
        else {
          const unsigned threshold =
              env->me_maxfree_1pg *
              ((avail_gs_slots < prefer_max_scatter) ? avail_gs_slots : prefer_max_scatter);
          if (left < threshold)
            chunk = env->me_maxfree_1pg;
          else {
            const unsigned tail = left - threshold + env->me_maxfree_1pg + 1;
            unsigned span = 1;
            unsigned avail = (unsigned)((pgno2bytes(env, span) - PAGEHDRSZ) / sizeof(pgno_t)) /*- 1 + span */;
            if (tail > avail) {
              for (unsigned i = amount - span; i > 0; --i) {
                if (MDBX_PNL_ASCENDING
                        ? (env->me_reclaimed_pglist[i] + span)
                        : (env->me_reclaimed_pglist[i] - span) == env->me_reclaimed_pglist[i + span]) {
                  span += 1;
                  avail = (unsigned)((pgno2bytes(env, span) - PAGEHDRSZ) / sizeof(pgno_t)) - 1 + span;
                  if (avail >= tail)
                    break;
                }
              }
            }

            chunk =
                (avail >= tail)
                    ? tail - span
                    : (avail_gs_slots > 3 && reused_gc_slot < prefer_max_scatter - 3) ? avail - span : tail;
          }
        }
      }
    }
    assert(chunk > 0);

    mdbx_trace("%s: head_gc_id %" PRIaTXN ", reused_gc_slot %u, reservation-id "
               "%" PRIaTXN,
               dbg_prefix_mode, head_gc_id, reused_gc_slot, reservation_gc_id);

    mdbx_trace("%s: chunk %u, gc-per-ovpage %u", dbg_prefix_mode, chunk, env->me_maxfree_1pg);

    assert(reservation_gc_id < *env->me_oldest);
    if (unlikely(reservation_gc_id < 1 || reservation_gc_id >= *env->me_oldest)) {
      /* LY: not any txn in the past of freedb. */
      rc = MDBX_PROBLEM;
      goto bailout;
    }

    key.iov_len = sizeof(reservation_gc_id);
    key.iov_base = &reservation_gc_id;
    data.iov_len = (chunk + 1) * sizeof(pgno_t);
    mdbx_trace("%s.reserve: %u [%u...%u] @%" PRIaTXN, dbg_prefix_mode, chunk, settled + 1, settled + chunk + 1,
               reservation_gc_id);
    rc = cursor_put(&mc.primal, &key, &data, MDBX_IUD_RESERVE | MDBX_IUD_NOOVERWRITE);
    assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    /* PNL is initially empty, zero out at least the length */
    memset(data.iov_base, 0, sizeof(pgno_t));
    settled += chunk;
    mdbx_trace("%s.settled %u (+%u), continue", dbg_prefix_mode, settled, chunk);

    if (txn->mt_lifo_reclaimed && unlikely(amount < MDBX_PNL_SIZE(env->me_reclaimed_pglist))) {
      mdbx_notice("** restart: reclaimed-list growth %u -> %u", amount,
                  (unsigned)MDBX_PNL_SIZE(env->me_reclaimed_pglist));
      goto retry;
    }

    continue;
  }

  assert(cleaned_gc_slot == (txn->mt_lifo_reclaimed ? MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) : 0));

  mdbx_trace(" >> filling");
  /* Fill in the reserved records */
  filled_gc_slot = txn->mt_lifo_reclaimed ? (unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) - reused_gc_slot
                                          : reused_gc_slot;
  rc = MDBX_SUCCESS;
  assert(mdbx_pnl_check(env->me_reclaimed_pglist, true));
  if (env->me_reclaimed_pglist && MDBX_PNL_SIZE(env->me_reclaimed_pglist)) {
    MDBX_iov_t key, data;
    key.iov_len = data.iov_len = 0; /* avoid MSVC warning */
    key.iov_base = data.iov_base = NULL;

    const unsigned amount = MDBX_PNL_SIZE(env->me_reclaimed_pglist);
    unsigned left = amount;
    if (txn->mt_lifo_reclaimed == nullptr) {
      assert(lifo == 0);
      rc = cursor_first(&mc.primal, &key, &data);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    } else {
      assert(lifo != 0);
    }

    while (1) {
      txnid_t fill_gc_id;
      mdbx_trace("%s: left %u of %u", dbg_prefix_mode, left,
                 (unsigned)MDBX_PNL_SIZE(env->me_reclaimed_pglist));
      if (txn->mt_lifo_reclaimed == nullptr) {
        assert(lifo == 0);
        fill_gc_id = *(txnid_t *)key.iov_base;
        if (filled_gc_slot-- == 0 || fill_gc_id > env->me_last_reclaimed) {
          mdbx_notice("** restart: reserve depleted (filled_slot %u, fill_id %" PRIaTXN
                      " > last_reclaimed %" PRIaTXN,
                      filled_gc_slot, fill_gc_id, env->me_last_reclaimed);
          goto retry;
        }
      } else {
        assert(lifo != 0);
        if (++filled_gc_slot > (unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed)) {
          mdbx_notice("** restart: reserve depleted (filled_gc_slot %u > "
                      "lifo_reclaimed %u" PRIaTXN,
                      filled_gc_slot, (unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed));
          goto retry;
        }
        fill_gc_id = txn->mt_lifo_reclaimed[filled_gc_slot];
        mdbx_trace("%s.seek-reservation @%" PRIaTXN " at lifo_reclaimed[%u]", dbg_prefix_mode, fill_gc_id,
                   filled_gc_slot);
        key.iov_base = &fill_gc_id;
        key.iov_len = sizeof(fill_gc_id);
        rc = cursor_get(&mc.primal, &key, &data, MDBX_SET_KEY);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      assert(cleaned_gc_slot == (txn->mt_lifo_reclaimed ? MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) : 0));
      assert(fill_gc_id > 0 && fill_gc_id < *env->me_oldest);
      key.iov_base = &fill_gc_id;
      key.iov_len = sizeof(fill_gc_id);

      assert(data.iov_len >= sizeof(pgno_t) * 2);
      mc.primal.mc_state8 |= C_RECLAIMING | C_GCFREEZE;
      unsigned chunk = (unsigned)(data.iov_len / sizeof(pgno_t)) - 1;
      if (unlikely(chunk > left)) {
        mdbx_trace("%s: chunk %u > left %u, @%" PRIaTXN, dbg_prefix_mode, chunk, left, fill_gc_id);
        if (loop < 5 || chunk - left > env->me_maxfree_1pg) {
          data.iov_len = (left + 1) * sizeof(pgno_t);
          if (loop < 21)
            mc.primal.mc_state8 -= C_GCFREEZE;
        }
        chunk = left;
      }
      rc = cursor_put(&mc.primal, &key, &data, MDBX_IUD_CURRENT | MDBX_IUD_RESERVE);
      mc.primal.mc_state8 &= ~(C_RECLAIMING | C_GCFREEZE);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;

      if (unlikely(txn->mt_loose_count || amount != MDBX_PNL_SIZE(env->me_reclaimed_pglist))) {
        memset(data.iov_base, 0, sizeof(pgno_t));
        mdbx_notice("** restart: reclaimed-list changed (%u -> %u, %u)", amount,
                    MDBX_PNL_SIZE(env->me_reclaimed_pglist), txn->mt_loose_count);
        goto retry;
      }
      if (unlikely(txn->mt_lifo_reclaimed ? cleaned_gc_slot < MDBX_PNL_SIZE(txn->mt_lifo_reclaimed)
                                          : cleaned_gc_id < env->me_last_reclaimed)) {
        memset(data.iov_base, 0, sizeof(pgno_t));
        mdbx_notice("** restart: reclaimed-slots changed");
        goto retry;
      }

      pgno_t *dst = data.iov_base;
      *dst++ = chunk;
      pgno_t *src = MDBX_PNL_BEGIN(env->me_reclaimed_pglist) + left - chunk;
      memcpy(dst, src, chunk * sizeof(pgno_t));
      pgno_t *from = src, *to = src + chunk;
      mdbx_trace("%s.fill: %u [ %u:%" PRIaPGNO "...%u:%" PRIaPGNO "] @%" PRIaTXN, dbg_prefix_mode, chunk,
                 (unsigned)(from - env->me_reclaimed_pglist), from[0],
                 (unsigned)(to - env->me_reclaimed_pglist), to[-1], fill_gc_id);

      left -= chunk;
      if (MDBX_AUDIT_ENABLED()) {
        rc = audit(txn, befree_stored + amount - left);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      if (left == 0) {
        rc = MDBX_SUCCESS;
        break;
      }

      if (txn->mt_lifo_reclaimed == nullptr) {
        assert(lifo == 0);
        rc = cursor_next(&mc.primal, &key, &data, MDBX_NEXT);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      } else {
        assert(lifo != 0);
      }
    }
  }

  assert(rc == MDBX_SUCCESS);
  if (unlikely(txn->mt_loose_count != 0 ||
               filled_gc_slot !=
                   (txn->mt_lifo_reclaimed ? (unsigned)MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) : 0))) {
    mdbx_notice("** restart: reserve excess (filled-slot %u, loose-count %u)", filled_gc_slot,
                txn->mt_loose_count);
    goto retry;
  }

  assert(txn->mt_lifo_reclaimed == NULL || cleaned_gc_slot == MDBX_PNL_SIZE(txn->mt_lifo_reclaimed));

bailout:
  *tracking_head = mc.mc_next;

bailout_notracking:
  if (txn->mt_lifo_reclaimed) {
    MDBX_PNL_SIZE(txn->mt_lifo_reclaimed) = 0;
    if (txn != env->me_wpa_txn) {
      mdbx_txl_free(txn->mt_lifo_reclaimed);
      txn->mt_lifo_reclaimed = NULL;
    }
  }

  mdbx_trace("<<< %u loops, rc = %d", loop, rc);
  return rc;
}
