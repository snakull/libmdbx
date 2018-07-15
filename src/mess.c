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

/*----------------------------------------------------------------------------*/

/* Find oldest txnid still referenced. */
static txnid_t find_oldest(MDBX_txn_t *txn) {
  assert((txn->mt_flags & MDBX_RDONLY) == 0);
  const MDBX_env_t *env = txn->mt_env;
  MDBX_lockinfo_t *const lck = env->me_lck;

  const txnid_t edge = reclaiming_detent(env);
  assert(edge <= txn->mt_txnid - 1);
  const txnid_t last_oldest = lck->li_oldest;
  assert(edge >= last_oldest);
  if (last_oldest == edge)
    return edge;

  const uint32_t nothing_changed = MDBX_STRING_TETRAD("None");
  const uint32_t snap_readers_refresh_flag = lck->li_readers_refresh_flag;
  mdbx_jitter4testing(false);
  if (snap_readers_refresh_flag == nothing_changed)
    return last_oldest;

  txnid_t oldest = edge;
  lck->li_readers_refresh_flag = nothing_changed;
  mdbx_coherent_barrier();
  const unsigned snap_nreaders = lck->li_numreaders;
  for (unsigned i = 0; i < snap_nreaders; ++i) {
    if (lck->li_readers[i].mr_pid) {
      mdbx_jitter4testing(true);
      const txnid_t snap = lck->li_readers[i].mr_txnid;
      if (oldest > snap && last_oldest <= /* ignore pending updates */ snap) {
        oldest = snap;
        if (oldest == last_oldest)
          return oldest;
      }
    }
  }

  if (oldest != last_oldest) {
    txn_notice("update oldest %" PRIaTXN " -> %" PRIaTXN, last_oldest, oldest);
    assert(oldest >= lck->li_oldest);
    lck->li_oldest = oldest;
  }
  return oldest;
}

static int mdbx_mapresize(MDBX_env_t *env, const pgno_t size_pgno, const pgno_t limit_pgno) {
#ifdef USE_VALGRIND
  const size_t prev_mapsize = env->me_mapsize;
  void *const prev_mapaddr = env->me_map;
#endif

  const size_t limit_bytes = pgno_align2os_bytes(env, limit_pgno);
  const size_t size_bytes = pgno_align2os_bytes(env, size_pgno);

  env_info("resize datafile/mapping: "
           "present %" PRIuPTR " -> %" PRIuPTR ", "
           "limit %" PRIuPTR " -> %" PRIuPTR,
           env->me_dxb_geo.now, size_bytes, env->me_dxb_geo.upper, limit_bytes);

  mdbx_assert(env, limit_bytes >= size_bytes);
  mdbx_assert(env, bytes2pgno(env, size_bytes) == size_pgno);
  mdbx_assert(env, bytes2pgno(env, limit_bytes) == limit_pgno);

#if defined(_WIN32) || defined(_WIN64)
  /* Acquire guard in exclusive mode for:
   *   - to avoid collision between read and write txns around env->me_dxb_geo;
   *   - to avoid attachment of new reading threads (see mdbx_rdt_lock); */
  AcquireSRWLockExclusive(&env->me_remap_guard);
  mdbx_handle_array_t *suspended = NULL;
  mdbx_handle_array_t array_onstack;
  int rc = MDBX_SUCCESS;
  if (limit_bytes == env->me_dxb_mmap.length && size_bytes == env->me_dxb_mmap.current &&
      env->me_dxb_mmap.current == env->me_dxb_mmap.filesize)
    goto bailout;

  if ((env->me_flags32 & MDBX_RDONLY) || limit_bytes != env->me_dxb_mmap.length ||
      size_bytes < env->me_dxb_mmap.current) {
    /* Windows allows only extending a read-write section, but not a
     * corresponing mapped view. Therefore in other cases we must suspend
     * the local threads for safe remap. */
    array_onstack.limit = ARRAY_LENGTH(array_onstack.handles);
    array_onstack.count = 0;
    suspended = &array_onstack;
    rc = mdbx_suspend_threads_before_remap(env, &suspended);
    if (rc != MDBX_SUCCESS) {
      mdbx_error("failed suspend-for-remap: errcode %d", rc);
      goto bailout;
    }
  }
#else
  /* Acquire guard to avoid collision between read and write txns
   * around env->me_dxb_geo */
  int rc = mdbx_fastmutex_acquire(&env->me_remap_guard, 0);
  if (rc != MDBX_SUCCESS)
    return rc;
  if (limit_bytes == env->me_dxb_mmap.length && bytes2pgno(env, size_bytes) == env->me_dxb_geo.now)
    goto bailout;
#endif /* Windows */

  rc = mdbx_mresize(env->me_flags32, &env->me_dxb_mmap, size_bytes, limit_bytes);

bailout:
  if (rc == MDBX_SUCCESS) {
    env->me_dxb_geo.now = size_bytes;
    env->me_dxb_geo.upper = limit_bytes;
    if (env->me_current_txn) {
      mdbx_assert(env, env->me_current_txn->mt_owner == mdbx_thread_self());
      mdbx_assert(env, size_pgno >= env->me_current_txn->mt_next_pgno);
      env->me_current_txn->mt_end_pgno = size_pgno;
    }
#ifdef USE_VALGRIND
    if (prev_mapsize != env->me_mapsize || prev_mapaddr != env->me_map) {
      VALGRIND_DISCARD(env->me_valgrind_handle);
      env->me_valgrind_handle = 0;
      if (env->me_mapsize)
        env->me_valgrind_handle = VALGRIND_CREATE_BLOCK(env->me_map, env->me_mapsize, "mdbx");
    }
#endif
  } else if (rc != MDBX_SIGN) {
    mdbx_error("failed resize datafile/mapping: "
               "present %" PRIuPTR " -> %" PRIuPTR ", "
               "limit %" PRIuPTR " -> %" PRIuPTR ", errcode %d",
               env->me_dxb_geo.now, size_bytes, env->me_dxb_geo.upper, limit_bytes, rc);
  } else {
    mdbx_notice("unable resize datafile/mapping: "
                "present %" PRIuPTR " -> %" PRIuPTR ", "
                "limit %" PRIuPTR " -> %" PRIuPTR ", errcode %d",
                env->me_dxb_geo.now, size_bytes, env->me_dxb_geo.upper, limit_bytes, rc);
    if (!env->me_dxb_mmap.address) {
      env->me_flags32 |= MDBX_ENV_TAINTED;
      if (env->me_current_txn)
        env->me_current_txn->mt_flags |= MDBX_TXN_ERROR;
      rc = MDBX_PANIC;
    }
  }

#if defined(_WIN32) || defined(_WIN64)
  int err = MDBX_SUCCESS;
  ReleaseSRWLockExclusive(&env->me_remap_guard);
  if (suspended) {
    err = mdbx_resume_threads_after_remap(suspended);
    if (suspended != &array_onstack)
      free(suspended);
  }
#else
  int err = mdbx_fastmutex_release(&env->me_remap_guard);
#endif /* Windows */
  if (err != MDBX_SUCCESS) {
    mdbx_fatal(env, MDBX_LOG_ENV, "failed resume-after-remap: errcode %d", err);
    return MDBX_PANIC;
  }
  return rc;
}

MDBX_error_t mdbx_sync_ex(MDBX_env_t *env, size_t dirty_volume_threshold, size_t txn_gap_threshold,
                          size_t time_gap_threshold) {
  (void)env;
  (void)dirty_volume_threshold;
  (void)txn_gap_threshold;
  (void)time_gap_threshold;
  return MDBX_ENOSYS /* FIXME */;
}

MDBX_error_t mdbx_sync(MDBX_env_t *env) {
  mdbx_trace(">>(env = %p)", env);
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_flags32 & (MDBX_RDONLY | MDBX_ENV_TAINTED)))
    return MDBX_EACCESS;

  if (unlikely(!env->me_lck))
    return MDBX_PANIC;

  const bool outside_txn = ((env->me_flags32 & MDBX_EXCLUSIVE) == 0 &&
                            (!env->me_wpa_txn || env->me_wpa_txn->mt_owner != mdbx_thread_self()));
  if (outside_txn) {
    int rc = lck_writer_acquire(env, 0 & MDBX_NONBLOCK /* FIXME: TODO */);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  const unsigned flags = env->me_flags32 & MDBX_WRITEMAP;
  meta_t *head = meta_head(env);
  if (!META_IS_STEADY(head) || env->me_lck->li_dirty_volume) {

    if (outside_txn && env->me_lck->li_dirty_volume > pgno2bytes(env, 16 /* FIXME: define threshold */)) {
      const size_t usedbytes = pgno_align2os_bytes(env, head->mm_dxb_geo.next);

      lck_writer_release(env);

      /* LY: pre-sync without holding lock to reduce latency for writer(s) */
      int rc = (flags & MDBX_WRITEMAP) ? mdbx_msync(&env->me_dxb_mmap, 0, usedbytes, false)
                                       : mdbx_filesync(env->me_dxb_fd, false);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;

      rc = lck_writer_acquire(env, 0 & MDBX_NONBLOCK /* FIXME: TODO */);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;

      /* LY: head may be changed. */
      head = meta_head(env);
    }

    if (!META_IS_STEADY(head) || env->me_lck->li_dirty_volume) {
      mdbx_trace("meta-head %" PRIaPGNO ", %s, sync_pending %" PRIu64,
                 container_of(head, page_t, mp_data)->mp_pgno, durable_str(head),
                 env->me_lck->li_dirty_volume);
      meta_t meta = *head;
      int rc = mdbx_sync_locked(env, flags | MDBX_SHRINK_ALLOWED, &meta);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (outside_txn)
          lck_writer_release(env);
        return rc;
      }
    }
  }

  if (outside_txn)
    lck_writer_release(env);
  return MDBX_SUCCESS;
}

/* Close this write txn's cursors, give parent txn's cursors back to parent.
 *
 * [in] txn     the transaction handle.
 * [in] merge   true to keep changes to parent cursors, false to revert.
 *
 * Returns 0 on success, non-zero on failure. */
static void cursors_eot(MDBX_txn_t *txn, unsigned merge) {
  for (size_t i = txn->txn_ah_num; i > 0;) {
    for (MDBX_cursor_t *next, *mc = txn->mt_cursors[--i]; mc; mc = next) {
      mdbx_ensure(txn->mt_env, mc->mc_signature == MDBX_MC_SIGNATURE || mc->mc_signature == MDBX_MC_WAIT4EOT);
      next = mc->mc_next;
      if (mc->mc_backup) {
        mdbx_ensure(txn->mt_env, mc->mc_backup->mc_signature == MDBX_MC_BACKUP);
        cursor_unshadow(mc, merge);
        if (mc->mc_backup)
          continue;
      }

      if (mc->mc_signature == MDBX_MC_WAIT4EOT) {
        set_signature(&mc->mc_signature, ~0u);
        free(mc);
      } else {
        set_signature(&mc->mc_signature, MDBX_MC_READY4CLOSE);
        mc->primal.mc_state8 = 0 /* reset C_UNTRACK */;
      }
    }
    txn->mt_cursors[i] = nullptr;
  }
}

MDBX_error_t mdbx_txn_reset(MDBX_txn_t *txn) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(txn->mt_owner != mdbx_thread_self()))
    return MDBX_THREAD_MISMATCH;

  /* This call is only valid for read-only txns */
  if (unlikely(!(txn->mt_flags & MDBX_RDONLY)))
    return MDBX_EINVAL;

  return txn_end(txn, MDBX_END_RESET);
}

MDBX_error_t mdbx_abort(MDBX_txn_t *txn) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(/* txn->mt_owner && WTF? */ txn->mt_owner != mdbx_thread_self()))
    return MDBX_THREAD_MISMATCH;

  if (txn->mt_child)
    mdbx_abort(txn->mt_child);

  return txn_end(txn, MDBX_END_ABORT | MDBX_END_SLOT | MDBX_END_FREE);
}

/* Read the databook header before mapping it into memory. */
static int __cold mdbx_read_header(MDBX_env_t *env, meta_t *meta) {
  assert(offsetof(page_t, mp_meta) == mdbx_roundup2(PAGEHDRSZ, 8));
  memset(meta, 0, sizeof(meta_t));
  meta->mm_sign_checksum = MDBX_DATASIGN_WEAK;
  int rc = MDBX_CORRUPTED;

  /* Read twice all meta pages so we can find the latest one. */
  unsigned loop_limit = MDBX_NUM_METAS * 2;
  for (unsigned loop_count = 0; loop_count < loop_limit; ++loop_count) {
    page_t page;

    /* We don't know the page size on first time.
     * So, just guess it. */
    unsigned guess_pagesize = meta->mm_psize32;
    if (guess_pagesize == 0)
      guess_pagesize = (loop_count > MDBX_NUM_METAS) ? env->me_psize : osal_syspagesize;

    const unsigned meta_number = loop_count % MDBX_NUM_METAS;
    const unsigned offset = guess_pagesize * meta_number;

    unsigned retryleft = 42;
    while (1) {
      int err = mdbx_pread(env->me_dxb_fd, &page, sizeof(page), offset);
      if (unlikely(err != MDBX_SUCCESS)) {
        mdbx_log(MDBX_LOG_META, (err != MDBX_ENODATA) ? MDBX_LOGLEVEL_ERROR : MDBX_LOGLEVEL_NOTICE,
                 "read meta[%u,%u]: %i, %s", offset, (unsigned)sizeof(page), err, mdbx_strerror(err));
        return err;
      }

      page_t again;
      err = mdbx_pread(env->me_dxb_fd, &again, sizeof(again), offset);
      if (unlikely(err != MDBX_SUCCESS)) {
        mdbx_error("read meta[%u,%u]: %i, %s", offset, (unsigned)sizeof(again), err, mdbx_strerror(err));
        return err;
      }

      if (memcmp(&page, &again, sizeof(page)) == 0 || --retryleft == 0)
        break;

      meta_info("meta[%u] was updated, re-read it", meta_number);
    }

    if (!retryleft) {
      meta_error("meta[%u] is too volatile, skip it", meta_number);
      continue;
    }

    if (page.mp_pgno != meta_number) {
      meta_error("meta[%u] has invalid pageno %" PRIaPGNO, meta_number, page.mp_pgno);
      return MDBX_INVALID;
    }

    if (!F_ISSET(page.mp_flags16, P_META)) {
      meta_error("page #%u not a meta-page", meta_number);
      return MDBX_INVALID;
    }

    if (page.mp_meta.mm_magic_and_version != MDBX_DATA_MAGIC) {
      meta_error("meta[%u] has invalid magic/version", meta_number);
      return ((page.mp_meta.mm_magic_and_version >> 8) != MDBX_MAGIC) ? MDBX_INVALID : MDBX_VERSION_MISMATCH;
    }

    if (page.mp_meta.mm_txnid_a != page.mp_meta.mm_txnid_b) {
      meta_warning("meta[%u] not completely updated, skip it", meta_number);
      continue;
    }

    /* LY: check signature as a checksum */
    if (META_IS_STEADY(&page.mp_meta) && page.mp_meta.mm_sign_checksum != meta_sign(&page.mp_meta)) {
      meta_notice("meta[%u] has invalid steady-checksum (0x%" PRIx64 " != 0x%" PRIx64 "), skip it",
                  meta_number, page.mp_meta.mm_sign_checksum, meta_sign(&page.mp_meta));
      continue;
    }

    /* LY: check pagesize */
    if (!is_power_of_2(page.mp_meta.mm_psize32) || page.mp_meta.mm_psize32 < MIN_PAGESIZE ||
        page.mp_meta.mm_psize32 > MAX_PAGESIZE) {
      meta_notice("meta[%u] has invalid pagesize (%u), skip it", meta_number, page.mp_meta.mm_psize32);
      rc = MDBX_VERSION_MISMATCH;
      continue;
    }

    meta_verbose("read meta%" PRIaPGNO " = root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO "/%" PRIaPGNO
                 "-%" PRIaPGNO "/%" PRIaPGNO " +%u -%u, txn_id %" PRIaTXN ", %s",
                 page.mp_pgno, page.mp_meta.mm_aas[MDBX_MAIN_AAH].aa_root,
                 page.mp_meta.mm_aas[MDBX_GACO_AAH].aa_root, page.mp_meta.mm_dxb_geo.lower,
                 page.mp_meta.mm_dxb_geo.next, page.mp_meta.mm_dxb_geo.now, page.mp_meta.mm_dxb_geo.upper,
                 page.mp_meta.mm_dxb_geo.grow16, page.mp_meta.mm_dxb_geo.shrink16, page.mp_meta.mm_txnid_a,
                 durable_str(&page.mp_meta));

    /* LY: check min-pages value */
    if (page.mp_meta.mm_dxb_geo.lower < MIN_PAGENO || page.mp_meta.mm_dxb_geo.lower > MAX_PAGENO) {
      meta_notice("meta[%u] has invalid min-pages (%" PRIaPGNO "), skip it", meta_number,
                  page.mp_meta.mm_dxb_geo.lower);
      rc = MDBX_INVALID;
      continue;
    }

    /* LY: check max-pages value */
    if (page.mp_meta.mm_dxb_geo.upper < MIN_PAGENO || page.mp_meta.mm_dxb_geo.upper > MAX_PAGENO ||
        page.mp_meta.mm_dxb_geo.upper < page.mp_meta.mm_dxb_geo.lower) {
      meta_notice("meta[%u] has invalid max-pages (%" PRIaPGNO "), skip it", meta_number,
                  page.mp_meta.mm_dxb_geo.upper);
      rc = MDBX_INVALID;
      continue;
    }

    /* LY: check end_pgno */
    if (page.mp_meta.mm_dxb_geo.now < page.mp_meta.mm_dxb_geo.lower ||
        page.mp_meta.mm_dxb_geo.now > page.mp_meta.mm_dxb_geo.upper) {
      meta_notice("meta[%u] has invalid end-pageno (%" PRIaPGNO "), skip it", meta_number,
                  page.mp_meta.mm_dxb_geo.now);
      rc = MDBX_CORRUPTED;
      continue;
    }

    /* LY: check last_pgno */
    if (page.mp_meta.mm_dxb_geo.next < MIN_PAGENO || page.mp_meta.mm_dxb_geo.next - 1 > MAX_PAGENO) {
      meta_notice("meta[%u] has invalid next-pageno (%" PRIaPGNO "), skip it", meta_number,
                  page.mp_meta.mm_dxb_geo.next);
      rc = MDBX_CORRUPTED;
      continue;
    }

    /* LY: check mapsize limits */
    const uint64_t mapsize_min = page.mp_meta.mm_dxb_geo.lower * (uint64_t)page.mp_meta.mm_psize32;
    STATIC_ASSERT(MAX_MAPSIZE < SSIZE_MAX - MAX_PAGESIZE);
    if (mapsize_min < MIN_MAPSIZE || mapsize_min > MAX_MAPSIZE) {
      meta_notice("meta[%u] has invalid min-mapsize (%" PRIu64 "), skip it", meta_number, mapsize_min);
      rc = MDBX_VERSION_MISMATCH;
      continue;
    }

    STATIC_ASSERT(MIN_MAPSIZE < MAX_MAPSIZE);
    const uint64_t mapsize_max = page.mp_meta.mm_dxb_geo.upper * (uint64_t)page.mp_meta.mm_psize32;
    if (mapsize_max > MAX_MAPSIZE ||
        MAX_PAGENO < mdbx_roundup2((size_t)mapsize_max, osal_syspagesize) / (size_t)page.mp_meta.mm_psize32) {
      const uint64_t used_bytes = page.mp_meta.mm_dxb_geo.next * (uint64_t)page.mp_meta.mm_psize32;
      if (page.mp_meta.mm_dxb_geo.next - 1 > MAX_PAGENO || used_bytes > MAX_MAPSIZE) {
        meta_notice("meta[%u] has too large max-mapsize (%" PRIu64 "), skip it", meta_number, mapsize_max);
        rc = MDBX_TOO_LARGE;
        continue;
      }

      /* allow to open large DB from a 32-bit environment */
      meta_notice("meta[%u] has too large max-mapsize (%" PRIu64 "), "
                  "but size of used space still acceptable (%" PRIu64 ")",
                  meta_number, mapsize_max, used_bytes);
      page.mp_meta.mm_dxb_geo.upper = (pgno_t)(MAX_MAPSIZE / page.mp_meta.mm_psize32);
      if (page.mp_meta.mm_dxb_geo.now > page.mp_meta.mm_dxb_geo.upper)
        page.mp_meta.mm_dxb_geo.now = page.mp_meta.mm_dxb_geo.upper;
    }

    if (page.mp_meta.mm_dxb_geo.next > page.mp_meta.mm_dxb_geo.now) {
      meta_notice("meta[%u] next-pageno (%" PRIaPGNO ") is beyond end-pgno (%" PRIaPGNO "), skip it",
                  meta_number, page.mp_meta.mm_dxb_geo.next, page.mp_meta.mm_dxb_geo.now);
      rc = MDBX_CORRUPTED;
      continue;
    }

    /* LY: GACO root */
    if (page.mp_meta.mm_aas[MDBX_GACO_AAH].aa_root == P_INVALID) {
      if (page.mp_meta.mm_aas[MDBX_GACO_AAH].aa_branch_pages ||
          page.mp_meta.mm_aas[MDBX_GACO_AAH].aa_depth16 || page.mp_meta.mm_aas[MDBX_GACO_AAH].aa_entries ||
          page.mp_meta.mm_aas[MDBX_GACO_AAH].aa_leaf_pages ||
          page.mp_meta.mm_aas[MDBX_GACO_AAH].aa_overflow_pages) {
        meta_notice("meta[%u] has false-empty GACO, skip it", meta_number);
        rc = MDBX_CORRUPTED;
        continue;
      }
    } else if (page.mp_meta.mm_aas[MDBX_GACO_AAH].aa_root >= page.mp_meta.mm_dxb_geo.next) {
      meta_notice("meta[%u] has invalid GACO-root %" PRIaPGNO ", skip it", meta_number,
                  page.mp_meta.mm_aas[MDBX_GACO_AAH].aa_root);
      rc = MDBX_CORRUPTED;
      continue;
    }

    /* LY: MainDB root */
    if (page.mp_meta.mm_aas[MDBX_MAIN_AAH].aa_root == P_INVALID) {
      if (page.mp_meta.mm_aas[MDBX_MAIN_AAH].aa_branch_pages ||
          page.mp_meta.mm_aas[MDBX_MAIN_AAH].aa_depth16 || page.mp_meta.mm_aas[MDBX_MAIN_AAH].aa_entries ||
          page.mp_meta.mm_aas[MDBX_MAIN_AAH].aa_leaf_pages ||
          page.mp_meta.mm_aas[MDBX_MAIN_AAH].aa_overflow_pages) {
        mdbx_notice("meta[%u] has false-empty maindb", meta_number);
        rc = MDBX_CORRUPTED;
        continue;
      }
    } else if (page.mp_meta.mm_aas[MDBX_MAIN_AAH].aa_root >= page.mp_meta.mm_dxb_geo.next) {
      meta_notice("meta[%u] has invalid maindb-root %" PRIaPGNO ", skip it", meta_number,
                  page.mp_meta.mm_aas[MDBX_MAIN_AAH].aa_root);
      rc = MDBX_CORRUPTED;
      continue;
    }

    if (page.mp_meta.mm_features16 != 0) {
      mdbx_notice("meta[%u] has unsupported mm_features16 0x%x, skip it", meta_number,
                  page.mp_meta.mm_features16);
      rc = MDBX_INCOMPATIBLE;
      continue;
    }

    if (page.mp_meta.mm_sld_geo.lower != 0 || page.mp_meta.mm_sld_geo.now != 0 ||
        page.mp_meta.mm_sld_geo.upper != 0) {
      meta_notice("meta[%u] has unsupported (non-zeroed) SLD-geometry, skip it", meta_number);
      rc = MDBX_INCOMPATIBLE;
      continue;
    }

    if (page.mp_meta.mm_txnid_a == 0) {
      meta_warning("meta[%u] has zero txnid, skip it", meta_number);
      continue;
    }

    if (meta_ot(prefer_noweak, env, meta, &page.mp_meta)) {
      *meta = page.mp_meta;
      if (META_IS_WEAK(meta))
        loop_limit += 1; /* LY: should re-read to hush race with update */
      meta_info("latch meta[%u]", meta_number);
    }
  }

  if (META_IS_WEAK(meta)) {
    mdbx_error("no usable meta-pages, database is corrupted");
    return rc;
  }

  return MDBX_SUCCESS;
}

static int mdbx_sync_locked(MDBX_env_t *env, unsigned flags, meta_t *const pending) {
  mdbx_trace(">>(env = %p, flags = 0x%x)", env, flags);
  mdbx_assert(env, ((env->me_flags32 ^ flags) & MDBX_WRITEMAP) == 0);
  meta_t *const meta0 = metapage(env, 0);
  meta_t *const meta1 = metapage(env, 1);
  meta_t *const meta2 = metapage(env, 2);
  meta_t *const head = meta_head(env);

  mdbx_assert(env, meta_eq_mask(env) == 0);
  mdbx_assert(env, pending < metapage(env, 0) || pending > metapage(env, MDBX_NUM_METAS));
  mdbx_assert(env, (env->me_flags32 & (MDBX_RDONLY | MDBX_ENV_TAINTED)) == 0);
  mdbx_assert(env, !META_IS_STEADY(head) || env->me_lck->li_dirty_volume != 0);
  mdbx_assert(env, pending->mm_dxb_geo.next <= pending->mm_dxb_geo.now);

  const size_t usedbytes = pgno_align2os_bytes(env, pending->mm_dxb_geo.next);
  if (env->me_lck->li_autosync_threshold && env->me_lck->li_dirty_volume >= env->me_lck->li_autosync_threshold)
    flags &= MDBX_WRITEMAP | MDBX_SHRINK_ALLOWED;

  /* LY: step#1 - sync previously written/updated data-pages */
  int rc = MDBX_SIGN;
  if (env->me_lck->li_dirty_volume && (flags & MDBX_NOSYNC) == 0) {
    mdbx_trace("== do-sync: me_lck->li_dirty_volume = %" PRIu64, env->me_lck->li_dirty_volume);
    mdbx_assert(env, ((flags ^ env->me_flags32) & MDBX_WRITEMAP) == 0);
    meta_t *const steady = meta_steady(env);
    if (flags & MDBX_WRITEMAP) {
      rc = mdbx_msync(&env->me_dxb_mmap, 0, usedbytes, flags & MDBX_MAPASYNC);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
      if ((flags & MDBX_MAPASYNC) == 0) {
        if (unlikely(pending->mm_dxb_geo.next > steady->mm_dxb_geo.now)) {
          rc = mdbx_filesize_sync(env->me_dxb_fd);
          if (unlikely(rc != MDBX_SUCCESS))
            goto fail;
        }
        env->me_lck->li_dirty_volume = 0;
      }
    } else {
      rc = mdbx_filesync(env->me_dxb_fd, pending->mm_dxb_geo.next > steady->mm_dxb_geo.now);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
      env->me_lck->li_dirty_volume = 0;
    }
  }

  /* LY: check conditions to shrink datafile */
  pgno_t shrink = 0;
  const pgno_t backlog_gap = pending->mm_aas[MDBX_GACO_AAH].aa_depth16 + backlog_extragap(env);
  if ((flags & MDBX_SHRINK_ALLOWED) && pending->mm_dxb_geo.shrink16 &&
      pending->mm_dxb_geo.now - pending->mm_dxb_geo.next > pending->mm_dxb_geo.shrink16 + backlog_gap) {
    const pgno_t aligner =
        pending->mm_dxb_geo.grow16 ? pending->mm_dxb_geo.grow16 : pending->mm_dxb_geo.shrink16;
    const pgno_t with_backlog_gap = pending->mm_dxb_geo.next + backlog_gap;
    const pgno_t aligned = pgno_align2os_pgno(env, with_backlog_gap + aligner - with_backlog_gap % aligner);
    const pgno_t bottom = (aligned > pending->mm_dxb_geo.lower) ? aligned : pending->mm_dxb_geo.lower;
    if (pending->mm_dxb_geo.now > bottom) {
      shrink = pending->mm_dxb_geo.now - bottom;
      pending->mm_dxb_geo.now = bottom;
      if (meta_txnid_stable(env, head) == pending->mm_txnid_a)
        meta_set_txnid(env, pending, pending->mm_txnid_a + 1);
    }
  }

  /* Steady or Weak */
  if (env->me_lck->li_dirty_volume == 0) {
    pending->mm_sign_checksum = meta_sign(pending);
  } else {
    pending->mm_sign_checksum =
        (flags & MDBX_UTTERLY_NOSYNC) == MDBX_UTTERLY_NOSYNC ? MDBX_DATASIGN_NONE : MDBX_DATASIGN_WEAK;
  }
  mdbx_trace("== pending->mm_datasync_sign = 0x%" PRIx64, pending->mm_sign_checksum);

  meta_t *target = nullptr;
  if (meta_txnid_stable(env, head) == pending->mm_txnid_a) {
    mdbx_assert(env, memcmp(&head->mm_aas, &pending->mm_aas, sizeof(head->mm_aas)) == 0);
    mdbx_assert(env, memcmp(&head->mm_canary, &pending->mm_canary, sizeof(head->mm_canary)) == 0);
    mdbx_assert(env, memcmp(&head->mm_dxb_geo, &pending->mm_dxb_geo, sizeof(pending->mm_dxb_geo)) == 0);
    if (!META_IS_STEADY(head) && META_IS_STEADY(pending))
      target = head;
    else {
      mdbx_ensure(env, meta_eq(env, head, pending));
      mdbx_debug("skip update meta");
      return MDBX_SUCCESS;
    }
  } else if (head == meta0)
    target = meta_ancient(prefer_steady, env, meta1, meta2);
  else if (head == meta1)
    target = meta_ancient(prefer_steady, env, meta0, meta2);
  else {
    mdbx_assert(env, head == meta2);
    target = meta_ancient(prefer_steady, env, meta0, meta1);
  }

  /* LY: step#2 - update meta-page. */
  mdbx_debug("writing meta%" PRIaPGNO " = root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO "/%" PRIaPGNO
             "-%" PRIaPGNO "/%" PRIaPGNO " +%u -%u, txn_id %" PRIaTXN ", %s",
             container_of(target, page_t, mp_data)->mp_pgno, pending->mm_aas[MDBX_MAIN_AAH].aa_root,
             pending->mm_aas[MDBX_GACO_AAH].aa_root, pending->mm_dxb_geo.lower, pending->mm_dxb_geo.next,
             pending->mm_dxb_geo.now, pending->mm_dxb_geo.upper, pending->mm_dxb_geo.grow16,
             pending->mm_dxb_geo.shrink16, pending->mm_txnid_a, durable_str(pending));

  mdbx_debug("meta0: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO "/%" PRIaPGNO,
             (meta0 == head) ? "head" : (meta0 == target) ? "tail" : "stay", durable_str(meta0),
             meta_txnid_fluid(env, meta0), meta0->mm_aas[MDBX_MAIN_AAH].aa_root,
             meta0->mm_aas[MDBX_GACO_AAH].aa_root);
  mdbx_debug("meta1: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO "/%" PRIaPGNO,
             (meta1 == head) ? "head" : (meta1 == target) ? "tail" : "stay", durable_str(meta1),
             meta_txnid_fluid(env, meta1), meta1->mm_aas[MDBX_MAIN_AAH].aa_root,
             meta1->mm_aas[MDBX_GACO_AAH].aa_root);
  mdbx_debug("meta2: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO "/%" PRIaPGNO,
             (meta2 == head) ? "head" : (meta2 == target) ? "tail" : "stay", durable_str(meta2),
             meta_txnid_fluid(env, meta2), meta2->mm_aas[MDBX_MAIN_AAH].aa_root,
             meta2->mm_aas[MDBX_GACO_AAH].aa_root);

  mdbx_assert(env, !meta_eq(env, pending, meta0));
  mdbx_assert(env, !meta_eq(env, pending, meta1));
  mdbx_assert(env, !meta_eq(env, pending, meta2));

  mdbx_assert(env, ((env->me_flags32 ^ flags) & MDBX_WRITEMAP) == 0);
  mdbx_ensure(env, target == head || meta_txnid_stable(env, target) < pending->mm_txnid_a);
  if (env->me_flags32 & MDBX_WRITEMAP) {
    jitter4testing(true);
    if (likely(target != head)) {
      /* LY: 'invalidate' the meta. */
      target->mm_sign_checksum = MDBX_DATASIGN_WEAK;
      meta_update_begin(env, target, pending->mm_txnid_a);
#ifndef NDEBUG
      /* debug: provoke failure to catch a violators */
      memset(target->mm_aas, 0xCC, sizeof(target->mm_aas) + sizeof(target->mm_canary));
      jitter4testing(false);
#endif

      /* LY: update info */
      target->mm_dxb_geo = pending->mm_dxb_geo;
      target->mm_aas[MDBX_GACO_AAH] = pending->mm_aas[MDBX_GACO_AAH];
      target->mm_aas[MDBX_MAIN_AAH] = pending->mm_aas[MDBX_MAIN_AAH];
      target->mm_canary = pending->mm_canary;
      jitter4testing(true);
      mdbx_coherent_barrier();

      /* LY: 'commit' the meta */
      meta_update_end(env, target, pending->mm_txnid_b);
      jitter4testing(true);
    } else {
      /* dangerous case (target == head), only mm_sign_checksum could
       * me updated, check assertions once again */
      mdbx_ensure(env, meta_txnid_stable(env, head) == pending->mm_txnid_a && !META_IS_STEADY(head) &&
                           META_IS_STEADY(pending));
      mdbx_ensure(env, memcmp(&head->mm_dxb_geo, &pending->mm_dxb_geo, sizeof(head->mm_dxb_geo)) == 0);
      mdbx_ensure(env, memcmp(&head->mm_aas, &pending->mm_aas, sizeof(head->mm_aas)) == 0);
      mdbx_ensure(env, memcmp(&head->mm_canary, &pending->mm_canary, sizeof(head->mm_canary)) == 0);
    }
    target->mm_sign_checksum = pending->mm_sign_checksum;
    mdbx_coherent_barrier();
    jitter4testing(true);
  } else {
    rc = mdbx_pwrite(env->me_dxb_fd, pending, sizeof(meta_t), (uint8_t *)target - env->me_map);
    if (unlikely(rc != MDBX_SUCCESS)) {
    undo:
      mdbx_debug("write failed, disk error?");
      /* On a failure, the pagecache still contains the new data.
       * Try write some old data back, to prevent it from being used. */
      mdbx_pwrite(env->me_dxb_fd, (void *)target, sizeof(meta_t), (uint8_t *)target - env->me_map);
      goto fail;
    }
    mdbx_invalidate_cache(target, sizeof(meta_t));
  }

  /* LY: step#3 - sync meta-pages. */
  mdbx_assert(env, ((env->me_flags32 ^ flags) & MDBX_WRITEMAP) == 0);
  if ((flags & (MDBX_NOSYNC | MDBX_NOMETASYNC)) == 0) {
    mdbx_assert(env, ((flags ^ env->me_flags32) & MDBX_WRITEMAP) == 0);
    if (flags & MDBX_WRITEMAP) {
      const size_t offset = ((uint8_t *)container_of(head, page_t, mp_meta)) - env->me_dxb_mmap.dxb;
      const size_t paged_offset = offset & ~(osal_syspagesize - 1);
      const size_t paged_length = mdbx_roundup2(env->me_psize + offset - paged_offset, osal_syspagesize);
      rc = mdbx_msync(&env->me_dxb_mmap, paged_offset, paged_length, flags & MDBX_MAPASYNC);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    } else {
      rc = mdbx_filesync(env->me_dxb_fd, false);
      if (rc != MDBX_SUCCESS)
        goto undo;
    }
  }

  /* LY: shrink datafile if needed */
  if (unlikely(shrink)) {
    env_info("shrink to %" PRIaPGNO " pages (-%" PRIaPGNO ")", pending->mm_dxb_geo.now, shrink);
    rc = mdbx_mapresize(env, pending->mm_dxb_geo.now, pending->mm_dxb_geo.upper);
    if (MDBX_IS_ERROR(rc))
      goto fail;
  }
  return MDBX_SUCCESS;

fail:
  env->me_flags32 |= MDBX_ENV_TAINTED;
  return rc;
}

static void __cold setup_pagesize(MDBX_env_t *env, const size_t pagesize) {
  STATIC_ASSERT(SSIZE_MAX > MAX_MAPSIZE);
  STATIC_ASSERT(MIN_PAGESIZE > sizeof(page_t));
  mdbx_ensure(env, is_power_of_2(pagesize));
  mdbx_ensure(env, pagesize >= MIN_PAGESIZE);
  mdbx_ensure(env, pagesize <= MAX_PAGESIZE);
  env->me_psize = (unsigned)pagesize;

  STATIC_ASSERT(mdbx_maxfree1pg(MIN_PAGESIZE) > 42);
  STATIC_ASSERT(mdbx_maxfree1pg(MAX_PAGESIZE) < MDBX_PNL_DB_MAX);
  const intptr_t maxfree_1pg =
      (pagesize - PAGEHDRSZ - NODESIZE - sizeof(txnid_t) - sizeof(indx_t)) / sizeof(pgno_t) - 1;
  mdbx_ensure(env, maxfree_1pg > 42 && maxfree_1pg < MDBX_PNL_DB_MAX);
  env->me_maxfree_1pg = (unsigned)maxfree_1pg;

  STATIC_ASSERT(mdbx_nodemax(MIN_PAGESIZE) > 42);
  STATIC_ASSERT(mdbx_nodemax(MAX_PAGESIZE) < UINT16_MAX);
  const intptr_t nodemax = mdbx_nodemax(pagesize);
  mdbx_ensure(env, nodemax > 42 && nodemax < UINT16_MAX);
  env->me_nodemax = (unsigned)nodemax;

  STATIC_ASSERT(mdbx_maxkey(MIN_PAGESIZE) > 42);
  STATIC_ASSERT(mdbx_maxkey(MIN_PAGESIZE) < MIN_PAGESIZE);
  STATIC_ASSERT(mdbx_maxkey(MAX_PAGESIZE) > 42);
  STATIC_ASSERT(mdbx_maxkey(MAX_PAGESIZE) < MAX_PAGESIZE);
  const intptr_t maxkey_limit = mdbx_maxkey(env->me_nodemax);
  mdbx_ensure(env, maxkey_limit > 42 && (size_t)maxkey_limit < pagesize);
  env->me_keymax = (unsigned)maxkey_limit;

  env->me_psize2log = uint_log2_ceil(pagesize);
  mdbx_assert(env, pgno2bytes(env, 1) == pagesize);
  mdbx_assert(env, bytes2pgno(env, pagesize + pagesize) == 2);
}

static int __cold mdbx_bk_map(MDBX_env_t *env, size_t usedsize) {
  int rc = mdbx_mmap(env->me_flags32, &env->me_dxb_mmap, env->me_dxb_geo.now, env->me_dxb_geo.upper);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

#ifdef MADV_DONTFORK
  if (madvise(env->me_map, env->me_mapsize, MADV_DONTFORK))
    return mdbx_get_errno();
#endif

#ifdef MADV_NOHUGEPAGE
  (void)madvise(env->me_map, env->me_mapsize, MADV_NOHUGEPAGE);
#endif

#if defined(MADV_DODUMP) && defined(MADV_DONTDUMP)
  const size_t meta_length = pgno2bytes(env, MDBX_NUM_METAS);
  (void)madvise(env->me_map, meta_length, MADV_DODUMP);
  if (!(env->me_flags32 & MDBX_PAGEPERTURB))
    (void)madvise(env->me_map + meta_length, env->me_mapsize - meta_length, MADV_DONTDUMP);
#endif

#ifdef MADV_REMOVE
  if (usedsize && (env->me_flags32 & MDBX_WRITEMAP)) {
    (void)madvise(env->me_map + usedsize, env->me_mapsize - usedsize, MADV_REMOVE);
  }
#else
  (void)usedsize;
#endif

#if defined(MADV_RANDOM) && defined(MADV_WILLNEED)
  /* Turn on/off readahead. It's harmful when the databook is larger than RAM. */
  if (madvise(env->me_map, env->me_mapsize, (env->me_flags32 & MDBX_NORDAHEAD) ? MADV_RANDOM : MADV_WILLNEED))
    return mdbx_get_errno();
#endif

#ifdef USE_VALGRIND
  env->me_valgrind_handle = VALGRIND_CREATE_BLOCK(env->me_map, env->me_mapsize, "mdbx");
#endif

  return MDBX_SUCCESS;
}

LIBMDBX_API MDBX_error_t mdbx_set_geometry(MDBX_env_t *env, intptr_t size_lower, intptr_t size_now,
                                           intptr_t size_upper, intptr_t growth_step,
                                           intptr_t shrink_threshold, intptr_t pagesize) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

#if MDBX_DEBUG
  if (growth_step < 0)
    growth_step = 1;
  if (shrink_threshold < 0)
    shrink_threshold = 1;
#endif

  if (unlikely(env->me_pid != mdbx_getpid()))
    env->me_flags32 |= MDBX_ENV_TAINTED;

  if (unlikely(env->me_flags32 & MDBX_ENV_TAINTED))
    return MDBX_PANIC;

  const bool outside_txn = ((env->me_flags32 & MDBX_EXCLUSIVE) == 0 &&
                            (!env->me_wpa_txn || env->me_wpa_txn->mt_owner != mdbx_thread_self()));
  bool need_unlock = false;
  int rc = MDBX_PROBLEM;

  if (env->me_map) {
    /* env already mapped */
    if (!env->me_lck || (env->me_flags32 & MDBX_RDONLY))
      return MDBX_EACCESS;

    if (outside_txn) {
      int err = lck_writer_acquire(env, 0);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      need_unlock = true;
    }
    const meta_t *const head = meta_head(env);

    if (pagesize < 0)
      /* TODO: ioctl(BLKSSZGET) */
      pagesize = env->me_psize;
    if (pagesize != (intptr_t)env->me_psize) {
      rc = MDBX_EINVAL;
      goto bailout;
    }

    if (size_lower < 0)
      size_lower = pgno2bytes(env, head->mm_dxb_geo.lower);
    if (size_now < 0)
      size_now = pgno2bytes(env, head->mm_dxb_geo.now);
    if (size_upper < 0)
      size_upper = pgno2bytes(env, head->mm_dxb_geo.upper);
    if (growth_step < 0)
      growth_step = pgno2bytes(env, head->mm_dxb_geo.grow16);
    if (shrink_threshold < 0)
      shrink_threshold = pgno2bytes(env, head->mm_dxb_geo.shrink16);

    const size_t usedbytes = pgno2bytes(env, head->mm_dxb_geo.next);
    if ((size_t)size_upper < usedbytes) {
      rc = MDBX_MAP_FULL;
      goto bailout;
    }
    if ((size_t)size_now < usedbytes)
      size_now = usedbytes;
#if defined(_WIN32) || defined(_WIN64)
    if ((size_t)size_now < env->me_dxb_geo.now || (size_t)size_upper < env->me_dxb_geo.upper) {
      /* Windows is unable shrinking a mapped file */
      return ERROR_USER_MAPPED_FILE;
    }
#endif /* Windows */
  } else {
    /* env NOT yet mapped */
    if (unlikely(!outside_txn))
      return MDBX_PANIC;

    if (pagesize < 0) {
      pagesize = osal_syspagesize;
      if ((uintptr_t)pagesize > MAX_PAGESIZE)
        pagesize = MAX_PAGESIZE;
      mdbx_assert(env, (uintptr_t)pagesize >= MIN_PAGESIZE);
    }
  }

  if (pagesize < (intptr_t)MIN_PAGESIZE || pagesize > (intptr_t)MAX_PAGESIZE || !is_power_of_2(pagesize)) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if (size_lower < 0) {
    size_lower = MIN_MAPSIZE;
    if (MIN_MAPSIZE / pagesize < MIN_PAGENO)
      size_lower = MIN_PAGENO * pagesize;
  }

  if (size_now < 0) {
    size_now = DEFAULT_MAPSIZE;
    if (size_now < size_lower)
      size_now = size_lower;
  }

  if (size_upper < 0) {
    if ((size_t)size_now >= MAX_MAPSIZE / 2)
      size_upper = MAX_MAPSIZE;
    else if (MAX_MAPSIZE != MAX_MAPSIZE32 && (size_t)size_now >= MAX_MAPSIZE32 / 2 &&
             (size_t)size_now <= MAX_MAPSIZE32 / 4 * 3)
      size_upper = MAX_MAPSIZE32;
    else {
      size_upper = size_now + size_now;
      if ((size_t)size_upper < DEFAULT_MAPSIZE * 2)
        size_upper = DEFAULT_MAPSIZE * 2;
    }
    if ((size_t)size_upper / pagesize > MAX_PAGENO)
      size_upper = pagesize * MAX_PAGENO;
  }

  if (unlikely(size_lower < (intptr_t)MIN_MAPSIZE || size_lower > size_upper)) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if ((uint64_t)size_lower / pagesize < MIN_PAGENO) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if (unlikely((size_t)size_upper > MAX_MAPSIZE || (uint64_t)size_upper / pagesize > MAX_PAGENO)) {
    rc = MDBX_TOO_LARGE;
    goto bailout;
  }

  size_lower = mdbx_roundup2(size_lower, osal_syspagesize);
  size_upper = mdbx_roundup2(size_upper, osal_syspagesize);
  size_now = mdbx_roundup2(size_now, osal_syspagesize);

  /* LY: подбираем значение size_upper:
   *  - кратное размеру системной страницы
   *  - без нарушения MAX_MAPSIZE или MAX_PAGENO */
  while (unlikely((size_t)size_upper > MAX_MAPSIZE || (uint64_t)size_upper / pagesize > MAX_PAGENO)) {
    if ((size_t)size_upper < osal_syspagesize + MIN_MAPSIZE ||
        (size_t)size_upper < osal_syspagesize * (MIN_PAGENO + 1)) {
      /* паранойа на случай переполнения при невероятных значениях */
      rc = MDBX_EINVAL;
      goto bailout;
    }
    size_upper -= osal_syspagesize;
    if ((size_t)size_upper < (size_t)size_lower)
      size_lower = size_upper;
  }
  mdbx_assert(env, (size_upper - size_lower) % osal_syspagesize == 0);

  if (size_now < size_lower)
    size_now = size_lower;
  if (size_now > size_upper)
    size_now = size_upper;

  if (growth_step < 0) {
    growth_step = ((size_t)(size_upper - size_lower)) / 42;
    if (growth_step > size_lower)
      growth_step = size_lower;
    if (growth_step < 65536)
      growth_step = 65536;
    if ((size_t)growth_step > MEGABYTE * 16)
      growth_step = MEGABYTE * 16;
  }
  growth_step = mdbx_roundup2(growth_step, osal_syspagesize);
  if (bytes2pgno(env, growth_step) > UINT16_MAX)
    growth_step = pgno2bytes(env, UINT16_MAX);

  if (shrink_threshold < 0) {
    shrink_threshold = growth_step + growth_step;
    if (shrink_threshold < growth_step)
      shrink_threshold = growth_step;
  }
  shrink_threshold = mdbx_roundup2(shrink_threshold, osal_syspagesize);
  if (bytes2pgno(env, shrink_threshold) > UINT16_MAX)
    shrink_threshold = pgno2bytes(env, UINT16_MAX);

  /* save user's geo-params for future open/create */
  env->me_dxb_geo.lower = size_lower;
  env->me_dxb_geo.now = size_now;
  env->me_dxb_geo.upper = size_upper;
  env->me_dxb_geo.grow = growth_step;
  env->me_dxb_geo.shrink = shrink_threshold;
  rc = MDBX_SUCCESS;

  if (env->me_map) {
    /* apply new params */
    mdbx_assert(env, pagesize == (intptr_t)env->me_psize);

    const meta_t *head = meta_head(env);
    meta_t meta = *head;
    meta.mm_dxb_geo.lower = bytes2pgno(env, env->me_dxb_geo.lower);
    meta.mm_dxb_geo.now = bytes2pgno(env, env->me_dxb_geo.now);
    meta.mm_dxb_geo.upper = bytes2pgno(env, env->me_dxb_geo.upper);
    meta.mm_dxb_geo.grow16 = (uint16_t)bytes2pgno(env, env->me_dxb_geo.grow);
    meta.mm_dxb_geo.shrink16 = (uint16_t)bytes2pgno(env, env->me_dxb_geo.shrink);

    mdbx_assert(env, env->me_dxb_geo.lower >= MIN_MAPSIZE);
    mdbx_assert(env, meta.mm_dxb_geo.lower >= MIN_PAGENO);
    mdbx_assert(env, env->me_dxb_geo.upper <= MAX_MAPSIZE);
    mdbx_assert(env, meta.mm_dxb_geo.upper <= MAX_PAGENO);
    mdbx_assert(env, meta.mm_dxb_geo.now >= meta.mm_dxb_geo.next);
    mdbx_assert(env, env->me_dxb_geo.upper >= env->me_dxb_geo.lower);
    mdbx_assert(env, meta.mm_dxb_geo.upper >= meta.mm_dxb_geo.now);
    mdbx_assert(env, meta.mm_dxb_geo.now >= meta.mm_dxb_geo.lower);
    mdbx_assert(env, meta.mm_dxb_geo.grow16 == bytes2pgno(env, env->me_dxb_geo.grow));
    mdbx_assert(env, meta.mm_dxb_geo.shrink16 == bytes2pgno(env, env->me_dxb_geo.shrink));

    if (memcmp(&meta.mm_dxb_geo, &head->mm_dxb_geo, sizeof(meta.mm_dxb_geo))) {
      if (meta.mm_dxb_geo.now != head->mm_dxb_geo.now || meta.mm_dxb_geo.upper != head->mm_dxb_geo.upper) {
        rc = mdbx_mapresize(env, meta.mm_dxb_geo.now, meta.mm_dxb_geo.upper);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        head = /* base address could be changed */ meta_head(env);
      }
      if (env->me_lck)
        env->me_lck->li_dirty_volume += env->me_psize;
      meta_set_txnid(env, &meta, meta_txnid_stable(env, head) + 1);
      rc = mdbx_sync_locked(env, env->me_flags32, &meta);
    }
  } else if (pagesize != (intptr_t)env->me_psize) {
    setup_pagesize(env, pagesize);
  }

bailout:
  if (need_unlock)
    lck_writer_release(env);
  return rc;
}

MDBX_error_t __cold mdbx_set_mapsize(MDBX_env_t *env, size_t size) {
  return mdbx_set_geometry(env, -1, size, -1, -1, -1, -1);
}

MDBX_error_t mdbx_set_maxhandles(MDBX_env_t *env, size_t count) {
  if (unlikely(count > MAX_AAH))
    return MDBX_EINVAL;

  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_map))
    return MDBX_EPERM;

  env->env_ah_max = (unsigned)(count + CORE_AAH);
  return MDBX_SUCCESS;
}

MDBX_error_t mdbx_set_maxreaders(MDBX_env_t *env, unsigned readers) {
  if (unlikely(readers < 1 || readers > INT16_MAX))
    return MDBX_EINVAL;

  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_map))
    return MDBX_EPERM;

  env->me_maxreaders = readers;
  return MDBX_SUCCESS;
}

MDBX_error_t __cold mdbx_get_maxreaders(MDBX_env_t *env, unsigned *readers) {
  if (!env || !readers)
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  *readers = env->me_maxreaders;
  return MDBX_SUCCESS;
}

/* Further setup required for opening an MDBX databook */
static MDBX_error_t __cold mdbx_setup_dxb(MDBX_env_t *env, const MDBX_seize_t seize) {
  meta_t meta;
  int rc = MDBX_SUCCESS;
  int err = mdbx_read_header(env, &meta);
  if (unlikely(err != MDBX_SUCCESS)) {
    if (seize != MDBX_SEIZE_EXCLUSIVE_FIRST || err != MDBX_ENODATA || (env->me_flags32 & MDBX_RDONLY) != 0)
      return err;

    mdbx_debug("create new database");
    rc = /* new database */ MDBX_SIGN;

    if (!env->me_dxb_geo.now) {
      /* set defaults if not configured */
      err = mdbx_set_mapsize(env, DEFAULT_MAPSIZE);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }

    void *buffer = calloc(MDBX_NUM_METAS, env->me_psize);
    if (!buffer)
      return MDBX_ENOMEM;

    meta = init_metas(env, buffer)->mp_meta;
    err = mdbx_pwrite(env->me_dxb_fd, buffer, env->me_psize * MDBX_NUM_METAS, 0);
    free(buffer);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    err = mdbx_ftruncate(env->me_dxb_fd, env->me_dxb_geo.now);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

#ifndef NDEBUG /* just for checking */
    err = mdbx_read_header(env, &meta);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
#endif
  }

  env_info("header: root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
           " +%u -%u, txn_id %" PRIaTXN ", %s",
           meta.mm_aas[MDBX_MAIN_AAH].aa_root, meta.mm_aas[MDBX_GACO_AAH].aa_root, meta.mm_dxb_geo.lower,
           meta.mm_dxb_geo.next, meta.mm_dxb_geo.now, meta.mm_dxb_geo.upper, meta.mm_dxb_geo.grow16,
           meta.mm_dxb_geo.shrink16, meta.mm_txnid_a, durable_str(&meta));

  setup_pagesize(env, meta.mm_psize32);
  const size_t used_bytes = pgno2bytes(env, meta.mm_dxb_geo.next);
  if ((env->me_flags32 & MDBX_RDONLY) || !IS_SEIZE_EXCLUSIVE(seize)) {
    /* apply geometry from db */
    const intptr_t meta_page_size = meta.mm_psize32;
    err = mdbx_set_geometry(env, meta.mm_dxb_geo.lower * meta_page_size, meta.mm_dxb_geo.now * meta_page_size,
                            meta.mm_dxb_geo.upper * meta_page_size, meta.mm_dxb_geo.grow16 * meta_page_size,
                            meta.mm_dxb_geo.shrink16 * meta_page_size, meta_page_size);
    if (unlikely(err != MDBX_SUCCESS)) {
      mdbx_error("could not use present dbsize-params from db");
      return MDBX_INCOMPATIBLE;
    }
  } else if (env->me_dxb_geo.now) {
    /* silently growth to last used page */
    if (env->me_dxb_geo.now < used_bytes)
      env->me_dxb_geo.now = used_bytes;
    if (env->me_dxb_geo.upper < used_bytes)
      env->me_dxb_geo.upper = used_bytes;

    /* apply preconfigured params, but only if substantial changes:
     *  - upper or lower limit changes
     *  - shrink theshold or growth step
     * But ignore just chagne just a 'now/current' size. */
    if (bytes_align2os_bytes(env, env->me_dxb_geo.upper) != pgno_align2os_bytes(env, meta.mm_dxb_geo.upper) ||
        bytes_align2os_bytes(env, env->me_dxb_geo.lower) != pgno_align2os_bytes(env, meta.mm_dxb_geo.lower) ||
        bytes_align2os_bytes(env, env->me_dxb_geo.shrink) !=
            pgno_align2os_bytes(env, meta.mm_dxb_geo.shrink16) ||
        bytes_align2os_bytes(env, env->me_dxb_geo.grow) != pgno_align2os_bytes(env, meta.mm_dxb_geo.grow16)) {

      if (env->me_dxb_geo.shrink && env->me_dxb_geo.now > used_bytes)
        /* pre-shrink if enabled */
        env->me_dxb_geo.now = used_bytes + env->me_dxb_geo.shrink - used_bytes % env->me_dxb_geo.shrink;

      /* При изменении параметров mdbx_set_geometry() обновляет мета и выполняет sync, что не допустимо
       * до анализа номеров txnid и выполнения rollback. Но здесь этот вызов mdbx_set_geometry() безопасен,
       * так как при нулевом me_map описанных действий не будет. */
      err = mdbx_set_geometry(env, env->me_dxb_geo.lower, env->me_dxb_geo.now, env->me_dxb_geo.upper,
                              env->me_dxb_geo.grow, env->me_dxb_geo.shrink, meta.mm_psize32);
      if (unlikely(err != MDBX_SUCCESS)) {
        mdbx_error("could not apply preconfigured dbsize-params to db");
        return MDBX_INCOMPATIBLE;
      }

      /* update meta fields */
      meta.mm_dxb_geo.now = bytes2pgno(env, env->me_dxb_geo.now);
      meta.mm_dxb_geo.lower = bytes2pgno(env, env->me_dxb_geo.lower);
      meta.mm_dxb_geo.upper = bytes2pgno(env, env->me_dxb_geo.upper);
      meta.mm_dxb_geo.grow16 = (uint16_t)bytes2pgno(env, env->me_dxb_geo.grow);
      meta.mm_dxb_geo.shrink16 = (uint16_t)bytes2pgno(env, env->me_dxb_geo.shrink);

      env_info("amended: root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO "/%" PRIaPGNO "-%" PRIaPGNO
               "/%" PRIaPGNO " +%u -%u, txn_id %" PRIaTXN ", %s",
               meta.mm_aas[MDBX_MAIN_AAH].aa_root, meta.mm_aas[MDBX_GACO_AAH].aa_root, meta.mm_dxb_geo.lower,
               meta.mm_dxb_geo.next, meta.mm_dxb_geo.now, meta.mm_dxb_geo.upper, meta.mm_dxb_geo.grow16,
               meta.mm_dxb_geo.shrink16, meta.mm_txnid_a, durable_str(&meta));
    }
    mdbx_ensure(env, meta.mm_dxb_geo.now >= meta.mm_dxb_geo.next);
  } else {
    /* geometry-params not pre-configured by user,
     * get current values from a meta. */
    env->me_dxb_geo.now = pgno2bytes(env, meta.mm_dxb_geo.now);
    env->me_dxb_geo.lower = pgno2bytes(env, meta.mm_dxb_geo.lower);
    env->me_dxb_geo.upper = pgno2bytes(env, meta.mm_dxb_geo.upper);
    env->me_dxb_geo.grow = pgno2bytes(env, meta.mm_dxb_geo.grow16);
    env->me_dxb_geo.shrink = pgno2bytes(env, meta.mm_dxb_geo.shrink16);
  }

  uint64_t filesize_before_mmap;
  err = mdbx_filesize(env->me_dxb_fd, &filesize_before_mmap);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const size_t expected_bytes = mdbx_roundup2(pgno2bytes(env, meta.mm_dxb_geo.now), osal_syspagesize);
  mdbx_ensure(env, expected_bytes >= used_bytes);
  if (filesize_before_mmap != expected_bytes) {
    if (!IS_SEIZE_EXCLUSIVE(seize)) {
      env_info("filesize mismatch (expect %" PRIuSIZE "/%" PRIaPGNO ", have %" PRIu64 "/%" PRIaPGNO "), "
               "assume collision in non-exclusive mode",
               expected_bytes, bytes2pgno(env, expected_bytes), filesize_before_mmap,
               bytes2pgno(env, (size_t)filesize_before_mmap));
    } else {
      mdbx_notice("filesize mismatch (expect %" PRIuSIZE "/%" PRIaPGNO ", have %" PRIu64 "/%" PRIaPGNO ")",
                  expected_bytes, bytes2pgno(env, expected_bytes), filesize_before_mmap,
                  bytes2pgno(env, (size_t)filesize_before_mmap));
      if (filesize_before_mmap < used_bytes) {
        mdbx_error("last-page beyond end-of-file (last %" PRIaPGNO ", have %" PRIaPGNO ")",
                   meta.mm_dxb_geo.next, bytes2pgno(env, (size_t)filesize_before_mmap));
        return MDBX_CORRUPTED;
      }

      if (env->me_flags32 & MDBX_RDONLY) {
        mdbx_notice("ignore filesize mismatch in readonly-mode");
      } else {
        env_info("resize datafile to %" PRIuSIZE " bytes, %" PRIaPGNO " pages", expected_bytes,
                 bytes2pgno(env, expected_bytes));
        err = mdbx_ftruncate(env->me_dxb_fd, expected_bytes);
        if (unlikely(err != MDBX_SUCCESS)) {
          mdbx_error("error %d, while resize datafile to %" PRIuSIZE " bytes, %" PRIaPGNO " pages", rc,
                     expected_bytes, bytes2pgno(env, expected_bytes));
          return err;
        }
        filesize_before_mmap = expected_bytes;
      }
    }
  }

  log_info(MDBX_LOG_MISC, "current boot-id %" PRIx64 "-%" PRIx64 " (%savailable)", osal_bootid_value.qwords[0],
           osal_bootid_value.qwords[1],
           (osal_bootid_value.qwords[0] | osal_bootid_value.qwords[1]) ? "" : "not-");

  err = mdbx_bk_map(env, IS_SEIZE_EXCLUSIVE(seize) ? expected_bytes : 0);
  if (err != MDBX_SUCCESS)
    return err;

  const unsigned meta_clash_mask = meta_eq_mask(env);
  if (meta_clash_mask) {
    mdbx_error("meta-pages are clashed: mask 0x%d", meta_clash_mask);
    return MDBX_WANNA_RECOVERY;
  }

  while (1) {
    meta_t *head = meta_head(env);
    const txnid_t head_txnid = meta_txnid_fluid(env, head);
    if (head_txnid == meta.mm_txnid_a)
      break;

    /* Don't try to rollback for seize == MDBX_SEIZE_EXCLUSIVE_LIVE,
     * i.e. when DB was closed by other process right nows  */
    if (seize == MDBX_SEIZE_EXCLUSIVE_FIRST || seize == MDBX_SEIZE_NOLCK) {
      assert(META_IS_STEADY(&meta) && !META_IS_STEADY(head));
      if (env->me_flags32 & MDBX_RDONLY) {
        mdbx_error("rollback needed: (from head %" PRIaTXN " to steady %" PRIaTXN
                   "), but unable in read-only mode",
                   head_txnid, meta.mm_txnid_a);
        return MDBX_WANNA_RECOVERY /* LY: could not recovery/rollback */;
      }

      if ((osal_bootid_value.qwords[0] | osal_bootid_value.qwords[1]) /* sys boot-id available */ &&
          osal_bootid_value.qwords[0] == env->me_lck->li_bootid.qwords[0] &&
          osal_bootid_value.qwords[1] == env->me_lck->li_bootid.qwords[1] /* and match */) {
        env_notice("opening after a unclean shutdown, but boot-id is match, rollback not needed");
        break;
      }

      const meta_t *const meta0 = metapage(env, 0);
      const meta_t *const meta1 = metapage(env, 1);
      const meta_t *const meta2 = metapage(env, 2);
      txnid_t undo_txnid = 0;
      while ((head != meta0 && meta_txnid_fluid(env, meta0) == undo_txnid) ||
             (head != meta1 && meta_txnid_fluid(env, meta1) == undo_txnid) ||
             (head != meta2 && meta_txnid_fluid(env, meta2) == undo_txnid))
        undo_txnid += 1;
      if (unlikely(undo_txnid >= meta.mm_txnid_a)) {
        mdbx_fatal(env, MDBX_LOG_ENV, "rollback failed: no suitable txnid (0,1,2) < %" PRIaTXN,
                   meta.mm_txnid_a);
        return MDBX_PROBLEM /* LY: could not recovery/rollback */;
      }

      /* LY: rollback weak checkpoint */
      mdbx_trace("rollback: from %" PRIaTXN ", to %" PRIaTXN " as %" PRIaTXN, head_txnid, meta.mm_txnid_a,
                 undo_txnid);
      mdbx_ensure(env, head_txnid == meta_txnid_stable(env, head));

      if (env->me_flags32 & MDBX_WRITEMAP) {
        head->mm_txnid_a = undo_txnid;
        head->mm_sign_checksum = MDBX_DATASIGN_WEAK;
        head->mm_txnid_b = undo_txnid;
        const size_t offset = ((uint8_t *)container_of(head, page_t, mp_meta)) - env->me_dxb_mmap.dxb;
        const size_t paged_offset = offset & ~(osal_syspagesize - 1);
        const size_t paged_length = mdbx_roundup2(env->me_psize + offset - paged_offset, osal_syspagesize);
        err = mdbx_msync(&env->me_dxb_mmap, paged_offset, paged_length, false);
      } else {
        meta_t rollback = *head;
        meta_set_txnid(env, &rollback, undo_txnid);
        rollback.mm_sign_checksum = MDBX_DATASIGN_WEAK;
        err = mdbx_pwrite(env->me_dxb_fd, &rollback, sizeof(meta_t), (uint8_t *)head - (uint8_t *)env->me_map);
      }
      if (err)
        return err;

      mdbx_invalidate_cache(env->me_map, pgno2bytes(env, MDBX_NUM_METAS));
      mdbx_ensure(env, undo_txnid == meta_txnid_fluid(env, head));
      mdbx_ensure(env, 0 == meta_eq_mask(env));
      continue;
    }

    if (!env->me_lck) {
      /* LY: without-lck (read-only) mode, so it is imposible that other
       * process made weak checkpoint. */
      mdbx_error("without-lck, unable recovery/rollback");
      return MDBX_WANNA_RECOVERY;
    }

    /* LY: assume just have a collision with other running process,
     *     or someone make a weak checkpoint */
    env_info("assume collision or online weak checkpoint");
    break;
  }

  const meta_t *head = meta_head(env);
  if (IS_SEIZE_EXCLUSIVE(seize)) {
    /* set current bootid, even it not available */
    env->me_lck->li_bootid = osal_bootid_value;

    /* re-check file size after mmap */
    uint64_t filesize_after_mmap;
    err = mdbx_filesize(env->me_dxb_fd, &filesize_after_mmap);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    if (filesize_after_mmap != expected_bytes) {
      if (filesize_after_mmap != filesize_before_mmap)
        env_info("datafile resized by system to %" PRIu64 " bytes", filesize_after_mmap);
      if (filesize_after_mmap % osal_syspagesize || filesize_after_mmap > env->me_dxb_geo.upper ||
          filesize_after_mmap < used_bytes) {
        env_info("unacceptable/unexpected  datafile size %" PRIu64, filesize_after_mmap);
        return MDBX_PROBLEM;
      }
      if ((env->me_flags32 & MDBX_RDONLY) == 0) {
        meta.mm_dxb_geo.now = bytes2pgno(env, env->me_dxb_geo.now = (size_t)filesize_after_mmap);
        env_info("update meta-geo to filesize %" PRIuPTR " bytes, %" PRIaPGNO " pages", env->me_dxb_geo.now,
                 meta.mm_dxb_geo.now);
      }
    }

    if (memcmp(&meta.mm_dxb_geo, &head->mm_dxb_geo, sizeof(meta.mm_dxb_geo))) {
      const txnid_t txnid = meta_txnid_stable(env, head);
      env_info("updating meta.geo: "
               "from l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO "/s%u-g%u (txn#%" PRIaTXN "), "
               "to l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO "/s%u-g%u (txn#%" PRIaTXN ")",
               head->mm_dxb_geo.lower, head->mm_dxb_geo.now, head->mm_dxb_geo.upper, head->mm_dxb_geo.shrink16,
               head->mm_dxb_geo.grow16, txnid, meta.mm_dxb_geo.lower, meta.mm_dxb_geo.now,
               meta.mm_dxb_geo.upper, meta.mm_dxb_geo.shrink16, meta.mm_dxb_geo.grow16, txnid + 1);

      mdbx_ensure(env, meta_eq(env, &meta, head));
      meta_set_txnid(env, &meta, txnid + 1);
      env->me_lck->li_dirty_volume += env->me_psize;
      err = mdbx_sync_locked(env, env->me_flags32 | MDBX_SHRINK_ALLOWED, &meta);
      if (err) {
        env_info("error %d, while updating meta.geo: "
                 "from l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO "/s%u-g%u (txn#%" PRIaTXN "), "
                 "to l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO "/s%u-g%u (txn#%" PRIaTXN ")",
                 err, head->mm_dxb_geo.lower, head->mm_dxb_geo.now, head->mm_dxb_geo.upper,
                 head->mm_dxb_geo.shrink16, head->mm_dxb_geo.grow16, txnid, meta.mm_dxb_geo.lower,
                 meta.mm_dxb_geo.now, meta.mm_dxb_geo.upper, meta.mm_dxb_geo.shrink16, meta.mm_dxb_geo.grow16,
                 txnid + 1);
        return err;
      }
    }
  }

  env->env_ahe_array[MDBX_GACO_AAH].ax_flags16 = MDBX_INTEGERKEY;
  env->env_ahe_array[MDBX_GACO_AAH].ax_kcmp = cmp_int_aligned /* aligned MDBX_INTEGERKEY */;
  env->env_ahe_array[MDBX_GACO_AAH].ax_refcounter16 = 1;
  assert(env->env_ahe_array[MDBX_GACO_AAH].ax_aah == MDBX_GACO_AAH);
  env->env_ahe_array[MDBX_GACO_AAH].ax_until = MAX_TXNID;
  env->env_ahe_array[MDBX_GACO_AAH].ax_ident = mdbx_str2iov("@GaCo");

  const uint16_t main_aa_flags = get_le16_aligned(&meta.mm_db_flags16);
  env->env_ahe_array[MDBX_MAIN_AAH].ax_flags16 = main_aa_flags;
  env->env_ahe_array[MDBX_MAIN_AAH].ax_kcmp = default_keycmp(main_aa_flags);
  env->env_ahe_array[MDBX_MAIN_AAH].ax_dcmp = default_datacmp(main_aa_flags);
  env->env_ahe_array[MDBX_MAIN_AAH].ax_refcounter16 = 1;
  env->env_ahe_array[MDBX_MAIN_AAH].ax_aah = MDBX_MAIN_AAH;
  env->env_ahe_array[MDBX_MAIN_AAH].ax_until = MAX_TXNID;
  env->env_ahe_array[MDBX_MAIN_AAH].ax_ident = mdbx_str2iov("@Main");

  return rc;
}

/****************************************************************************/

/* Open and/or initialize the lock region for the databook. */
static MDBX_seize_result_t __cold setup_lck(MDBX_env_t *env, const char *lck_pathname, mode_t mode) {
  assert(env->me_dxb_fd != MDBX_INVALID_FD);
  assert(env->me_lck_fd == MDBX_INVALID_FD);

  int err = mdbx_openfile(lck_pathname, O_RDWR | O_CREAT, mode, &env->me_lck_fd);
  if (unlikely(err != MDBX_SUCCESS)) {
    if (err != MDBX_EROFS || (env->me_flags32 & MDBX_RDONLY) == 0)
      return seize_failed(err);
    /* LY: without-lck mode (e.g. on read-only filesystem) */
    env->me_lck_fd = MDBX_INVALID_FD;
    env->me_oldest = &env->me_oldest_stub;
    env->me_maxreaders = UINT_MAX;
  }

  /* Try to get exclusive lock. If we succeed, then
   * nobody is using the lock region and we should initialize it. */
  const MDBX_seize_result_t rc = env->ops.locking.ops_seize(env, env->me_flags32);
  if (unlikely(rc.err != MDBX_SUCCESS))
    return rc;

  if (rc.seize == MDBX_SEIZE_NOLCK) {
    mdbx_debug("seize %s ", "lockless-readonly");
    return rc;
  }
  mdbx_debug("seize %s ", IS_SEIZE_EXCLUSIVE(rc.seize) ? "exclusive" : "shared");

  uint64_t size;
  err = mdbx_filesize(env->me_lck_fd, &size);
  if (unlikely(err != MDBX_SUCCESS))
    return seize_failed(err);

  if (rc.seize == MDBX_SEIZE_EXCLUSIVE_FIRST) {
    uint64_t wanna = mdbx_roundup2((env->me_maxreaders - 1) * sizeof(MDBX_reader_t) + sizeof(MDBX_lockinfo_t),
                                   osal_syspagesize);
#ifndef NDEBUG
    err = mdbx_ftruncate(env->me_lck_fd, size = 0);
    if (unlikely(err != MDBX_SUCCESS))
      return seize_failed(err);
#endif
    jitter4testing(false);

    if (size != wanna) {
      err = mdbx_ftruncate(env->me_lck_fd, wanna);
      if (unlikely(err != MDBX_SUCCESS))
        return seize_failed(err);
      size = wanna;
    }
  } else if (size > SSIZE_MAX || (size & (osal_syspagesize - 1)) || size < osal_syspagesize) {
    mdbx_notice("lck-file has invalid size %" PRIu64 " bytes", size);
    return seize_failed(MDBX_PROBLEM);
  }

  const size_t maxreaders = ((size_t)size - sizeof(MDBX_lockinfo_t)) / sizeof(MDBX_reader_t) + 1;
  if (maxreaders > UINT16_MAX) {
    mdbx_error("lck-size too big (up to %" PRIuPTR " readers)", maxreaders);
    return seize_failed(MDBX_PROBLEM);
  }
  env->me_maxreaders = (unsigned)maxreaders;

  err = mdbx_mmap(MDBX_WRITEMAP, &env->me_lck_mmap, (size_t)size, (size_t)size);
  if (unlikely(err != MDBX_SUCCESS))
    return seize_failed(err);

#ifdef MADV_DODUMP
  (void)madvise(env->me_lck, size, MADV_DODUMP);
#endif

#ifdef MADV_DONTFORK
  if (madvise(env->me_lck, size, MADV_DONTFORK) < 0)
    return seize_failed(mdbx_get_errno());
#endif

#ifdef MADV_WILLNEED
  if (madvise(env->me_lck, size, MADV_WILLNEED) < 0)
    return seize_failed(mdbx_get_errno());
#endif

#ifdef MADV_RANDOM
  if (madvise(env->me_lck, size, MADV_RANDOM) < 0)
    return seize_failed(mdbx_get_errno());
#endif

  if (rc.seize == MDBX_SEIZE_EXCLUSIVE_FIRST) {
    /* LY: exlcusive mode, init lck */
    memset(env->me_lck, 0, (size_t)size);
    err = env->ops.locking.ops_init(env);
    if (err)
      return seize_failed(err);

    env->me_lck->li_magic_and_version = MDBX_LOCK_MAGIC;
    env->me_lck->li_os_and_format = MDBX_LOCK_FORMAT;
  } else {
    if (env->me_lck->li_magic_and_version != MDBX_LOCK_MAGIC) {
      mdbx_error("lock region has invalid magic/version");
      err = ((env->me_lck->li_magic_and_version >> 8) != MDBX_MAGIC) ? MDBX_INVALID : MDBX_VERSION_MISMATCH;
      return seize_failed(err);
    }
    if (env->me_lck->li_os_and_format != MDBX_LOCK_FORMAT) {
      mdbx_error("lock region has os/format 0x%" PRIx32 ", expected 0x%" PRIx32, env->me_lck->li_os_and_format,
                 MDBX_LOCK_FORMAT);
      return seize_failed(MDBX_VERSION_MISMATCH);
    }
  }

  if (rc.seize >= MDBX_SEIZE_EXCLUSIVE_CONTINUE)
    lck_seized_exclusive(env);

  env->me_oldest = &env->me_lck->li_oldest;
  return rc;
}

MDBX_error_t mdbx_open(MDBX_env_t *env, const char *path, MDBX_flags_t flags, mode_t mode4create) {
  int rc = mdbx_is_directory(path);
  if (MDBX_IS_ERROR(rc)) {
    if (rc != MDBX_ENOENT || (flags & MDBX_CREATE) == 0)
      return rc;
  }

  STATIC_ASSERT((MDBX_REGIME_CHANGEABLE & MDBX_REGIME_CHANGELESS) == 0);
  STATIC_ASSERT(((MDBX_AA_FLAGS | MDBX_DB_FLAGS | MDBX_AA_OPEN_FLAGS) &
                 (MDBX_REGIME_CHANGEABLE | MDBX_REGIME_CHANGELESS)) == 0);
  STATIC_ASSERT((MDBX_TXN_STATE_FLAGS & MDBX_TXN_BEGIN_FLAGS) == 0);

  return mdbx_open_ex(env, nullptr /* required base address */, path /* dxb pathname */,
                      (rc == MDBX_SUCCESS) ? NULL : "." /* lck pathname */, nullptr /* sld pathname */,
                      flags /* regime flags */, MDBX_REGIME_PRINCIPAL_FLAGS /* regime check mask */,
                      nullptr /* regime present */, mode4create);
}

MDBX_error_t __cold mdbx_open_ex(MDBX_env_t *env, void *required_base_address, const char *dxb_pathname,
                                 const char *lck_pathname, const char *sld_pathname, unsigned regime_flags,
                                 unsigned regime_check_mask, unsigned *regime_present, mode_t mode4create) {
  if (unlikely(!env || !dxb_pathname))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;
  if (unlikely(env->me_pid != mdbx_getpid()))
    return MDBX_PANIC;

  if (env->me_dxb_fd != MDBX_INVALID_FD || (regime_flags & ~MDBX_DB_OPEN_FLAGS) != 0)
    return MDBX_EINVAL;

  if ((regime_flags & (MDBX_RDONLY | MDBX_CREATE)) == (MDBX_RDONLY | MDBX_CREATE))
    return MDBX_EINVAL;

  if (sld_pathname && (sld_pathname[0] == '\0' || strcmp(sld_pathname, ".") == 0))
    sld_pathname = nullptr;

  if (sld_pathname && required_base_address)
    /* FIXME: This is correct (and could be implemented),
     * in the case databook size is fixed */
    return MDBX_EINVAL;

  if (sld_pathname || required_base_address)
    return MDBX_ENOSYS /* FIXME: TODO - implement both features */;

  char *molded_lck_filename = nullptr;
  char *molded_dxb_filename = nullptr;
  int rc = mdbx_is_directory(dxb_pathname);
  if (!lck_pathname) {
    /* subdir mode:
     *   - dxb and lck resides in directory given by dxb_pathname. */
    if (MDBX_IS_ERROR(rc))
      goto bailout;
    if (rc != MDBX_SUCCESS) {
      rc = MDBX_EINVAL /* dxb_pathname is not a directory */;
      goto bailout;
    }

    if (mdbx_asprintf(&molded_dxb_filename, "%s%s", dxb_pathname, MDBX_PATH_SEPARATOR MDBX_DATANAME) < 0 ||
        mdbx_asprintf(&molded_lck_filename, "%s%s", dxb_pathname, MDBX_PATH_SEPARATOR MDBX_LOCKNAME) < 0) {
      rc = mdbx_get_errno();
      goto bailout;
    }
    dxb_pathname = molded_dxb_filename;
    lck_pathname = molded_lck_filename;
  } else if (lck_pathname[0] == '\0' || strcmp(lck_pathname, ".") == 0) {
    /* no-subdir mode:
     *   - dxb given by dxb_pathname,
     *   - lck is same with MDBX_LOCK_SUFFIX appended. */
    if (rc == MDBX_SUCCESS) {
      rc = MDBX_EINVAL /* dxb_pathname is a directory */;
      goto bailout;
    }
    if (mdbx_asprintf(&molded_lck_filename, "%s%s", dxb_pathname, MDBX_LOCK_SUFFIX) < 0) {
      rc = mdbx_get_errno();
      goto bailout;
    }
    lck_pathname = molded_lck_filename;
  } else {
    /* separate mode: dxb given by dxb_pathname, lck given by lck_pathname */
    if (rc == MDBX_SUCCESS || mdbx_is_directory(lck_pathname) == MDBX_SUCCESS) {
      rc = MDBX_EINVAL /* dxb_pathname or lck_pathname is a directory */;
      goto bailout;
    }
  }

  if (regime_flags & MDBX_RDONLY) {
    /* LY: silently ignore irrelevant flags when
     * we're only getting read access */
    regime_flags &= ~(MDBX_WRITEMAP | MDBX_MAPASYNC | MDBX_NOSYNC | MDBX_NOMETASYNC | MDBX_COALESCE |
                      MDBX_LIFORECLAIM | MDBX_NOMEMINIT);
  } else {
    env->me_free_pgs = mdbx_pnl_alloc(MDBX_PNL_UM_MAX);
    env->me_dirtylist = (MDBX_ID2L)calloc(MDBX_PNL_UM_SIZE, sizeof(MDBX_ID2));
    if (unlikely(!env->me_free_pgs || !env->me_dirtylist)) {
      rc = MDBX_ENOMEM;
      goto bailout;
    }
  }

  regime_check_mask &= MDBX_REGIME_PRINCIPAL_FLAGS /* silently ignore all non-regime flags */;
  env->me_flags32 = regime_flags | MDBX_ENV_ACTIVE;

  const size_t pathname_buflen =
      strlen(lck_pathname) + 1 + strlen(dxb_pathname) + 1 + (sld_pathname ? strlen(sld_pathname) : 0) + 1;
  env->me_pathname_buf = malloc(pathname_buflen);
  env->env_ahe_array = calloc(env->env_ah_max, sizeof(ahe_t));
  if (!(env->env_ahe_array && env->me_pathname_buf)) {
    rc = MDBX_ENOMEM;
    goto bailout;
  }

  char *append_ptr = env->me_pathname_buf;
  append_ptr = mdbx_stpcpy(env->me_pathname_dxb = append_ptr, dxb_pathname);
  append_ptr = mdbx_stpcpy(env->me_pathname_lck = append_ptr, lck_pathname);
  if (sld_pathname)
    append_ptr = mdbx_stpcpy(env->me_pathname_sld = append_ptr, sld_pathname);
  else
    *append_ptr++ = '\0';
  mdbx_assert(env, append_ptr == env->me_pathname_buf + pathname_buflen);

  int oflags;
  if (regime_flags & MDBX_RDONLY)
    oflags = O_RDONLY;
  else if (regime_flags & MDBX_CREATE)
    oflags = O_RDWR | O_CREAT;
  else
    oflags = O_RDWR;

  rc = mdbx_openfile(env->me_pathname_dxb, oflags, mode4create, &env->me_dxb_fd);
  if (rc != MDBX_SUCCESS)
    goto bailout;

  const MDBX_seize_result_t lck_rc = setup_lck(env, env->me_pathname_lck, mode4create);
  if (unlikely(lck_rc.err != MDBX_SUCCESS)) {
    rc = lck_rc.err;
    goto bailout;
  }

  const MDBX_error_t dxb_rc = mdbx_setup_dxb(env, lck_rc.seize);
  if (unlikely(MDBX_IS_ERROR(dxb_rc))) {
    rc = dxb_rc;
    goto bailout;
  }

  mdbx_debug("opened dbenv %p", (void *)env);
  if (IS_SEIZE_EXCLUSIVE(lck_rc.seize)) {
    /* setup regime */
    env->me_lck->li_regime = env->me_flags32 & (MDBX_REGIME_PRINCIPAL_FLAGS | MDBX_RDONLY);
    if ((regime_flags & MDBX_EXCLUSIVE) == 0) {
      rc = lck_downgrade(env);
      if (rc != MDBX_SUCCESS)
        goto bailout;
    }
  } else /* got shared mode */ {
    if ((env->me_flags32 & MDBX_RDONLY) == 0) {
      /* update */
      while (env->me_lck->li_regime == MDBX_RDONLY) {
        if (mdbx_atomic_compare_and_swap32(&env->me_lck->li_regime, MDBX_RDONLY,
                                           env->me_flags32 & MDBX_REGIME_PRINCIPAL_FLAGS))
          break;
        /* TODO: yield/relax cpu */
      }
      if ((env->me_lck->li_regime ^ env->me_flags32) & regime_check_mask) {
        mdbx_error("current mode/flags incompatible with requested");
        rc = MDBX_INCOMPATIBLE;
        goto bailout;
      }
    }
  }

  if (env->me_lck && (env->me_flags32 & MDBX_NOTLS) == 0) {
    rc = rthc_alloc(&env->me_txkey, &env->me_lck->li_readers[0], &env->me_lck->li_readers[env->me_maxreaders]);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
    env->me_flags32 |= MDBX_ENV_TXKEY;
  }

  if ((regime_flags & MDBX_RDONLY) == 0) {
    MDBX_txn_t *txn;
    const size_t tsize = mdbx_roundup_ptrsize(sizeof(MDBX_txn_t));
    const size_t size =
        tsize + mdbx_roundup_ptrsize(env->env_ah_max * (sizeof(aht_t) + sizeof(MDBX_cursor_t *)));
    env->me_pagebuf = calloc(1, env->me_psize);
    txn = calloc(1, size);
    if (likely(env->me_pagebuf && txn)) {
      txn->txn_aht_array = (aht_t *)((char *)txn + tsize);
      txn->mt_cursors = (MDBX_cursor_t **)(txn->txn_aht_array + env->env_ah_max);
      *(MDBX_env_t **)&txn->mt_env = env;
      txn->mt_flags = MDBX_TXN_FINISHED;
      env->me_wpa_txn = txn;
    } else {
      rc = MDBX_ENOMEM;
      goto bailout;
    }
  }

  if ((env->me_flags32 ^ env->env_ahe_array[MDBX_MAIN_AAH].ax_flags16) & (MDBX_DB_FLAGS | MDBX_AA_FLAGS)) {
    mdbx_error("main-aa flags are not compatible: present 0x%x, wanna 0x%x",
               env->env_ahe_array[MDBX_MAIN_AAH].ax_flags16,
               env->me_flags32 & (MDBX_DB_FLAGS | MDBX_AA_FLAGS));
    rc = MDBX_INCOMPATIBLE;
    goto bailout;
  }

  rc = dxb_rc;
bailout:
  if (regime_present)
    *regime_present = env->env_ahe_array[MDBX_MAIN_AAH].ax_flags16 | (env->me_flags32 & MDBX_ENV_FLAGS) |
                      (env->me_lck ? env->me_lck->li_regime & MDBX_REGIME_PRINCIPAL_FLAGS : 0);

  if (unlikely(MDBX_IS_ERROR(rc))) {
    env_destroy(env);
  } else {
#if MDBX_DEBUG
    meta_t *meta = meta_head(env);
    aatree_t *db = &meta->mm_aas[MDBX_MAIN_AAH];

    mdbx_debug("opened database version %u, pagesize %u", (uint8_t)meta->mm_magic_and_version, env->me_psize);
    mdbx_debug("using meta page %" PRIaPGNO ", txn %" PRIaTXN "", container_of(meta, page_t, mp_data)->mp_pgno,
               meta_txnid_fluid(env, meta));
    mdbx_debug("depth: %u", db->aa_depth16);
    mdbx_debug("entries: %" PRIu64 "", db->aa_entries);
    mdbx_debug("branch pages: %" PRIaPGNO "", db->aa_branch_pages);
    mdbx_debug("leaf pages: %" PRIaPGNO "", db->aa_leaf_pages);
    mdbx_debug("overflow pages: %" PRIaPGNO "", db->aa_overflow_pages);
    mdbx_debug("root: %" PRIaPGNO "", db->aa_root);
#endif
  }

  free(molded_lck_filename);
  free(molded_dxb_filename);
  return rc;
}

static int cursor_put(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, unsigned flags) {
  page_t *fp, *sub_root = nullptr;
  uint16_t fp_flags;
  MDBX_iov_t xdata, *rdata, dkey, olddata;
  unsigned mcount = 0, dcount = 0;
  size_t nsize;
  unsigned nflags;
  DKBUF;
  aatree_t dummy;

  MDBX_env_t *env = mc->mc_txn->mt_env;
  cursor_t *const nested_or_null = cursor_nested_or_null(mc);
  int rc = MDBX_SUCCESS;

  /* Check this first so counter will always be zero on any early failures. */
  if (flags & MDBX_IUD_MULTIPLE) {
    if (unlikely(!F_ISSET(mc->mc_aht->aa.flags16, MDBX_DUPFIXED)))
      return MDBX_INCOMPATIBLE;
    if (unlikely(data[1].iov_len >= INT_MAX))
      return MDBX_EINVAL;
    dcount = (unsigned)data[1].iov_len;
    data[1].iov_len = 0;
  }

  const unsigned nospill = flags & MDBX_IUD_NOSPILL;
  flags &= ~MDBX_IUD_NOSPILL;

  mdbx_debug("==> put db %d key [%s], size %" PRIuPTR ", data [%s] size %" PRIuPTR, DAAH(mc), DKEY(key),
             key ? key->iov_len : 0, DVAL((flags & MDBX_IUD_RESERVE) ? nullptr : data), data->iov_len);

  int dupdata_flag = 0;
  if (flags & MDBX_IUD_CURRENT) {
    /* Опция MDBX_IUD_CURRENT означает, что запрошено обновление текущей
     * записи, на которой сейчас стоит курсор. Проверяем что переданный ключ
     * совпадает  со значением в текущей позиции курсора.
     * Здесь проще вызвать mdbx_cursor_get(), так как для обслуживания таблиц
     * с MDBX_DUPSORT также требуется текущий размер данных. */
    MDBX_iov_t current_key, current_data;
    rc = cursor_get(mc, &current_key, &current_data, MDBX_GET_CURRENT);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    if (unlikely(cursor_compare_keys(mc, key, &current_key) != 0))
      return MDBX_EKEYMISMATCH;

    if (mc->mc_kind8 & S_HAVESUB) {
      assert(mc->mc_aht->aa.flags16 & MDBX_DUPSORT);
      node_t *leaf = node_ptr(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (leaf->node_flags8 & NODE_DUP) {
        assert(nested_or_null != nullptr && (nested_or_null->mc_state8 & C_INITIALIZED));
        /* Если за ключом более одного значения, либо если размер данных
         * отличается, то вместо inplace обновления требуется удаление и
         * последующая вставка. */
        if (nested_subcursor(nested_or_null)->mx_aht_body.aa.entries > 1 ||
            current_data.iov_len != data->iov_len) {
          rc = cursor_delete(mc, 0);
          if (unlikely(rc != MDBX_SUCCESS))
            return rc;
          flags -= MDBX_IUD_CURRENT;
        }
      }
    } else {
      assert((mc->mc_aht->aa.flags16 & MDBX_DUPSORT) == 0);
    }
  }

  if (mc->mc_aht->aa.root == P_INVALID) {
    /* new database, cursor has nothing to point to */
    mc->mc_snum = 0;
    mc->mc_top = 0;
    mc->mc_state8 &= ~C_INITIALIZED;
    rc = MDBX_NO_ROOT;
  } else if ((flags & MDBX_IUD_CURRENT) == 0) {
    int exact = 0;
    MDBX_iov_t d2;
    if (flags & MDBX_IUD_APPEND) {
      MDBX_iov_t k2;
      rc = cursor_last(mc, &k2, &d2);
      if (rc == MDBX_SUCCESS) {
        if (cursor_compare_keys(mc, key, &k2) > 0) {
          rc = MDBX_NOTFOUND;
          mc->mc_ki[mc->mc_top]++;
        } else {
          /* new key is <= last key */
          rc = MDBX_EKEYMISMATCH;
        }
      }
    } else {
      rc = cursor_set(mc, key, &d2, MDBX_SET, &exact);
    }
    if ((flags & MDBX_IUD_NOOVERWRITE) && (rc == MDBX_SUCCESS || rc == MDBX_EKEYMISMATCH)) {
      mdbx_debug("duplicate key [%s]", DKEY(key));
      *data = d2;
      return MDBX_KEYEXIST;
    }
    if (rc != MDBX_SUCCESS && unlikely(rc != MDBX_NOTFOUND))
      return rc;
  }

  mc->mc_state8 &= ~C_AFTERDELETE;

  /* Cursor is positioned, check for room in the dirty list */
  if (!nospill) {
    if (unlikely(flags & MDBX_IUD_MULTIPLE)) {
      rdata = &xdata;
      xdata.iov_len = data->iov_len * dcount;
    } else {
      rdata = data;
    }
    int err = page_spill(mc, key, rdata);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }

  if (rc == MDBX_NO_ROOT) {
    page_t *np;
    /* new database, write a root leaf page */
    mdbx_debug("allocating new root leaf page");
    int err = page_new(mc, P_LEAF, 1, &np);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    assert(np->mp_flags16 & P_LEAF);
    err = cursor_push(mc, np);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    mc->mc_aht->aa.root = np->mp_pgno;
    mc->mc_aht->aa.depth16++;
    mc->mc_aht->ah.state8 |= MDBX_AAH_DIRTY;
    if (mc->mc_kind8 & S_DUPFIXED) {
      assert((mc->mc_aht->aa.flags16 & (MDBX_DUPSORT | MDBX_DUPFIXED)) == MDBX_DUPFIXED);
      np->mp_flags16 |= P_DFL;
    } else {
      assert((mc->mc_aht->aa.flags16 & (MDBX_DUPSORT | MDBX_DUPFIXED)) != MDBX_DUPFIXED);
    }
    mc->mc_state8 |= C_INITIALIZED;
  } else {
    /* make sure all cursor pages are writable */
    int err = cursor_touch(mc);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }

  bool insert_key, insert_data, do_sub = false;
  insert_key = insert_data = (rc != MDBX_SUCCESS);
  if (insert_key) {
    /* The key does not exist */
    mdbx_debug("inserting key at index %i", mc->mc_ki[mc->mc_top]);
    assert(((mc->mc_aht->aa.flags16 & MDBX_DUPSORT) != 0) == ((mc->mc_kind8 & S_HAVESUB) != 0));
    if ((mc->mc_kind8 & S_HAVESUB) && LEAFSIZE(key, data) > env->me_nodemax) {
      /* Too big for a node, insert in sub-AA.  Set up an empty
       * "old sub-page" for prep_subDB to expand to a full page. */
      fp_flags = P_LEAF | P_DIRTY;
      fp = env->me_pagebuf;
      fp->mp_leaf2_ksize16 = (uint16_t)data->iov_len; /* used if MDBX_DUPFIXED */
      fp->mp_lower = fp->mp_upper = 0;
      olddata.iov_len = PAGEHDRSZ;
      goto prep_subDB;
    }
  } else {
    /* there's only a key anyway, so this is a no-op */
    if (IS_DFL(mc->mc_pg[mc->mc_top])) {
      const unsigned keysize = mc->mc_aht->aa.xsize32;
      if (key->iov_len != keysize)
        return MDBX_BAD_VALSIZE;
      else {
        void *ptr = DFLKEY(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top], keysize);
        memcpy(ptr, key->iov_base, keysize);
      }
    fix_parent:
      /* if overwriting slot 0 of leaf, need to
       * update branch key if there is a parent page */
      if (mc->mc_top && !mc->mc_ki[mc->mc_top]) {
        unsigned dtop = 1;
        mc->mc_top--;
        /* slot 0 is always an empty key, find real slot */
        while (mc->mc_top && !mc->mc_ki[mc->mc_top]) {
          mc->mc_top--;
          dtop++;
        }
        int err = (mc->mc_ki[mc->mc_top]) ? update_key(mc, key) : MDBX_SUCCESS;
        assert(mc->mc_top + dtop < UINT16_MAX);
        mc->mc_top += (uint16_t)dtop;
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }
      return MDBX_SUCCESS;
    }

  more:;
    node_t *leaf = node_ptr(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
    olddata.iov_len = node_get_datasize(leaf);
    olddata.iov_base = NODEDATA(leaf);

    /* AA has dups? */
    if (mc->mc_kind8 & S_HAVESUB) {
      assert(mc->mc_aht->aa.flags16 & MDBX_DUPSORT);
      /* Prepare (sub-)page/sub-AA to accept the new item, if needed.
       * fp: old sub-page or a header faking it.
       * mp: new (sub-)page.  offset: growth in page size.
       * xdata: node data with new page or AA. */
      unsigned i, offset = 0;
      page_t *mp = fp = xdata.iov_base = env->me_pagebuf;
      mp->mp_pgno = mc->mc_pg[mc->mc_top]->mp_pgno;

      /* Was a single item before, must convert now */
      if (!F_ISSET(leaf->node_flags8, NODE_DUP)) {

        /* does data match? */
        if (!cursor_compare_data(mc, data, &olddata)) {
          if (unlikely(flags & (MDBX_IUD_NODUP | MDBX_IUD_APPENDDUP)))
            return MDBX_KEYEXIST;
          /* overwrite it */
          goto current;
        }

        /* Just overwrite the current item */
        if (flags & MDBX_IUD_CURRENT)
          goto current;

        /* Back up original data item */
        dupdata_flag = 1;
        dkey.iov_len = olddata.iov_len;
        dkey.iov_base = memcpy(fp + 1, olddata.iov_base, olddata.iov_len);

        /* Make sub-page header for the dup items, with dummy body */
        fp->mp_flags16 = P_LEAF | P_DIRTY | P_SUBP;
        fp->mp_lower = 0;
        xdata.iov_len = PAGEHDRSZ + dkey.iov_len + data->iov_len;
        if (mc->mc_kind8 & S_DUPFIXED) {
          assert(mc->mc_aht->aa.flags16 & MDBX_DUPFIXED);
          fp->mp_flags16 |= P_DFL;
          fp->mp_leaf2_ksize16 = (uint16_t)data->iov_len;
          xdata.iov_len += 2 * data->iov_len; /* leave space for 2 more */
        } else {
          assert(!(mc->mc_aht->aa.flags16 & MDBX_DUPFIXED));
          xdata.iov_len += 2 * (sizeof(indx_t) + NODESIZE) + (dkey.iov_len & 1) + (data->iov_len & 1);
        }
        fp->mp_upper = (uint16_t)(xdata.iov_len - PAGEHDRSZ);
        olddata.iov_len = xdata.iov_len; /* pretend olddata is fp */
      } else if (leaf->node_flags8 & NODE_SUBTREE) {
        /* Data is on sub-AA, just store it */
        flags |= NODE_DUP | NODE_SUBTREE;
        goto put_sub;
      } else {
        /* Data is on sub-page */
        fp = olddata.iov_base;
        switch (flags) {
        default:
          if ((mc->mc_kind8 & S_DUPFIXED) == 0) {
            assert(!(mc->mc_aht->aa.flags16 & MDBX_DUPFIXED));
            offset = EVEN(NODESIZE + sizeof(indx_t) + data->iov_len);
            break;
          } else {
            assert(mc->mc_aht->aa.flags16 & MDBX_DUPFIXED);
          }
          offset = fp->mp_leaf2_ksize16;
          if (page_spaceleft(fp) < offset) {
            offset *= 4; /* space for 4 more */
            break;
          } else {
            /* Big enough MDBX_DUPFIXED sub-page */
          }
        /* fallthrough */
        case MDBX_IUD_CURRENT | MDBX_IUD_NODUP:
        case MDBX_IUD_CURRENT:
          fp->mp_flags16 |= P_DIRTY;
          fp->mp_pgno = mp->mp_pgno;
          nested_or_null->mc_pg[0] = fp;
          flags |= NODE_DUP;
          goto put_sub;
        }
        xdata.iov_len = olddata.iov_len + offset;
      }

      fp_flags = fp->mp_flags16;
      if (NODESIZE + node_get_keysize(leaf) + xdata.iov_len > env->me_nodemax) {
        /* Too big for a sub-page, convert to sub-tree */
        fp_flags &= ~P_SUBP;
      prep_subDB:;
        /* FIXME: формировать в nested */
        dummy.aa_xsize32 = 0;
        dummy.aa_flags16 = 0;
        if (mc->mc_aht->aa.flags16 & MDBX_DUPFIXED) {
          fp_flags |= P_DFL;
          dummy.aa_xsize32 = fp->mp_leaf2_ksize16;
          dummy.aa_flags16 = MDBX_DUPFIXED;
          if (mc->mc_aht->aa.flags16 & MDBX_INTEGERDUP)
            dummy.aa_flags16 |= MDBX_INTEGERKEY;
        }
        dummy.aa_depth16 = 1;
        dummy.aa_branch_pages = 0;
        dummy.aa_leaf_pages = 1;
        dummy.aa_overflow_pages = 0;
        dummy.aa_entries = page_numkeys(fp);
        xdata.iov_len = sizeof(aatree_t);
        xdata.iov_base = &dummy;
        rc = page_alloc(mc, 1, &mp, MDBX_ALLOC_ALL);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
        assert(env->me_psize > olddata.iov_len);
        offset = env->me_psize - (unsigned)olddata.iov_len;
        flags |= NODE_DUP | NODE_SUBTREE;
        dummy.aa_root = mp->mp_pgno;
        sub_root = mp;
      }
      if (mp != fp) {
        mp->mp_flags16 = fp_flags | P_DIRTY;
        mp->mp_leaf2_ksize16 = fp->mp_leaf2_ksize16;
        mp->mp_lower = fp->mp_lower;
        assert(fp->mp_upper + offset <= UINT16_MAX);
        mp->mp_upper = (indx_t)(fp->mp_upper + offset);
        if (fp_flags & P_DFL) {
          memcpy(page_data(mp), page_data(fp), page_numkeys(fp) * fp->mp_leaf2_ksize16);
        } else {
          memcpy((char *)mp + mp->mp_upper + PAGEHDRSZ, (char *)fp + fp->mp_upper + PAGEHDRSZ,
                 olddata.iov_len - fp->mp_upper - PAGEHDRSZ);
          memcpy((char *)(&mp->mp_ptrs), (char *)(&fp->mp_ptrs), page_numkeys(fp) * sizeof(mp->mp_ptrs[0]));
          for (i = 0; i < page_numkeys(fp); i++) {
            mdbx_assert(env, mp->mp_ptrs[i] + offset <= UINT16_MAX);
            mp->mp_ptrs[i] += (indx_t)offset;
          }
        }
      }

      rdata = &xdata;
      flags |= NODE_DUP;
      do_sub = true;
      if (!insert_key)
        node_del(mc, 0);
      goto new_sub;
    } else {
      assert(!(mc->mc_aht->aa.flags16 & MDBX_DUPSORT));
    }
  current:
    /* MDBX passes NODE_SUBTREE in 'flags' to write a AA record */
    if (unlikely((leaf->node_flags8 ^ flags) & NODE_SUBTREE))
      return MDBX_INCOMPATIBLE;
    /* overflow page overwrites need special handling */
    if (unlikely(leaf->node_flags8 & NODE_BIG)) {
      int level, ovpages, dpages = OVPAGES(env, data->iov_len);
      pgno_t pgno = get_pgno_aligned2(olddata.iov_base);
      page_t *omp;
      int err = page_get(mc->mc_txn, pgno, &omp, &level);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      ovpages = omp->mp_pages;

      /* Is the ov page large enough? */
      if (ovpages >= dpages) {
        if (!(omp->mp_flags16 & P_DIRTY) && (level || (env->me_flags32 & MDBX_WRITEMAP))) {
          rc = page_unspill(mc->mc_txn, omp, &omp);
          if (unlikely(rc != MDBX_SUCCESS))
            return rc;
          level = 0; /* dirty in this txn or clean */
        }
        /* Is it dirty? */
        if (omp->mp_flags16 & P_DIRTY) {
          /* yes, overwrite it. Note in this case we don't
           * bother to try shrinking the page if the new data
           * is smaller than the overflow threshold. */
          if (unlikely(level > 1)) {
            /* It is writable only in a parent txn */
            page_t *np = page_malloc(mc->mc_txn, ovpages);
            if (unlikely(!np))
              return MDBX_ENOMEM;
            MDBX_ID2 id2 = {pgno, np};
            /* Note - this page is already counted in parent's dirtyroom */
            err = mdbx_mid2l_insert(mc->mc_txn->mt_rw_dirtylist, &id2);
            assert(err == 0);

            /* Currently we make the page look as with put() in the
             * parent txn, in case the user peeks at MDBX_RESERVEd
             * or unused parts. Some users treat ovpages specially. */
            const size_t whole = pgno2bytes(env, ovpages);
            /* Skip the part where MDBX will put *data.
             * Copy end of page, adjusting alignment so
             * compiler may copy words instead of bytes. */
            const size_t off = (PAGEHDRSZ + data->iov_len) & -(intptr_t)sizeof(size_t);
            memcpy((size_t *)((char *)np + off), (size_t *)((char *)omp + off), whole - off);
            memcpy(np, omp, PAGEHDRSZ); /* Copy header of page */
            omp = np;
          }
          node_set_datasize(leaf, data->iov_len);
          if (F_ISSET(flags, MDBX_IUD_RESERVE))
            data->iov_base = page_data(omp);
          else
            memcpy(page_data(omp), data->iov_base, data->iov_len);
          return MDBX_SUCCESS;
        }
      }
      err = ovpage_free(mc, omp);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    } else if (data->iov_len == olddata.iov_len) {
      assert(EVEN(key->iov_len) == EVEN(leaf->mn_ksize16));
      /* same size, just replace it. Note that we could
       * also reuse this node if the new data is smaller,
       * but instead we opt to shrink the node in that case. */
      if (F_ISSET(flags, MDBX_IUD_RESERVE))
        data->iov_base = olddata.iov_base;
      else if (!(mc->mc_kind8 & S_SUBCURSOR))
        memcpy(olddata.iov_base, data->iov_base, data->iov_len);
      else {
        assert(page_numkeys(mc->mc_pg[mc->mc_top]) == 1);
        assert(mc->mc_pg[mc->mc_top]->mp_upper == mc->mc_pg[mc->mc_top]->mp_lower);
        assert(IS_LEAF(mc->mc_pg[mc->mc_top]) && !IS_DFL(mc->mc_pg[mc->mc_top]));
        assert(node_get_datasize(leaf) == 0);
        assert(leaf->node_flags8 == 0);
        assert(key->iov_len < UINT16_MAX);
        leaf->mn_ksize16 = (uint16_t)key->iov_len;
        memcpy(NODEKEY(leaf), key->iov_base, key->iov_len);
        assert((char *)NODEDATA(leaf) + node_get_datasize(leaf) <
               (char *)(mc->mc_pg[mc->mc_top]) + env->me_psize);
        goto fix_parent;
      }
      return MDBX_SUCCESS;
    }
    node_del(mc, 0);
  }

  rdata = data;

new_sub:
  nsize = IS_DFL(mc->mc_pg[mc->mc_top]) ? key->iov_len : leaf_size(env, key, rdata);
  nflags = flags & NODE_ADD_FLAGS;
  if (page_spaceleft(mc->mc_pg[mc->mc_top]) < nsize) {
    if ((flags & (NODE_DUP | NODE_SUBTREE)) == NODE_DUP)
      nflags &= ~MDBX_IUD_APPEND; /* sub-page may need room to grow */
    if (!insert_key)
      nflags |= MDBX_SPLIT_REPLACE;
    rc = page_split(mc, key, rdata, P_INVALID, nflags);
  } else {
    /* There is room already in this leaf page. */
    rc = node_add(mc, mc->mc_ki[mc->mc_top], key, rdata, 0, nflags);
    if (likely(rc == MDBX_SUCCESS)) {
      /* Adjust other cursors pointing to mp */
      page_t *const page = mc->mc_pg[mc->mc_top];
      for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(mc)); bundle;
           bundle = bundle->mc_next) {
        cursor_t *scan = (mc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
        if (scan == mc || scan->mc_snum < mc->mc_top)
          continue;
        if (!(bundle->primal.mc_state8 & scan->mc_state8 & C_INITIALIZED))
          continue;
        if (scan->mc_pg[mc->mc_top] != page)
          continue;
        if (insert_key && scan->mc_ki[mc->mc_top] >= mc->mc_ki[mc->mc_top])
          scan->mc_ki[mc->mc_top]++;

        assert(IS_LEAF(page));
        cursor_refresh_subcursor(bundle, mc->mc_top, page);
      }
    }
  }

  if (likely(rc == MDBX_SUCCESS)) {
    /* Now store the actual data in the child AA. Note that we're
     * storing the user data in the keys field, so there are strict
     * size limits on dupdata. The actual data fields of the child
     * AA are all zero size. */
    if (do_sub) {
      int xflags;
    put_sub:
      xdata.iov_len = 0;
      xdata.iov_base = "";
      node_t *leaf = node_ptr(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (flags & MDBX_IUD_CURRENT) {
        xflags = (flags & MDBX_IUD_NODUP) ? MDBX_IUD_CURRENT | MDBX_IUD_NOOVERWRITE | MDBX_IUD_NOSPILL
                                          : MDBX_IUD_CURRENT | MDBX_IUD_NOSPILL;
      } else {
        nested_setup(mc, leaf);
        xflags = (flags & MDBX_IUD_NODUP) ? MDBX_IUD_NOOVERWRITE | MDBX_IUD_NOSPILL : MDBX_IUD_NOSPILL;
      }
      if (sub_root)
        nested_or_null->mc_pg[0] = sub_root;
      /* converted, write the original data first */
      if (dupdata_flag) {
        rc = cursor_put(nested_or_null, &dkey, &xdata, xflags);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bad_sub;
        /* we've done our job */
        dkey.iov_len = 0;
      }
      if (!(leaf->node_flags8 & NODE_SUBTREE) || sub_root) {
        /* Adjust other cursors pointing to mp */
        assert((mc->mc_kind8 & (S_SUBCURSOR | S_HAVESUB)) == S_HAVESUB);
        page_t *const page = mc->mc_pg[mc->mc_top];
        for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(mc)); bundle;
             bundle = bundle->mc_next) {
          cursor_t *scan = (mc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
          if (scan == mc || scan->mc_snum < mc->mc_top)
            continue;
          if (!(bundle->primal.mc_state8 & scan->mc_state8 & C_INITIALIZED))
            continue;
          if (scan->mc_pg[mc->mc_top] != page)
            continue;

          if (scan->mc_ki[mc->mc_top] == mc->mc_ki[mc->mc_top]) {
            subcursor_fixup(bundle, nested_or_null, dupdata_flag);
          } else if (!insert_key) {
            cursor_refresh_subcursor(bundle, mc->mc_top, page);
          }
        }
      }
      assert(nested_subcursor(nested_or_null)->mx_aht_body.aa.entries < SIZE_MAX);
      size_t entries_before_put = (size_t)nested_subcursor(nested_or_null)->mx_aht_body.aa.entries;
      if (flags & MDBX_IUD_APPENDDUP)
        xflags |= MDBX_IUD_APPEND;
      rc = cursor_put(nested_or_null, data, &xdata, xflags);
      if (flags & NODE_SUBTREE)
        aa_txn2db(env, &nested_subcursor(nested_or_null)->mx_aht_body, (aatree_t *)NODEDATA(leaf), af_nested);
      insert_data = (entries_before_put != (size_t)nested_subcursor(nested_or_null)->mx_aht_body.aa.entries);
    }
    /* Increment count unless we just replaced an existing item. */
    if (insert_data)
      mc->mc_aht->aa.entries++;
    if (insert_key) {
      /* Invalidate txn if we created an empty sub-AA */
      if (unlikely(rc != MDBX_SUCCESS))
        goto bad_sub;
      /* If we succeeded and the key didn't exist before,
       * make sure the cursor is marked valid. */
      mc->mc_state8 |= C_INITIALIZED;
    }
    if (flags & MDBX_IUD_MULTIPLE) {
      if (!rc) {
        mcount++;
        /* let caller know how many succeeded, if any */
        data[1].iov_len = mcount;
        if (mcount < dcount) {
          data[0].iov_base = (char *)data[0].iov_base + data[0].iov_len;
          insert_key = insert_data = false;
          goto more;
        }
      }
    }
    return rc;
  bad_sub:
    if (unlikely(rc == MDBX_KEYEXIST))
      mdbx_error("unexpected %s", "MDBX_KEYEXIST");
    /* should not happen, we deleted that item */
    rc = MDBX_PROBLEM;
  }
  mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

/* Complete a delete operation started by mdbx_cursor_delete(). */
static int mdbx_cr_del0(cursor_t *mc) {
  int rc;
  page_t *mp;
  indx_t ki;
  unsigned nkeys;

  ki = mc->mc_ki[mc->mc_top];
  mp = mc->mc_pg[mc->mc_top];
  node_del(mc, mc->mc_aht->aa.xsize32);
  mc->mc_aht->aa.entries--;
  /* Adjust other cursors pointing to mp */
  for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(mc)); bundle; bundle = bundle->mc_next) {
    cursor_t *scan = (mc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
    if (!(bundle->primal.mc_state8 & scan->mc_state8 & C_INITIALIZED))
      continue;
    if (scan == mc || scan->mc_snum < mc->mc_snum)
      continue;
    if (scan->mc_pg[mc->mc_top] == mp) {
      if (scan->mc_ki[mc->mc_top] == ki) {
        scan->mc_state8 |= C_AFTERDELETE;
        /* Sub-cursor referred into dataset which is gone */
        bundle->subcursor.mx_cursor.mc_state8 &= ~(C_INITIALIZED | C_EOF);
        continue;
      } else if (scan->mc_ki[mc->mc_top] > ki) {
        scan->mc_ki[mc->mc_top]--;
      }
      cursor_refresh_subcursor(bundle, mc->mc_top, scan->mc_pg[mc->mc_top]);
    }
  }
  rc = tree_rebalance(mc);

  if (likely(rc == MDBX_SUCCESS)) {
    /* AA is totally empty now, just bail out.
     * Other cursors adjustments were already done
     * by mdbx_rebalance and aren't needed here. */
    if (unlikely(mc->mc_snum == 0)) {
      mc->mc_state8 |= C_AFTERDELETE | C_EOF;
      return rc;
    }

    mp = mc->mc_pg[mc->mc_top];
    nkeys = page_numkeys(mp);

    /* Adjust other cursors pointing to mp */
    for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(mc)); rc == MDBX_SUCCESS && bundle;
         bundle = bundle->mc_next) {
      cursor_t *scan = (mc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
      if (!(bundle->primal.mc_state8 & scan->mc_state8 & C_INITIALIZED))
        continue;
      if (scan->mc_snum < mc->mc_snum)
        continue;
      if (scan->mc_pg[mc->mc_top] == mp) {
        /* if m3 points past last node in page, find next sibling */
        if (scan->mc_ki[mc->mc_top] >= mc->mc_ki[mc->mc_top]) {
          if (scan->mc_ki[mc->mc_top] >= nkeys) {
            rc = cursor_sibling(scan, 1);
            if (rc == MDBX_NOTFOUND) {
              scan->mc_state8 |= C_EOF;
              rc = MDBX_SUCCESS;
              continue;
            }
          }
          if (bundle->primal.mc_kind8 & S_HAVESUB) {
            node_t *node = node_ptr(scan->mc_pg[scan->mc_top], scan->mc_ki[scan->mc_top]);
            /* If this node has dupdata, it may need to be reinited
             * because its data has moved.
             * If the xcursor was not initd it must be reinited.
             * Else if node points to a subDB, nothing is needed. */
            if (node->node_flags8 & NODE_DUP) {
              if (bundle->subcursor.mx_cursor.mc_state8 & C_INITIALIZED) {
                if ((node->node_flags8 & NODE_SUBTREE) == 0)
                  bundle->subcursor.mx_cursor.mc_pg[0] = NODEDATA(node);
              } else {
                nested_setup(&bundle->primal, node);
                bundle->subcursor.mx_cursor.mc_state8 |= C_AFTERDELETE;
              }
            }
          }
        }
      }
    }

    mc->mc_state8 |= C_AFTERDELETE;
  }

  if (unlikely(rc != MDBX_SUCCESS))
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

static int cursor_delete(cursor_t *mc, unsigned flags) {
  if (unlikely(!(mc->mc_state8 & C_INITIALIZED)))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_ki[mc->mc_top] >= page_numkeys(mc->mc_pg[mc->mc_top])))
    return MDBX_NOTFOUND;

  if (likely((flags & MDBX_IUD_NOSPILL) == 0)) {
    int rc = page_spill(mc, nullptr, nullptr);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  int rc = cursor_touch(mc);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  page_t *const page = mc->mc_pg[mc->mc_top];
  if (unlikely(IS_DFL(page)))
    goto del_key;

  node_t *leaf = node_ptr(page, mc->mc_ki[mc->mc_top]);
  if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
    subcur_t *const subcursor = cursor_subcur(mc);
    if (flags & MDBX_IUD_NODUP) {
      /* mdbx_cr_del0() will subtract the final entry */
      mc->mc_aht->aa.entries -= subcursor->mx_aht_body.aa.entries - 1;
      subcursor->mx_cursor.mc_state8 &= ~C_INITIALIZED;
    } else {
      if (!F_ISSET(leaf->node_flags8, NODE_SUBTREE))
        subcursor->mx_cursor.mc_pg[0] = NODEDATA(leaf);

      rc = cursor_delete(&subcursor->mx_cursor, MDBX_IUD_NOSPILL);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;

      /* If sub-tree still has entries, we're done */
      if (likely(subcursor->mx_aht_body.aa.entries)) {
        if (likely(leaf->node_flags8 & NODE_SUBTREE)) {
          /* update sub-tree info */
          aa_txn2db(mc->mc_txn->mt_env, &subcursor->mx_aht_body, (aatree_t *)NODEDATA(leaf), af_nested);
        } else {
          /* shrink fake page */
          node_shrink(page, mc->mc_ki[mc->mc_top]);
          leaf = node_ptr(page, mc->mc_ki[mc->mc_top]);
          subcursor->mx_cursor.mc_pg[0] = NODEDATA(leaf);
          /* fix other sub-tree cursors pointed at fake pages on this page */
          const unsigned top = mc->mc_top;
          assert((mc->mc_kind8 & (S_SUBCURSOR | S_HAVESUB)) == S_HAVESUB);
          for (MDBX_cursor_t *scan = *cursor_tracking_head(cursor_bundle(mc)); scan; scan = scan->mc_next) {
            if (&scan->subcursor == subcursor)
              continue;
            if (top >= scan->primal.mc_snum /* scan->primal.mc_snum < mc->mc_snum */)
              continue;
            if (!(scan->primal.mc_state8 & C_INITIALIZED))
              continue;
            if (scan->primal.mc_pg[top] != page)
              continue;
            cursor_refresh_subcursor(scan, top, page);
          }
        }
        mc->mc_aht->aa.entries--;
        return rc;
      } else {
        subcursor->mx_cursor.mc_state8 &= ~C_INITIALIZED;
      }
      /* otherwise fall thru and delete the sub-tree */
    }

    if (leaf->node_flags8 & NODE_SUBTREE) {
      /* add all the child AA's pages to the free list */
      rc = tree_drop(&subcursor->mx_cursor, 0);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    }
  }
  /* MDBX passes NODE_SUBTREE in 'flags' to delete a AA record */
  else if (unlikely((leaf->node_flags8 ^ flags) & NODE_SUBTREE)) {
    rc = MDBX_INCOMPATIBLE;
    goto fail;
  }

  /* add overflow pages to free list */
  if (unlikely(leaf->node_flags8 & NODE_BIG)) {
    page_t *omp;
    rc = page_get(mc->mc_txn, get_pgno_aligned2(NODEDATA(leaf)), &omp, nullptr);
    if (unlikely((rc != MDBX_SUCCESS)))
      goto fail;
    rc = ovpage_free(mc, omp);
    if (unlikely((rc != MDBX_SUCCESS)))
      goto fail;
  }

del_key:
  return mdbx_cr_del0(mc);

fail:
  mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

/* Allocate and initialize new pages for a database.
 * Set MDBX_TXN_ERROR on failure.
 *
 * [in] mc a  cursor on the database being added to.
 * [in] flags flags defining what type of page is being allocated.
 * [in] num   the number of pages to allocate. This is usually 1,
 *            unless allocating overflow pages for a large record.
 * [out] mp   Address of a page, or nullptr on failure.
 *
 * Returns 0 on success, non-zero on failure. */
static int page_new(cursor_t *mc, unsigned flags, unsigned num, page_t **mp) {
  page_t *np;

  int rc = page_alloc(mc, num, &np, MDBX_ALLOC_ALL);
  if (unlikely((rc != MDBX_SUCCESS)))
    return rc;

  mdbx_debug("allocated new page #%" PRIaPGNO ", size %u", np->mp_pgno, mc->mc_txn->mt_env->me_psize);
  np->mp_flags16 = (uint16_t)(flags | P_DIRTY);
  np->mp_lower = 0;
  np->mp_upper = (indx_t)(mc->mc_txn->mt_env->me_psize - PAGEHDRSZ);

  if (IS_BRANCH(np))
    mc->mc_aht->aa.branch_pages++;
  else if (IS_LEAF(np))
    mc->mc_aht->aa.leaf_pages++;
  else if (IS_OVERFLOW(np)) {
    mc->mc_aht->aa.overflow_pages += num;
    np->mp_pages = num;
  }
  *mp = np;

  return MDBX_SUCCESS;
}

/* Calculate the size of a leaf node.
 *
 * The size depends on the databook's page size; if a data item
 * is too large it will be put onto an overflow page and the node
 * size will only include the key and not the data. Sizes are always
 * rounded up to an even number of bytes, to guarantee 2-byte alignment
 * of the node_t headers.
 *
 * [in] env   The databook handle.
 * [in] key   The key for the node.
 * [in] data  The data for the node.
 *
 * Returns The number of bytes needed to store the node. */
static inline size_t leaf_size(MDBX_env_t *env, MDBX_iov_t *key, MDBX_iov_t *data) {
  size_t sz;

  sz = LEAFSIZE(key, data);
  if (sz > env->me_nodemax) {
    /* put on overflow page */
    sz -= data->iov_len - sizeof(pgno_t);
  }

  return EVEN(sz + sizeof(indx_t));
}

/* Calculate the size of a branch node.
 *
 * The size should depend on the databook's page size but since
 * we currently don't support spilling large keys onto overflow
 * pages, it's simply the size of the node_t header plus the
 * size of the key. Sizes are always rounded up to an even number
 * of bytes, to guarantee 2-byte alignment of the node_t headers.
 *
 * [in] env The databook handle.
 * [in] key The key for the node.
 *
 * Returns The number of bytes needed to store the node. */
static inline size_t branch_size(MDBX_env_t *env, MDBX_iov_t *key) {
  size_t sz;

  sz = INDXSIZE(key);
  if (unlikely(sz > env->me_nodemax)) {
    /* put on overflow page */
    /* not implemented */
    mdbx_assert_fail(env, "INDXSIZE(key) <= env->me_nodemax", __func__, __LINE__);
    sz -= key->iov_len - sizeof(pgno_t);
  }

  return sz + sizeof(indx_t);
}

/* Add a node to the page pointed to by the cursor.
 * Set MDBX_TXN_ERROR on failure.
 *
 * [in] mc    The cursor for this operation.
 * [in] indx  The index on the page where the new node should be added.
 * [in] key   The key for the new node.
 * [in] data  The data for the new node, if any.
 * [in] pgno  The page number, if adding a branch node.
 * [in] flags Flags for the node.
 *
 * Returns 0 on success, non-zero on failure. Possible errors are:
 *
 * MDBX_ENOMEM    - failed to allocate overflow pages for the node.
 * MDBX_PAGE_FULL  - there is insufficient room in the page. This error
 *                  should never happen since all callers already calculate
 *                  the page's free space before calling this function. */
static int node_add(cursor_t *mc, unsigned indx, MDBX_iov_t *key, MDBX_iov_t *data, pgno_t pgno,
                    unsigned flags) {
  unsigned i;
  size_t node_size = NODESIZE;
  intptr_t room;
  node_t *node;
  page_t *mp = mc->mc_pg[mc->mc_top];
  page_t *ofp = nullptr; /* overflow page */
  void *ndata;
  DKBUF;

  assert(mp->mp_upper >= mp->mp_lower);

  mdbx_debug("add to %s %spage %" PRIaPGNO " index %i, data size %" PRIuPTR " key size %" PRIuPTR " [%s]",
             IS_LEAF(mp) ? "leaf" : "branch", IS_SUBP(mp) ? "sub-" : "", mp->mp_pgno, indx,
             data ? data->iov_len : 0, key ? key->iov_len : 0, DKEY(key));

  if (IS_DFL(mp)) {
    assert(key);
    /* Move higher keys up one slot. */
    const int keysize = mc->mc_aht->aa.xsize32;
    char *const ptr = DFLKEY(mp, indx, keysize);
    const int diff = page_numkeys(mp) - indx;
    if (diff > 0)
      memmove(ptr + keysize, ptr, diff * keysize);
    /* insert new key */
    memcpy(ptr, key->iov_base, keysize);

    /* Just using these for counting */
    assert(UINT16_MAX - mp->mp_lower >= (int)sizeof(indx_t));
    mp->mp_lower += sizeof(indx_t);
    assert(mp->mp_upper >= keysize - sizeof(indx_t));
    mp->mp_upper -= (indx_t)(keysize - sizeof(indx_t));
    return MDBX_SUCCESS;
  }

  room = (intptr_t)page_spaceleft(mp) - (intptr_t)sizeof(indx_t);
  if (key != nullptr)
    node_size += key->iov_len;
  if (IS_LEAF(mp)) {
    assert(key && data);
    if (unlikely(flags & NODE_BIG)) {
      /* Data already on overflow page. */
      node_size += sizeof(pgno_t);
    } else if (unlikely(node_size + data->iov_len > mc->mc_txn->mt_env->me_nodemax)) {
      pgno_t ovpages = OVPAGES(mc->mc_txn->mt_env, data->iov_len);
      int rc;
      /* Put data on overflow page. */
      mdbx_debug("data size is %" PRIuPTR ", node would be %" PRIuPTR ", put data on overflow page",
                 data->iov_len, node_size + data->iov_len);
      node_size = EVEN(node_size + sizeof(pgno_t));
      if ((intptr_t)node_size > room)
        goto full;
      rc = page_new(mc, P_OVERFLOW, ovpages, &ofp);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      mdbx_debug("allocated overflow page %" PRIaPGNO "", ofp->mp_pgno);
      flags |= NODE_BIG;
      goto update;
    } else {
      node_size += data->iov_len;
    }
  }
  node_size = EVEN(node_size);
  if (unlikely((intptr_t)node_size > room))
    goto full;

update:
  /* Move higher pointers up one slot. */
  for (i = page_numkeys(mp); i > indx; i--)
    mp->mp_ptrs[i] = mp->mp_ptrs[i - 1];

  /* Adjust free space offsets. */
  size_t ofs = mp->mp_upper - node_size;
  assert(ofs >= mp->mp_lower + sizeof(indx_t));
  assert(ofs <= UINT16_MAX);
  mp->mp_ptrs[indx] = (uint16_t)ofs;
  mp->mp_upper = (uint16_t)ofs;
  mp->mp_lower += sizeof(indx_t);

  /* Write the node data. */
  node = node_ptr(mp, indx);
  node->mn_ksize16 = (key == nullptr) ? 0 : (uint16_t)key->iov_len;
  node->node_flags8 = (uint8_t)flags;
  if (IS_LEAF(mp))
    node_set_datasize(node, data->iov_len);
  else
    node_set_pgno(node, pgno);

  if (key)
    memcpy(NODEKEY(node), key->iov_base, key->iov_len);

  if (IS_LEAF(mp)) {
    ndata = NODEDATA(node);
    if (unlikely(ofp == nullptr)) {
      if (unlikely(flags & NODE_BIG))
        memcpy(ndata, data->iov_base, sizeof(pgno_t));
      else if (F_ISSET(flags, MDBX_IUD_RESERVE))
        data->iov_base = ndata;
      else if (likely(ndata != data->iov_base))
        memcpy(ndata, data->iov_base, data->iov_len);
    } else {
      memcpy(ndata, &ofp->mp_pgno, sizeof(pgno_t));
      ndata = page_data(ofp);
      if (F_ISSET(flags, MDBX_IUD_RESERVE))
        data->iov_base = ndata;
      else if (likely(ndata != data->iov_base))
        memcpy(ndata, data->iov_base, data->iov_len);
    }
  }

  return MDBX_SUCCESS;

full:
  mdbx_debug("not enough room in page %" PRIaPGNO ", got %u ptrs", mp->mp_pgno, page_numkeys(mp));
  mdbx_debug("upper-lower = %u - %u = %" PRIiPTR "", mp->mp_upper, mp->mp_lower, room);
  mdbx_debug("node size = %" PRIuPTR "", node_size);
  mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return MDBX_PAGE_FULL;
}

/* Move a node from csrc to cdst. */
static int node_move(cursor_t *csrc, cursor_t *cdst, int fromleft) {
  node_t *srcnode;
  MDBX_iov_t key, data;
  pgno_t srcpg;
  int rc;
  unsigned flags;

  DKBUF;

  /* Mark src and dst as dirty. */
  rc = page_touch(csrc);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  rc = page_touch(cdst);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (IS_DFL(csrc->mc_pg[csrc->mc_top])) {
    key.iov_len = csrc->mc_aht->aa.xsize32;
    key.iov_base = DFLKEY(csrc->mc_pg[csrc->mc_top], csrc->mc_ki[csrc->mc_top], key.iov_len);
    data.iov_len = 0;
    data.iov_base = nullptr;
    srcpg = 0;
    flags = 0;
  } else {
    srcnode = node_ptr(csrc->mc_pg[csrc->mc_top], csrc->mc_ki[csrc->mc_top]);
    assert(!((size_t)srcnode & 1));
    srcpg = node_get_pgno(srcnode);
    flags = srcnode->node_flags8;
    if (csrc->mc_ki[csrc->mc_top] == 0 && IS_BRANCH(csrc->mc_pg[csrc->mc_top])) {
      unsigned snum = csrc->mc_snum;
      node_t *s2;
      /* must find the lowest key below src */
      rc = page_search_lowest(csrc);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      if (IS_DFL(csrc->mc_pg[csrc->mc_top])) {
        key.iov_len = csrc->mc_aht->aa.xsize32;
        key.iov_base = DFLKEY(csrc->mc_pg[csrc->mc_top], 0, key.iov_len);
      } else {
        s2 = node_ptr(csrc->mc_pg[csrc->mc_top], 0);
        key.iov_len = node_get_keysize(s2);
        key.iov_base = NODEKEY(s2);
      }
      assert(snum >= 1 && snum <= UINT16_MAX);
      csrc->mc_snum = (uint16_t)snum--;
      csrc->mc_top = (uint16_t)snum;
    } else {
      key.iov_len = node_get_keysize(srcnode);
      key.iov_base = NODEKEY(srcnode);
    }
    data.iov_len = node_get_datasize(srcnode);
    data.iov_base = NODEDATA(srcnode);
  }

  if (IS_BRANCH(cdst->mc_pg[cdst->mc_top]) && cdst->mc_ki[cdst->mc_top] == 0) {
    unsigned snum = cdst->mc_snum;
    node_t *s2;
    MDBX_iov_t bkey;
    /* must find the lowest key below dst */
    cursor_t mn;
    cursor_copy_clearsub(cdst, &mn);
    rc = page_search_lowest(&mn);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    if (IS_DFL(mn.mc_pg[mn.mc_top])) {
      bkey.iov_len = mn.mc_aht->aa.xsize32;
      bkey.iov_base = DFLKEY(mn.mc_pg[mn.mc_top], 0, bkey.iov_len);
    } else {
      s2 = node_ptr(mn.mc_pg[mn.mc_top], 0);
      bkey.iov_len = node_get_keysize(s2);
      bkey.iov_base = NODEKEY(s2);
    }
    assert(snum >= 1 && snum <= UINT16_MAX);
    mn.mc_snum = (uint16_t)snum;
    mn.mc_top = (uint16_t)--snum;
    mn.mc_ki[snum] = 0;
    rc = update_key(&mn, &bkey);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  mdbx_debug("moving %s node %u [%s] on page %" PRIaPGNO " to node %u on page %" PRIaPGNO "",
             IS_LEAF(csrc->mc_pg[csrc->mc_top]) ? "leaf" : "branch", csrc->mc_ki[csrc->mc_top], DKEY(&key),
             csrc->mc_pg[csrc->mc_top]->mp_pgno, cdst->mc_ki[cdst->mc_top],
             cdst->mc_pg[cdst->mc_top]->mp_pgno);

  /* Add the node to the destination page. */
  rc = node_add(cdst, cdst->mc_ki[cdst->mc_top], &key, &data, srcpg, flags);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Delete the node from the source page. */
  node_del(csrc, key.iov_len);

  {
    /* Adjust other cursors pointing to mp */
    const page_t *page_src = csrc->mc_pg[csrc->mc_top];
    /* If we're adding on the left, bump others up */
    if (fromleft) {
      page_t *page_dst = cdst->mc_pg[csrc->mc_top];
      for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(csrc)); bundle;
           bundle = bundle->mc_next) {
        cursor_t *scan = (csrc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
        if (!(scan->mc_state8 & C_INITIALIZED) || scan->mc_top < csrc->mc_top)
          continue;
        if (scan != cdst && scan->mc_pg[csrc->mc_top] == page_dst &&
            scan->mc_ki[csrc->mc_top] >= cdst->mc_ki[csrc->mc_top]) {
          scan->mc_ki[csrc->mc_top] += 1;
        }
        if (scan != csrc && scan->mc_pg[csrc->mc_top] == page_src &&
            scan->mc_ki[csrc->mc_top] == csrc->mc_ki[csrc->mc_top]) {
          scan->mc_pg[csrc->mc_top] = cdst->mc_pg[cdst->mc_top];
          scan->mc_ki[csrc->mc_top] = cdst->mc_ki[cdst->mc_top];
          scan->mc_ki[csrc->mc_top - 1] += 1;
        }
        if (IS_LEAF(page_src))
          cursor_refresh_subcursor(bundle, csrc->mc_top, scan->mc_pg[csrc->mc_top]);
      }
    } else /* Adding on the right, bump others down */ {
      for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(csrc)); bundle;
           bundle = bundle->mc_next) {
        cursor_t *scan = (csrc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
        if (scan == csrc)
          continue;
        if (!(scan->mc_state8 & C_INITIALIZED) || scan->mc_top < csrc->mc_top)
          continue;
        if (scan->mc_pg[csrc->mc_top] == page_src) {
          if (!scan->mc_ki[csrc->mc_top]) {
            scan->mc_pg[csrc->mc_top] = cdst->mc_pg[cdst->mc_top];
            scan->mc_ki[csrc->mc_top] = cdst->mc_ki[cdst->mc_top];
            scan->mc_ki[csrc->mc_top - 1] -= 1;
          } else {
            scan->mc_ki[csrc->mc_top] -= 1;
          }
          if (IS_LEAF(page_src))
            cursor_refresh_subcursor(bundle, csrc->mc_top, scan->mc_pg[csrc->mc_top]);
        }
      }
    }
  }

  /* Update the parent separators. */
  if (csrc->mc_ki[csrc->mc_top] == 0) {
    if (csrc->mc_ki[csrc->mc_top - 1] != 0) {
      if (IS_DFL(csrc->mc_pg[csrc->mc_top])) {
        key.iov_base = DFLKEY(csrc->mc_pg[csrc->mc_top], 0, key.iov_len);
      } else {
        srcnode = node_ptr(csrc->mc_pg[csrc->mc_top], 0);
        key.iov_len = node_get_keysize(srcnode);
        key.iov_base = NODEKEY(srcnode);
      }
      mdbx_debug("update separator for source page %" PRIaPGNO " to [%s]", csrc->mc_pg[csrc->mc_top]->mp_pgno,
                 DKEY(&key));
      MDBX_cursor_t mn;
      cursor_copy_clearsub(csrc, &mn.primal);
      mn.primal.mc_snum--;
      mn.primal.mc_top--;
      /* We want mdbx_rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = update_key(&mn.primal, &key));
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    if (IS_BRANCH(csrc->mc_pg[csrc->mc_top])) {
      MDBX_iov_t nullkey;
      indx_t ix = csrc->mc_ki[csrc->mc_top];
      nullkey.iov_len = 0;
      csrc->mc_ki[csrc->mc_top] = 0;
      rc = update_key(csrc, &nullkey);
      csrc->mc_ki[csrc->mc_top] = ix;
      assert(rc == MDBX_SUCCESS);
    }
  }

  if (cdst->mc_ki[cdst->mc_top] == 0) {
    if (cdst->mc_ki[cdst->mc_top - 1] != 0) {
      if (IS_DFL(csrc->mc_pg[csrc->mc_top])) {
        key.iov_base = DFLKEY(cdst->mc_pg[cdst->mc_top], 0, key.iov_len);
      } else {
        srcnode = node_ptr(cdst->mc_pg[cdst->mc_top], 0);
        key.iov_len = node_get_keysize(srcnode);
        key.iov_base = NODEKEY(srcnode);
      }
      mdbx_debug("update separator for destination page %" PRIaPGNO " to [%s]",
                 cdst->mc_pg[cdst->mc_top]->mp_pgno, DKEY(&key));
      MDBX_cursor_t mn;
      cursor_copy_clearsub(cdst, &mn.primal);
      mn.primal.mc_snum--;
      mn.primal.mc_top--;
      /* We want mdbx_rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = update_key(&mn.primal, &key));
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    if (IS_BRANCH(cdst->mc_pg[cdst->mc_top])) {
      MDBX_iov_t nullkey;
      indx_t ix = cdst->mc_ki[cdst->mc_top];
      nullkey.iov_len = 0;
      cdst->mc_ki[cdst->mc_top] = 0;
      rc = update_key(cdst, &nullkey);
      cdst->mc_ki[cdst->mc_top] = ix;
      assert(rc == MDBX_SUCCESS);
    }
  }

  return MDBX_SUCCESS;
}

/* Merge one page into another.
 *
 * The nodes from the page pointed to by csrc will be copied to the page
 * pointed to by cdst and then the csrc page will be freed.
 *
 * [in] csrc Cursor pointing to the source page.
 * [in] cdst Cursor pointing to the destination page.
 *
 * Returns 0 on success, non-zero on failure. */
static int page_merge(cursor_t *csrc, cursor_t *cdst) {
  page_t *psrc, *pdst;
  node_t *srcnode;
  MDBX_iov_t key, data;
  unsigned nkeys;
  int rc;
  unsigned i, j;

  psrc = csrc->mc_pg[csrc->mc_top];
  pdst = cdst->mc_pg[cdst->mc_top];

  mdbx_debug("merging page %" PRIaPGNO " into %" PRIaPGNO "", psrc->mp_pgno, pdst->mp_pgno);

  assert(csrc->mc_snum > 1); /* can't merge root page */
  assert(cdst->mc_snum > 1);

  /* Mark dst as dirty. */
  rc = page_touch(cdst);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* get dst page again now that we've touched it. */
  pdst = cdst->mc_pg[cdst->mc_top];

  /* Move all nodes from src to dst. */
  j = nkeys = page_numkeys(pdst);
  if (IS_DFL(psrc)) {
    key.iov_len = csrc->mc_aht->aa.xsize32;
    key.iov_base = page_data(psrc);
    for (i = 0; i < page_numkeys(psrc); i++, j++) {
      rc = node_add(cdst, j, &key, nullptr, 0, 0);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      key.iov_base = (char *)key.iov_base + key.iov_len;
    }
  } else {
    for (i = 0; i < page_numkeys(psrc); i++, j++) {
      srcnode = node_ptr(psrc, i);
      if (i == 0 && IS_BRANCH(psrc)) {
        cursor_t mn;
        cursor_copy_clearsub(csrc, &mn);
        /* must find the lowest key below src */
        rc = page_search_lowest(&mn);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
        if (IS_DFL(mn.mc_pg[mn.mc_top])) {
          key.iov_len = mn.mc_aht->aa.xsize32;
          key.iov_base = DFLKEY(mn.mc_pg[mn.mc_top], 0, key.iov_len);
        } else {
          node_t *s2 = node_ptr(mn.mc_pg[mn.mc_top], 0);
          key.iov_len = node_get_keysize(s2);
          key.iov_base = NODEKEY(s2);
        }
      } else {
        key.iov_len = srcnode->mn_ksize16;
        key.iov_base = NODEKEY(srcnode);
      }

      data.iov_len = node_get_datasize(srcnode);
      data.iov_base = NODEDATA(srcnode);
      rc = node_add(cdst, j, &key, &data, node_get_pgno(srcnode), srcnode->node_flags8);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
  }

  mdbx_debug("dst page %" PRIaPGNO " now has %u keys (%.1f%% filled)", pdst->mp_pgno, page_numkeys(pdst),
             (float)PAGEFILL(cdst->mc_txn->mt_env, pdst) / 10);

  /* Unlink the src page from parent and add to free list. */
  csrc->mc_top--;
  node_del(csrc, 0);
  if (csrc->mc_ki[csrc->mc_top] == 0) {
    key.iov_len = 0;
    rc = update_key(csrc, &key);
    if (unlikely(rc != MDBX_SUCCESS)) {
      csrc->mc_top++;
      return rc;
    }
  }
  csrc->mc_top++;

  psrc = csrc->mc_pg[csrc->mc_top];
  /* If not operating on GACO, allow this page to be reused
   * in this txn. Otherwise just add to free list. */
  rc = page_loose(csrc, psrc);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  if (IS_LEAF(psrc))
    csrc->mc_aht->aa.leaf_pages--;
  else
    csrc->mc_aht->aa.branch_pages--;
  {
    /* Adjust other cursors pointing to mp */
    const unsigned top = csrc->mc_top;

    for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(csrc)); bundle;
         bundle = bundle->mc_next) {
      cursor_t *scan = (csrc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
      if (scan == csrc)
        continue;
      if (scan->mc_snum < csrc->mc_snum)
        continue;
      if (scan->mc_pg[top] == psrc) {
        scan->mc_pg[top] = pdst;
        assert(nkeys + scan->mc_ki[top] <= UINT16_MAX);
        scan->mc_ki[top] += (indx_t)nkeys;
        scan->mc_ki[top - 1] = cdst->mc_ki[top - 1];
      } else if (scan->mc_pg[top - 1] == csrc->mc_pg[top - 1] && scan->mc_ki[top - 1] > csrc->mc_ki[top - 1]) {
        scan->mc_ki[top - 1]--;
      }
      if (IS_LEAF(psrc))
        cursor_refresh_subcursor(bundle, top, scan->mc_pg[top]);
    }
  }
  {
    unsigned snum = cdst->mc_snum;
    uint16_t depth = cdst->mc_aht->aa.depth16;
    cursor_pop(cdst);
    rc = tree_rebalance(cdst);
    /* Did the tree height change? */
    if (depth != cdst->mc_aht->aa.depth16)
      snum += cdst->mc_aht->aa.depth16 - depth;
    assert(snum >= 1 && snum <= UINT16_MAX);
    cdst->mc_snum = (uint16_t)snum;
    cdst->mc_top = (uint16_t)(snum - 1);
  }
  return rc;
}

/* Rebalance the tree after a delete operation.
 * [in] mc Cursor pointing to the page where rebalancing should begin.
 * Returns 0 on success, non-zero on failure. */
static int tree_rebalance(cursor_t *mc) {
  node_t *node;
  int rc, fromleft;
  unsigned ptop, minkeys, thresh;
  indx_t oldki;

  if (IS_BRANCH(mc->mc_pg[mc->mc_top])) {
    minkeys = 2;
    thresh = 1;
  } else {
    minkeys = 1;
    thresh = FILL_THRESHOLD;
  }
  mdbx_debug("rebalancing %s page %" PRIaPGNO " (has %u keys, %.1f%% full)",
             IS_LEAF(mc->mc_pg[mc->mc_top]) ? "leaf" : "branch", mc->mc_pg[mc->mc_top]->mp_pgno,
             page_numkeys(mc->mc_pg[mc->mc_top]),
             (float)PAGEFILL(mc->mc_txn->mt_env, mc->mc_pg[mc->mc_top]) / 10);

  if (PAGEFILL(mc->mc_txn->mt_env, mc->mc_pg[mc->mc_top]) >= thresh &&
      page_numkeys(mc->mc_pg[mc->mc_top]) >= minkeys) {
    mdbx_debug("no need to rebalance page %" PRIaPGNO ", above fill threshold",
               mc->mc_pg[mc->mc_top]->mp_pgno);
    return MDBX_SUCCESS;
  }

  if (mc->mc_snum < 2) {
    page_t *mp = mc->mc_pg[0];
    unsigned nkeys = page_numkeys(mp);
    if (IS_SUBP(mp)) {
      mdbx_debug("Can't rebalance a subpage, ignoring");
      return MDBX_SUCCESS;
    }
    if (nkeys == 0) {
      mdbx_debug("tree is completely empty");
      mc->mc_aht->aa.root = P_INVALID;
      mc->mc_aht->aa.depth16 = 0;
      mc->mc_aht->aa.leaf_pages = 0;
      rc = mdbx_pnl_append(&mc->mc_txn->mt_befree_pages, mp->mp_pgno);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      /* Adjust cursors pointing to mp */
      for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(mc)); bundle;
           bundle = bundle->mc_next) {
        cursor_t *scan = (mc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
        if (!(scan->mc_state8 & C_INITIALIZED) || (scan->mc_snum < mc->mc_snum))
          continue;
        if (scan->mc_pg[0] == mp) {
          scan->mc_snum = 0;
          scan->mc_top = 0;
          scan->mc_state8 &= ~C_INITIALIZED;
        }
      }
      mc->mc_snum = 0;
      mc->mc_top = 0;
      mc->mc_state8 &= ~C_INITIALIZED;
    } else if (IS_BRANCH(mp) && page_numkeys(mp) == 1) {
      int i;
      mdbx_debug("collapsing root page!");
      rc = mdbx_pnl_append(&mc->mc_txn->mt_befree_pages, mp->mp_pgno);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      mc->mc_aht->aa.root = node_get_pgno(node_ptr(mp, 0));
      rc = page_get(mc->mc_txn, mc->mc_aht->aa.root, &mc->mc_pg[0], nullptr);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      mc->mc_aht->aa.depth16--;
      mc->mc_aht->aa.branch_pages--;
      mc->mc_ki[0] = mc->mc_ki[1];
      for (i = 1; i < mc->mc_aht->aa.depth16; i++) {
        mc->mc_pg[i] = mc->mc_pg[i + 1];
        mc->mc_ki[i] = mc->mc_ki[i + 1];
      }
      /* Adjust other cursors pointing to mp */
      for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(mc)); bundle;
           bundle = bundle->mc_next) {
        cursor_t *scan = (mc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
        if (scan == mc)
          continue;
        if (!(scan->mc_state8 & C_INITIALIZED))
          continue;
        if (scan->mc_pg[0] == mp) {
          for (i = 0; i < mc->mc_aht->aa.depth16; i++) {
            scan->mc_pg[i] = scan->mc_pg[i + 1];
            scan->mc_ki[i] = scan->mc_ki[i + 1];
          }
          scan->mc_snum--;
          scan->mc_top--;
        }
      }
    } else {
      mdbx_debug("root page %" PRIaPGNO " doesn't need rebalancing (flags 0x%x)", mp->mp_pgno, mp->mp_flags16);
    }
    return MDBX_SUCCESS;
  }

  /* The parent (branch page) must have at least 2 pointers,
   * otherwise the tree is invalid. */
  ptop = mc->mc_top - 1;
  assert(page_numkeys(mc->mc_pg[ptop]) > 1);

  /* Leaf page fill factor is below the threshold.
   * Try to move keys from left or right neighbor, or
   * merge with a neighbor page. */

  /* Find neighbors. */
  MDBX_cursor_t mn;
  cursor_copy_clearsub(mc, &mn.primal);

  oldki = mc->mc_ki[mc->mc_top];
  if (mc->mc_ki[ptop] == 0) {
    /* We're the leftmost leaf in our parent. */
    mdbx_debug("reading right neighbor");
    mn.primal.mc_ki[ptop]++;
    node = node_ptr(mc->mc_pg[ptop], mn.primal.mc_ki[ptop]);
    rc = page_get(mc->mc_txn, node_get_pgno(node), &mn.primal.mc_pg[mn.primal.mc_top], nullptr);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mn.primal.mc_ki[mn.primal.mc_top] = 0;
    mc->mc_ki[mc->mc_top] = page_numkeys(mc->mc_pg[mc->mc_top]);
    fromleft = 0;
  } else {
    /* There is at least one neighbor to the left. */
    mdbx_debug("reading left neighbor");
    mn.primal.mc_ki[ptop]--;
    node = node_ptr(mc->mc_pg[ptop], mn.primal.mc_ki[ptop]);
    rc = page_get(mc->mc_txn, node_get_pgno(node), &mn.primal.mc_pg[mn.primal.mc_top], nullptr);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mn.primal.mc_ki[mn.primal.mc_top] = page_numkeys(mn.primal.mc_pg[mn.primal.mc_top]) - 1;
    mc->mc_ki[mc->mc_top] = 0;
    fromleft = 1;
  }

  mdbx_debug("found neighbor page %" PRIaPGNO " (%u keys, %.1f%% full)",
             mn.primal.mc_pg[mn.primal.mc_top]->mp_pgno, page_numkeys(mn.primal.mc_pg[mn.primal.mc_top]),
             (float)PAGEFILL(mc->mc_txn->mt_env, mn.primal.mc_pg[mn.primal.mc_top]) / 10);

  /* If the neighbor page is above threshold and has enough keys,
   * move one key from it. Otherwise we should try to merge them.
   * (A branch page must never have less than 2 keys.) */
  if (PAGEFILL(mc->mc_txn->mt_env, mn.primal.mc_pg[mn.primal.mc_top]) >= thresh &&
      page_numkeys(mn.primal.mc_pg[mn.primal.mc_top]) > minkeys) {
    rc = node_move(&mn.primal, mc, fromleft);
    if (fromleft) {
      /* if we inserted on left, bump position up */
      oldki++;
    }
  } else {
    if (!fromleft) {
      rc = page_merge(&mn.primal, mc);
    } else {
      oldki += page_numkeys(mn.primal.mc_pg[mn.primal.mc_top]);
      assert(mn.primal.mc_ki[mn.primal.mc_top] + mc->mc_ki[mn.primal.mc_top] + 1 < INDX_MAX);
      mn.primal.mc_ki[mn.primal.mc_top] += mc->mc_ki[mn.primal.mc_top] + 1;
      /* We want mdbx_rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = page_merge(mc, &mn.primal));
      cursor_copy(&mn.primal, mc);
    }
    mc->mc_state8 &= ~C_EOF;
  }
  mc->mc_ki[mc->mc_top] = oldki;
  return rc;
}

/* Split a page and insert a new node.
 * Set MDBX_TXN_ERROR on failure.
 * [in,out] mc Cursor pointing to the page and desired insertion index.
 * The cursor will be updated to point to the actual page and index where
 * the node got inserted after the split.
 * [in] newkey The key for the newly inserted node.
 * [in] newdata The data for the newly inserted node.
 * [in] newpgno The page number, if the new node is a branch node.
 * [in] nflags The NODE_ADD_FLAGS for the new node.
 * Returns 0 on success, non-zero on failure. */
static int page_split(cursor_t *mc, MDBX_iov_t *newkey, MDBX_iov_t *newdata, pgno_t newpgno, unsigned nflags) {
  unsigned flags;
  int rc = MDBX_SUCCESS, new_root = 0, did_split = 0;
  pgno_t pgno = 0;
  unsigned i, ptop;
  MDBX_env_t *env = mc->mc_txn->mt_env;
  node_t *node;
  MDBX_iov_t sepkey, rkey, xdata, *rdata = &xdata;
  page_t *copy = nullptr;
  page_t *rp, *pp;
  DKBUF;

  page_t *mp = mc->mc_pg[mc->mc_top];
  unsigned newindx = mc->mc_ki[mc->mc_top];
  unsigned nkeys = page_numkeys(mp);

  mdbx_debug("-----> splitting %s page %" PRIaPGNO " and adding [%s] at index %i/%i",
             IS_LEAF(mp) ? "leaf" : "branch", mp->mp_pgno, DKEY(newkey), mc->mc_ki[mc->mc_top], nkeys);

  /* Create a right sibling. */
  rc = page_new(mc, mp->mp_flags16, 1, &rp);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  rp->mp_leaf2_ksize16 = mp->mp_leaf2_ksize16;
  mdbx_debug("new right sibling: page %" PRIaPGNO "", rp->mp_pgno);

  /* Usually when splitting the root page, the cursor
   * height is 1. But when called from mdbx_update_key,
   * the cursor height may be greater because it walks
   * up the stack while finding the branch slot to update. */
  if (mc->mc_top < 1) {
    rc = page_new(mc, P_BRANCH, 1, &pp);
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
    /* shift current top to make room for new parent */
    for (i = mc->mc_snum; i > 0; i--) {
      mc->mc_pg[i] = mc->mc_pg[i - 1];
      mc->mc_ki[i] = mc->mc_ki[i - 1];
    }
    mc->mc_pg[0] = pp;
    mc->mc_ki[0] = 0;
    mc->mc_aht->aa.root = pp->mp_pgno;
    mdbx_debug("root split! new root = %" PRIaPGNO "", pp->mp_pgno);
    new_root = mc->mc_aht->aa.depth16++;

    /* Add left (implicit) pointer. */
    if (unlikely((rc = node_add(mc, 0, nullptr, nullptr, mp->mp_pgno, 0)) != MDBX_SUCCESS)) {
      /* undo the pre-push */
      mc->mc_pg[0] = mc->mc_pg[1];
      mc->mc_ki[0] = mc->mc_ki[1];
      mc->mc_aht->aa.root = mp->mp_pgno;
      mc->mc_aht->aa.depth16--;
      goto done;
    }
    mc->mc_snum++;
    mc->mc_top++;
    ptop = 0;
  } else {
    ptop = mc->mc_top - 1;
    mdbx_debug("parent branch page is %" PRIaPGNO "", mc->mc_pg[ptop]->mp_pgno);
  }

  MDBX_cursor_t mn;
  cursor_copy_clearsub(mc, &mn.primal);
  mn.primal.mc_pg[mn.primal.mc_top] = rp;
  mn.primal.mc_ki[ptop] = mc->mc_ki[ptop] + 1;

  unsigned split_indx;
  if (nflags & MDBX_IUD_APPEND) {
    mn.primal.mc_ki[mn.primal.mc_top] = 0;
    sepkey = *newkey;
    split_indx = newindx;
    nkeys = 0;
  } else {
    split_indx = (nkeys + 1) / 2;

    if (IS_DFL(rp)) {
      char *split, *ins;
      int x;
      unsigned lsize, rsize, keysize;
      /* Move half of the keys to the right sibling */
      x = mc->mc_ki[mc->mc_top] - split_indx;
      keysize = mc->mc_aht->aa.xsize32;
      split = DFLKEY(mp, split_indx, keysize);
      rsize = (nkeys - split_indx) * keysize;
      lsize = (nkeys - split_indx) * sizeof(indx_t);
      assert(mp->mp_lower >= lsize);
      mp->mp_lower -= (indx_t)lsize;
      assert(rp->mp_lower + lsize <= UINT16_MAX);
      rp->mp_lower += (indx_t)lsize;
      assert(mp->mp_upper + rsize - lsize <= UINT16_MAX);
      mp->mp_upper += (indx_t)(rsize - lsize);
      assert(rp->mp_upper >= rsize - lsize);
      rp->mp_upper -= (indx_t)(rsize - lsize);
      sepkey.iov_len = keysize;
      if (newindx == split_indx) {
        sepkey.iov_base = newkey->iov_base;
      } else {
        sepkey.iov_base = split;
      }
      if (x < 0) {
        assert(keysize >= sizeof(indx_t));
        ins = DFLKEY(mp, mc->mc_ki[mc->mc_top], keysize);
        memcpy(rp->mp_ptrs, split, rsize);
        sepkey.iov_base = rp->mp_ptrs;
        memmove(ins + keysize, ins, (split_indx - mc->mc_ki[mc->mc_top]) * keysize);
        memcpy(ins, newkey->iov_base, keysize);
        assert(UINT16_MAX - mp->mp_lower >= (int)sizeof(indx_t));
        mp->mp_lower += sizeof(indx_t);
        assert(mp->mp_upper >= keysize - sizeof(indx_t));
        mp->mp_upper -= (indx_t)(keysize - sizeof(indx_t));
      } else {
        if (x)
          memcpy(rp->mp_ptrs, split, x * keysize);
        ins = DFLKEY(rp, x, keysize);
        memcpy(ins, newkey->iov_base, keysize);
        memcpy(ins + keysize, split + x * keysize, rsize - x * keysize);
        assert(UINT16_MAX - rp->mp_lower >= (int)sizeof(indx_t));
        rp->mp_lower += sizeof(indx_t);
        assert(rp->mp_upper >= keysize - sizeof(indx_t));
        rp->mp_upper -= (indx_t)(keysize - sizeof(indx_t));
        assert(x <= UINT16_MAX);
        mc->mc_ki[mc->mc_top] = (indx_t)x;
      }
    } else {
      size_t psize, nsize, k;
      /* Maximum free space in an empty page */
      unsigned pmax = env->me_psize - PAGEHDRSZ;
      if (IS_LEAF(mp))
        nsize = leaf_size(env, newkey, newdata);
      else
        nsize = branch_size(env, newkey);
      nsize = EVEN(nsize);

      /* grab a page to hold a temporary copy */
      copy = page_malloc(mc->mc_txn, 1);
      if (unlikely(copy == nullptr)) {
        rc = MDBX_ENOMEM;
        goto done;
      }
      copy->mp_pgno = mp->mp_pgno;
      copy->mp_flags16 = mp->mp_flags16;
      copy->mp_lower = 0;
      assert(env->me_psize - PAGEHDRSZ <= UINT16_MAX);
      copy->mp_upper = (indx_t)(env->me_psize - PAGEHDRSZ);

      /* prepare to insert */
      for (unsigned j = i = 0; i < nkeys; i++) {
        if (i == newindx)
          copy->mp_ptrs[j++] = 0;
        copy->mp_ptrs[j++] = mp->mp_ptrs[i];
      }

      /* When items are relatively large the split point needs
       * to be checked, because being off-by-one will make the
       * difference between success or failure in mdbx_node_add.
       *
       * It's also relevant if a page happens to be laid out
       * such that one half of its nodes are all "small" and
       * the other half of its nodes are "large." If the new
       * item is also "large" and falls on the half with
       * "large" nodes, it also may not fit.
       *
       * As a final tweak, if the new item goes on the last
       * spot on the page (and thus, onto the new page), bias
       * the split so the new page is emptier than the old page.
       * This yields better packing during sequential inserts. */
      int dir;
      if (nkeys < 20 || nsize > pmax / 16 || newindx >= nkeys) {
        /* Find split point */
        psize = 0;
        if (newindx <= split_indx || newindx >= nkeys) {
          i = 0;
          dir = 1;
          k = (newindx >= nkeys) ? nkeys : split_indx + 1 + IS_LEAF(mp);
        } else {
          i = nkeys;
          dir = -1;
          k = split_indx - 1;
        }
        for (; i != k; i += dir) {
          if (i == newindx) {
            psize += nsize;
            node = nullptr;
          } else {
            node = (node_t *)((char *)mp + copy->mp_ptrs[i] + PAGEHDRSZ);
            psize += NODESIZE + node_get_keysize(node) + sizeof(indx_t);
            if (IS_LEAF(mp)) {
              if (unlikely(node->node_flags8 & NODE_BIG))
                psize += sizeof(pgno_t);
              else
                psize += node_get_datasize(node);
            }
            psize = EVEN(psize);
          }
          if (psize > pmax || i == k - dir) {
            split_indx = i + (dir < 0);
            break;
          }
        }
      }
      if (split_indx == newindx) {
        sepkey.iov_len = newkey->iov_len;
        sepkey.iov_base = newkey->iov_base;
      } else {
        node = (node_t *)((char *)mp + copy->mp_ptrs[split_indx] + PAGEHDRSZ);
        sepkey.iov_len = node->mn_ksize16;
        sepkey.iov_base = NODEKEY(node);
      }
    }
  }

  mdbx_debug("separator is %d [%s]", split_indx, DKEY(&sepkey));

  /* Copy separator key to the parent. */
  if (page_spaceleft(mn.primal.mc_pg[ptop]) < branch_size(env, &sepkey)) {
    int snum = mc->mc_snum;
    mn.primal.mc_snum--;
    mn.primal.mc_top--;
    did_split = 1;
    /* We want other splits to find mn when doing fixups */
    WITH_CURSOR_TRACKING(mn, rc = page_split(&mn.primal, &sepkey, nullptr, rp->mp_pgno, 0));
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;

    /* root split? */
    if (snum < (int)mc->mc_snum)
      ptop++;

    /* Right page might now have changed parent.
     * Check if left page also changed parent. */
    if (mn.primal.mc_pg[ptop] != mc->mc_pg[ptop] && mc->mc_ki[ptop] >= page_numkeys(mc->mc_pg[ptop])) {
      for (i = 0; i < ptop; i++) {
        mc->mc_pg[i] = mn.primal.mc_pg[i];
        mc->mc_ki[i] = mn.primal.mc_ki[i];
      }
      mc->mc_pg[ptop] = mn.primal.mc_pg[ptop];
      if (mn.primal.mc_ki[ptop]) {
        mc->mc_ki[ptop] = mn.primal.mc_ki[ptop] - 1;
      } else {
        /* find right page's left sibling */
        mc->mc_ki[ptop] = mn.primal.mc_ki[ptop];
        rc = cursor_sibling(mc, 0);
      }
    }
  } else {
    mn.primal.mc_top--;
    rc = node_add(&mn.primal, mn.primal.mc_ki[ptop], &sepkey, nullptr, rp->mp_pgno, 0);
    mn.primal.mc_top++;
  }
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc == MDBX_NOTFOUND) /* improper mdbx_cr_sibling() result */ {
      mdbx_error("unexpected %s", "MDBX_NOTFOUND");
      rc = MDBX_PROBLEM;
    }
    goto done;
  }
  if (nflags & MDBX_IUD_APPEND) {
    mc->mc_pg[mc->mc_top] = rp;
    mc->mc_ki[mc->mc_top] = 0;
    rc = node_add(mc, 0, newkey, newdata, newpgno, nflags);
    if (rc)
      goto done;
    for (i = 0; i < mc->mc_top; i++)
      mc->mc_ki[i] = mn.primal.mc_ki[i];
  } else if (!IS_DFL(mp)) {
    /* Move nodes */
    mc->mc_pg[mc->mc_top] = rp;
    i = split_indx;
    indx_t n = 0;
    do {
      if (i == newindx) {
        rkey.iov_base = newkey->iov_base;
        rkey.iov_len = newkey->iov_len;
        if (IS_LEAF(mp)) {
          rdata = newdata;
        } else
          pgno = newpgno;
        flags = nflags;
        /* Update index for the new key. */
        mc->mc_ki[mc->mc_top] = n;
      } else {
        node = (node_t *)((char *)mp + copy->mp_ptrs[i] + PAGEHDRSZ);
        rkey.iov_base = NODEKEY(node);
        rkey.iov_len = node->mn_ksize16;
        if (IS_LEAF(mp)) {
          xdata.iov_base = NODEDATA(node);
          xdata.iov_len = node_get_datasize(node);
          rdata = &xdata;
        } else
          pgno = node_get_pgno(node);
        flags = node->node_flags8;
      }

      if (!IS_LEAF(mp) && n == 0) {
        /* First branch index doesn't need key data. */
        rkey.iov_len = 0;
      }

      rc = node_add(mc, n, &rkey, rdata, pgno, flags);
      if (rc)
        goto done;
      if (i == nkeys) {
        i = 0;
        n = 0;
        mc->mc_pg[mc->mc_top] = copy;
      } else {
        i++;
        n++;
      }
    } while (i != split_indx);

    nkeys = page_numkeys(copy);
    for (i = 0; i < nkeys; i++)
      mp->mp_ptrs[i] = copy->mp_ptrs[i];
    mp->mp_lower = copy->mp_lower;
    mp->mp_upper = copy->mp_upper;
    memcpy(node_ptr(mp, nkeys - 1), node_ptr(copy, nkeys - 1), env->me_psize - copy->mp_upper - PAGEHDRSZ);

    /* reset back to original page */
    if (newindx < split_indx) {
      mc->mc_pg[mc->mc_top] = mp;
    } else {
      mc->mc_pg[mc->mc_top] = rp;
      mc->mc_ki[ptop]++;
      /* Make sure mc_ki is still valid. */
      if (mn.primal.mc_pg[ptop] != mc->mc_pg[ptop] && mc->mc_ki[ptop] >= page_numkeys(mc->mc_pg[ptop])) {
        for (i = 0; i <= ptop; i++) {
          mc->mc_pg[i] = mn.primal.mc_pg[i];
          mc->mc_ki[i] = mn.primal.mc_ki[i];
        }
      }
    }
    if (nflags & MDBX_IUD_RESERVE) {
      node = node_ptr(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (likely(!(node->node_flags8 & NODE_BIG)))
        newdata->iov_base = NODEDATA(node);
    }
  } else {
    if (newindx >= split_indx) {
      mc->mc_pg[mc->mc_top] = rp;
      mc->mc_ki[ptop]++;
      /* Make sure mc_ki is still valid. */
      if (mn.primal.mc_pg[ptop] != mc->mc_pg[ptop] && mc->mc_ki[ptop] >= page_numkeys(mc->mc_pg[ptop])) {
        for (i = 0; i <= ptop; i++) {
          mc->mc_pg[i] = mn.primal.mc_pg[i];
          mc->mc_ki[i] = mn.primal.mc_ki[i];
        }
      }
    }
  }

  {
    /* Adjust other cursors pointing to mp */
    nkeys = page_numkeys(mp);
    for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(mc)); bundle; bundle = bundle->mc_next) {
      cursor_t *scan = (mc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
      if (scan == mc)
        continue;
      if (!(bundle->primal.mc_state8 & scan->mc_state8 & C_INITIALIZED))
        continue;
      if (new_root) {
        int k;
        /* sub cursors may be on different AA */
        if (scan->mc_pg[0] != mp)
          continue;
        /* root split */
        for (k = new_root; k >= 0; k--) {
          scan->mc_ki[k + 1] = scan->mc_ki[k];
          scan->mc_pg[k + 1] = scan->mc_pg[k];
        }
        scan->mc_ki[0] = (scan->mc_ki[0] >= nkeys) ? 1 : 0;
        scan->mc_pg[0] = mc->mc_pg[0];
        scan->mc_snum++;
        scan->mc_top++;
      }
      if (scan->mc_top >= mc->mc_top && scan->mc_pg[mc->mc_top] == mp) {
        if (scan->mc_ki[mc->mc_top] >= newindx && !(nflags & MDBX_SPLIT_REPLACE))
          scan->mc_ki[mc->mc_top]++;
        if (scan->mc_ki[mc->mc_top] >= nkeys) {
          scan->mc_pg[mc->mc_top] = rp;
          assert(scan->mc_ki[mc->mc_top] >= nkeys);
          scan->mc_ki[mc->mc_top] -= (indx_t)nkeys;
          for (i = 0; i < mc->mc_top; i++) {
            scan->mc_ki[i] = mn.primal.mc_ki[i];
            scan->mc_pg[i] = mn.primal.mc_pg[i];
          }
        }
      } else if (!did_split && scan->mc_top >= ptop && scan->mc_pg[ptop] == mc->mc_pg[ptop] &&
                 scan->mc_ki[ptop] >= mc->mc_ki[ptop]) {
        scan->mc_ki[ptop]++;
      }
      if (IS_LEAF(mp))
        cursor_refresh_subcursor(bundle, mc->mc_top, scan->mc_pg[mc->mc_top]);
    }
  }
  mdbx_debug("mp left: %d, rp left: %d", page_spaceleft(mp), page_spaceleft(rp));

done:
  if (copy) /* tmp page */
    dpage_free(env, copy);
  if (unlikely(rc != MDBX_SUCCESS))
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

#ifndef MDBX_WBUF
#define MDBX_WBUF (1024 * 1024)
#endif
#define MDBX_EOF 0x10 /* mdbx_bk_copyfd1() is done reading */

/* State needed for a double-buffering compacting copy. */
struct copy_ctx_ {
  MDBX_env_t *mc_book;
  MDBX_txn_t *mc_txn;
  mdbx_condmutex_t mc_condmutex;
  char *mc_wbuf[2];
  char *mc_over[2];
  size_t mc_wlen[2];
  size_t mc_olen[2];
  MDBX_filehandle_t mc_fd;
  volatile int mc_error;
  pgno_t mc_next_pgno;
  short mc_toggle; /* Buffer number in provider */
  short mc_new;    /* (0-2 buffers to write) | (MDBX_EOF at end) */
                   /* Error code.  Never cleared if set.  Both threads can set nonzero
                    * to fail the copy.  Not mutex-protected, MDBX expects atomic int. */
};

/* Dedicated writer thread for compacting copy. */
static THREAD_RESULT __cold THREAD_CALL mdbx_bk_copythr(void *arg) {
  copy_ctx_t *my = arg;
  char *ptr;
  int toggle = 0;
  int rc;

#if defined(F_SETNOSIGPIPE)
  /* OS X delivers SIGPIPE to the whole process, not the thread that caused
   * it.
   * Disable SIGPIPE using platform specific fcntl. */
  int enabled = 1;
  if (fcntl(my->mc_fd, F_SETNOSIGPIPE, &enabled))
    my->mc_error = mdbx_get_errno();
#endif

#if defined(SIGPIPE) && !defined(_WIN32) && !defined(_WIN64)
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  rc = pthread_sigmask(SIG_BLOCK, &set, nullptr);
  if (rc != 0)
    my->mc_error = rc;
#endif

  mdbx_condmutex_lock(&my->mc_condmutex);
  while (!my->mc_error) {
    while (!my->mc_new)
      mdbx_condmutex_wait(&my->mc_condmutex);
    if (my->mc_new == 0 + MDBX_EOF) /* 0 buffers, just EOF */
      break;
    size_t wsize = my->mc_wlen[toggle];
    ptr = my->mc_wbuf[toggle];
  again:
    if (wsize > 0 && !my->mc_error) {
      rc = mdbx_write(my->mc_fd, ptr, wsize);
      if (rc != MDBX_SUCCESS) {
#if defined(SIGPIPE) && !defined(_WIN32) && !defined(_WIN64)
        if (rc == EPIPE) {
          /* Collect the pending SIGPIPE, otherwise (at least OS X)
           * gives it to the process on thread-exit (ITS#8504). */
          int tmp;
          sigwait(&set, &tmp);
        }
#endif
        my->mc_error = rc;
      }
    }

    /* If there's an overflow page tail, write it too */
    if (my->mc_olen[toggle]) {
      wsize = my->mc_olen[toggle];
      ptr = my->mc_over[toggle];
      my->mc_olen[toggle] = 0;
      goto again;
    }
    my->mc_wlen[toggle] = 0;
    toggle ^= 1;
    /* Return the empty buffer to provider */
    my->mc_new--;
    mdbx_condmutex_signal(&my->mc_condmutex);
  }
  mdbx_condmutex_unlock(&my->mc_condmutex);
  return (THREAD_RESULT)0;
}

/* Give buffer and/or MDBX_EOF to writer thread, await unused buffer.
 *
 * [in] my control structure.
 * [in] adjust (1 to hand off 1 buffer) | (MDBX_EOF when ending). */
static int __cold mdbx_bk_cthr_toggle(copy_ctx_t *my, int adjust) {
  mdbx_condmutex_lock(&my->mc_condmutex);
  my->mc_new += (short)adjust;
  mdbx_condmutex_signal(&my->mc_condmutex);
  while (my->mc_new & 2) /* both buffers in use */
    mdbx_condmutex_wait(&my->mc_condmutex);
  mdbx_condmutex_unlock(&my->mc_condmutex);

  my->mc_toggle ^= (adjust & 1);
  /* Both threads reset mc_wlen, to be safe from threading errors */
  my->mc_wlen[my->mc_toggle] = 0;
  return my->mc_error;
}

/* Depth-first tree traversal for compacting copy.
 * [in] my control structure.
 * [in,out] pg database root.
 * [in] flags includes NODE_DUP if it is a sorted-duplicate sub-AA. */
static int __cold bk_copy_walk(copy_ctx_t *my, pgno_t *pg, int flags) {
  MDBX_cursor_t mc;
  node_t *ni;
  page_t *mo, *mp, *leaf;
  char *buf, *ptr;
  int rc, toggle;
  unsigned i;

  /* Empty AA, nothing to do */
  if (*pg == P_INVALID)
    return MDBX_SUCCESS;

  memset(&mc, 0, sizeof(mc));
  mc.primal.mc_snum = 1;
  mc.primal.mc_txn = my->mc_txn;

  rc = page_get(my->mc_txn, *pg, &mc.primal.mc_pg[0], nullptr);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  rc = page_search_root(&mc.primal, nullptr, MDBX_PS_FIRST);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Make cursor pages writable */
  buf = ptr = malloc(pgno2bytes(my->mc_book, mc.primal.mc_snum));
  if (buf == nullptr)
    return MDBX_ENOMEM;

  for (i = 0; i < mc.primal.mc_top; i++) {
    page_copy((page_t *)ptr, mc.primal.mc_pg[i], my->mc_book->me_psize);
    mc.primal.mc_pg[i] = (page_t *)ptr;
    ptr += my->mc_book->me_psize;
  }

  /* This is writable space for a leaf page. Usually not needed. */
  leaf = (page_t *)ptr;

  toggle = my->mc_toggle;
  while (mc.primal.mc_snum > 0) {
    unsigned n;
    mp = mc.primal.mc_pg[mc.primal.mc_top];
    n = page_numkeys(mp);

    if (IS_LEAF(mp)) {
      if (!IS_DFL(mp) && !(flags & NODE_DUP)) {
        for (i = 0; i < n; i++) {
          ni = node_ptr(mp, i);
          if (unlikely(ni->node_flags8 & NODE_BIG)) {
            page_t *omp;

            /* Need writable leaf */
            if (mp != leaf) {
              mc.primal.mc_pg[mc.primal.mc_top] = leaf;
              page_copy(leaf, mp, my->mc_book->me_psize);
              mp = leaf;
              ni = node_ptr(mp, i);
            }

            pgno_t pgno = get_pgno_aligned2(NODEDATA(ni));
            set_pgno_aligned2(NODEDATA(ni), my->mc_next_pgno);
            rc = page_get(my->mc_txn, pgno, &omp, nullptr);
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
            if (my->mc_wlen[toggle] >= MDBX_WBUF) {
              rc = mdbx_bk_cthr_toggle(my, 1);
              if (unlikely(rc != MDBX_SUCCESS))
                goto done;
              toggle = my->mc_toggle;
            }
            mo = (page_t *)(my->mc_wbuf[toggle] + my->mc_wlen[toggle]);
            memcpy(mo, omp, my->mc_book->me_psize);
            mo->mp_pgno = my->mc_next_pgno;
            my->mc_next_pgno += omp->mp_pages;
            my->mc_wlen[toggle] += my->mc_book->me_psize;
            if (omp->mp_pages > 1) {
              my->mc_olen[toggle] = pgno2bytes(my->mc_book, omp->mp_pages - 1);
              my->mc_over[toggle] = (char *)omp + my->mc_book->me_psize;
              rc = mdbx_bk_cthr_toggle(my, 1);
              if (unlikely(rc != MDBX_SUCCESS))
                goto done;
              toggle = my->mc_toggle;
            }
          } else if (ni->node_flags8 & NODE_SUBTREE) {
            aatree_t db;

            /* Need writable leaf */
            if (mp != leaf) {
              mc.primal.mc_pg[mc.primal.mc_top] = leaf;
              page_copy(leaf, mp, my->mc_book->me_psize);
              mp = leaf;
              ni = node_ptr(mp, i);
            }

            memcpy(&db, NODEDATA(ni), sizeof(db));
            my->mc_toggle = (short)toggle;
            rc = bk_copy_walk(my, &db.aa_root, ni->node_flags8 & NODE_DUP);
            if (rc)
              goto done;
            toggle = my->mc_toggle;
            memcpy(NODEDATA(ni), &db, sizeof(db));
          }
        }
      }
    } else {
      mc.primal.mc_ki[mc.primal.mc_top]++;
      if (mc.primal.mc_ki[mc.primal.mc_top] < n) {
        pgno_t pgno;
      again:
        ni = node_ptr(mp, mc.primal.mc_ki[mc.primal.mc_top]);
        pgno = node_get_pgno(ni);
        rc = page_get(my->mc_txn, pgno, &mp, nullptr);
        if (unlikely(rc != MDBX_SUCCESS))
          goto done;
        mc.primal.mc_top++;
        mc.primal.mc_snum++;
        mc.primal.mc_ki[mc.primal.mc_top] = 0;
        if (IS_BRANCH(mp)) {
          /* Whenever we advance to a sibling branch page,
           * we must proceed all the way down to its first leaf. */
          page_copy(mc.primal.mc_pg[mc.primal.mc_top], mp, my->mc_book->me_psize);
          goto again;
        } else
          mc.primal.mc_pg[mc.primal.mc_top] = mp;
        continue;
      }
    }
    if (my->mc_wlen[toggle] >= MDBX_WBUF) {
      rc = mdbx_bk_cthr_toggle(my, 1);
      if (unlikely(rc != MDBX_SUCCESS))
        goto done;
      toggle = my->mc_toggle;
    }
    mo = (page_t *)(my->mc_wbuf[toggle] + my->mc_wlen[toggle]);
    page_copy(mo, mp, my->mc_book->me_psize);
    mo->mp_pgno = my->mc_next_pgno++;
    my->mc_wlen[toggle] += my->mc_book->me_psize;
    if (mc.primal.mc_top) {
      /* Update parent if there is one */
      ni = node_ptr(mc.primal.mc_pg[mc.primal.mc_top - 1], mc.primal.mc_ki[mc.primal.mc_top - 1]);
      node_set_pgno(ni, mo->mp_pgno);
      cursor_pop(&mc.primal);
    } else {
      /* Otherwise we're done */
      *pg = mo->mp_pgno;
      break;
    }
  }
done:
  free(buf);
  return rc;
}

/* Copy databook with compaction. */
static int __cold bk_copy_compact(MDBX_env_t *env, MDBX_filehandle_t fd) {
  mdbx_thread_t thr;
  copy_ctx_t my;
  memset(&my, 0, sizeof(my));

  int rc = mdbx_condmutex_init(&my.mc_condmutex);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  my.mc_wbuf[0] = env->ops.memory.ops_aligned_alloc(osal_syspagesize, MDBX_WBUF * 2, env);
  if (unlikely(my.mc_wbuf == nullptr)) {
    rc = MDBX_ENOMEM;
    goto done;
  }

  memset(my.mc_wbuf[0], 0, MDBX_WBUF * 2);
  my.mc_wbuf[1] = my.mc_wbuf[0] + MDBX_WBUF;
  my.mc_next_pgno = MDBX_NUM_METAS;
  my.mc_book = env;
  my.mc_fd = fd;
  rc = mdbx_thread_create(&thr, mdbx_bk_copythr, &my);
  if (unlikely(rc != MDBX_SUCCESS))
    goto done;

  const MDBX_txn_result_t tr = mdbx_begin(env, nullptr, MDBX_RDONLY);
  rc = tr.err;
  my.mc_txn = tr.txn;
  if (unlikely(rc != MDBX_SUCCESS))
    goto finish;

  page_t *meta = init_metas(env, my.mc_wbuf[0]);

  /* Set metapage 1 with current main AA */
  pgno_t new_root, root = aht_main(my.mc_txn)->aa.root;
  if ((new_root = root) != P_INVALID) {
    /* Count free pages + GACO pages.  Subtract from last_pg
     * to find the new last_pg, which also becomes the new root. */
    pgno_t freecount = 0;
    MDBX_cursor_t mc;
    MDBX_iov_t key, data;

    rc = cursor_init(&mc, my.mc_txn, aht_gaco(my.mc_txn));
    if (unlikely(rc != MDBX_SUCCESS))
      goto finish;
    while ((rc = mdbx_cursor_get(&mc, &key, &data, MDBX_NEXT)) == 0)
      freecount += *(pgno_t *)data.iov_base;
    if (unlikely(rc != MDBX_NOTFOUND))
      goto finish;

    freecount += my.mc_txn->txn_aht_array[MDBX_GACO_AAH].aa.branch_pages +
                 my.mc_txn->txn_aht_array[MDBX_GACO_AAH].aa.leaf_pages +
                 my.mc_txn->txn_aht_array[MDBX_GACO_AAH].aa.overflow_pages;

    new_root = my.mc_txn->mt_next_pgno - 1 - freecount;
    meta->mp_meta.mm_dxb_geo.next = meta->mp_meta.mm_dxb_geo.now = new_root + 1;
    aa_txn2db(env, &my.mc_txn->txn_aht_array[MDBX_MAIN_AAH], &meta->mp_meta.mm_aas[MDBX_MAIN_AAH], af_main);
    meta->mp_meta.mm_aas[MDBX_MAIN_AAH].aa_root = new_root;
  } else {
    /* When the AA is empty, handle it specially to
     * fix any breakage like page leaks from ITS#8174. */
    meta->mp_meta.mm_aas[MDBX_MAIN_AAH].aa_flags16 = my.mc_txn->txn_aht_array[MDBX_MAIN_AAH].aa.flags16;
  }

  /* copy canary sequenses if present */
  if (my.mc_txn->mt_canary.v) {
    meta->mp_meta.mm_canary = my.mc_txn->mt_canary;
    meta->mp_meta.mm_canary.v = meta_txnid_stable(env, &meta->mp_meta);
  }

  /* update signature */
  meta->mp_meta.mm_sign_checksum = meta_sign(&meta->mp_meta);

  my.mc_wlen[0] = pgno2bytes(env, MDBX_NUM_METAS);
  rc = bk_copy_walk(&my, &root, 0);
  if (rc == MDBX_SUCCESS && root != new_root) {
    mdbx_error("unexpected root %" PRIaPGNO " (%" PRIaPGNO ")", root, new_root);
    rc = MDBX_PROBLEM; /* page leak or corrupt databook */
  }

finish:
  if (rc != MDBX_SUCCESS)
    my.mc_error = rc;
  mdbx_bk_cthr_toggle(&my, 1 | MDBX_EOF);
  rc = mdbx_thread_join(thr);
  mdbx_abort(my.mc_txn);

done:
  env->ops.memory.ops_aligned_free(my.mc_wbuf[0], env);
  mdbx_condmutex_destroy(&my.mc_condmutex);
  return rc ? rc : my.mc_error;
}

/* Copy databook as-is. */
static int __cold bk_copy_asis(MDBX_env_t *env, MDBX_filehandle_t fd) {
  /* Do the lock/unlock of the reader mutex before starting the
   * write txn.  Otherwise other read txns could block writers. */
  MDBX_txn_result_t tr = mdbx_begin(env, nullptr, MDBX_RDONLY);
  if (unlikely(tr.err != MDBX_SUCCESS))
    return tr.err;

  /* We must start the actual read txn after blocking writers */
  tr.err = txn_end(tr.txn, MDBX_END_RESET_TMP);
  if (unlikely(tr.err != MDBX_SUCCESS))
    goto bailout; /* FIXME: or just return? */

  if ((env->me_flags32 & MDBX_EXCLUSIVE) == 0) {
    /* Temporarily block writers until we snapshot the meta pages */
    tr.err = lck_writer_acquire(env, 0);
    if (unlikely(tr.err != MDBX_SUCCESS))
      goto bailout;

    tr.err = txn_renew(tr.txn, MDBX_RDONLY);
    if (unlikely(tr.err != MDBX_SUCCESS)) {
      lck_writer_release(env);
      goto bailout;
    }
  }

  tr.err = mdbx_write(fd, env->me_map, pgno2bytes(env, MDBX_NUM_METAS));
  meta_t *const head = meta_head(env);
  const uint64_t size = mdbx_roundup2(pgno2bytes(env, head->mm_dxb_geo.now), osal_syspagesize);
  if ((env->me_flags32 & MDBX_EXCLUSIVE) == 0)
    lck_writer_release(env);

  if (likely(tr.err == MDBX_SUCCESS))
    tr.err = mdbx_write(fd, env->me_map + pgno2bytes(env, MDBX_NUM_METAS),
                        pgno2bytes(env, tr.txn->mt_next_pgno - MDBX_NUM_METAS));

  if (likely(tr.err == MDBX_SUCCESS))
    tr.err = mdbx_ftruncate(fd, size);

bailout:
  mdbx_abort(tr.txn);
  return tr.err;
}

int __cold mdbx_bk_set_flags(MDBX_env_t *env, unsigned flags, int onoff) {
  if (unlikely(flags & ~MDBX_REGIME_CHANGEABLE))
    return MDBX_EINVAL;

  if (likely((env->me_flags32 & MDBX_EXCLUSIVE) == 0)) {
    int rc = lck_writer_acquire(env, 0);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  /* FIXME: TODO */
  if (onoff)
    env->me_flags32 |= flags;
  else
    env->me_flags32 &= ~flags;

  if (likely((env->me_flags32 & MDBX_EXCLUSIVE) == 0))
    lck_writer_release(env);
  return MDBX_SUCCESS;
}

/* Depth-first tree traversal. */
static int __cold do_walk(walk_ctx_t *ctx, const MDBX_iov_t ident, pgno_t pg, int deep) {
  page_t *mp;
  int rc, i, nkeys;
  size_t header_size, unused_size, payload_size, align_bytes;
  const char *type;

  if (pg == P_INVALID)
    return MDBX_SUCCESS; /* empty db */

  MDBX_cursor_t mc;
  memset(&mc, 0, sizeof(mc));
  mc.primal.mc_snum = 1;
  mc.primal.mc_txn = ctx->mw_txn;

  rc = page_get(ctx->mw_txn, pg, &mp, nullptr);
  if (rc)
    return rc;
  if (pg != mp->mp_pgno)
    return MDBX_CORRUPTED;

  nkeys = page_numkeys(mp);
  header_size = IS_DFL(mp) ? PAGEHDRSZ : PAGEHDRSZ + mp->mp_lower;
  unused_size = page_spaceleft(mp);
  payload_size = 0;

  /* LY: Don't use mask here, e.g bitwise
   * (P_BRANCH|P_LEAF|P_DFL|P_META|P_OVERFLOW|P_SUBP).
   * Pages should not me marked dirty/loose or otherwise. */
  switch (mp->mp_flags16) {
  case P_BRANCH:
    type = "branch";
    if (nkeys < 1)
      return MDBX_CORRUPTED;
    break;
  case P_LEAF:
    type = "leaf";
    break;
  case P_LEAF | P_SUBP:
    type = "dupsort-subleaf";
    break;
  case P_LEAF | P_DFL:
    type = "dupfixed-leaf";
    break;
  case P_LEAF | P_DFL | P_SUBP:
    type = "dupsort-dupfixed-subleaf";
    break;
  case P_META:
  case P_OVERFLOW:
  default:
    return MDBX_CORRUPTED;
  }

  for (align_bytes = i = 0; i < nkeys; align_bytes += ((payload_size + align_bytes) & 1), i++) {
    node_t *node;

    if (IS_DFL(mp)) {
      /* DFL pages have no mp_ptrs[] or node headers */
      payload_size += mp->mp_leaf2_ksize16;
      continue;
    }

    node = node_ptr(mp, i);
    payload_size += NODESIZE + node->mn_ksize16;

    if (IS_BRANCH(mp)) {
      rc = do_walk(ctx, ident, node_get_pgno(node), deep);
      if (rc)
        return rc;
      continue;
    }

    assert(IS_LEAF(mp));
    if (unlikely(node->node_flags8 & NODE_BIG)) {
      page_t *omp;
      pgno_t *opg;
      size_t over_header, over_payload, over_unused;

      payload_size += sizeof(pgno_t);
      opg = NODEDATA(node);
      rc = page_get(ctx->mw_txn, *opg, &omp, nullptr);
      if (rc)
        return rc;
      if (*opg != omp->mp_pgno)
        return MDBX_CORRUPTED;
      /* LY: Don't use mask here, e.g bitwise
       * (P_BRANCH|P_LEAF|P_DFL|P_META|P_OVERFLOW|P_SUBP).
       * Pages should not me marked dirty/loose or otherwise. */
      if (P_OVERFLOW != omp->mp_flags16)
        return MDBX_CORRUPTED;

      over_header = PAGEHDRSZ;
      over_payload = node_get_datasize(node);
      over_unused = pgno2bytes(ctx->mw_txn->mt_env, omp->mp_pages) - over_payload - over_header;

      rc = ctx->mw_visitor(*opg, omp->mp_pages, ctx->mw_user, ident, "overflow-data", 1, over_payload,
                           over_header, over_unused);
      if (rc)
        return rc;
      continue;
    }

    payload_size += node_get_datasize(node);
    if (node->node_flags8 & NODE_SUBTREE) {
      const aatree_t *unaligned_alien_db = NODEDATA(node);
      MDBX_iov_t aa_ident = ident;

      if (!(node->node_flags8 & NODE_DUP)) {
        aa_ident.iov_base = NODEKEY(node);
        aa_ident.iov_len = (size_t)((char *)unaligned_alien_db - (char *)aa_ident.iov_base);
      }
      rc = do_walk(ctx, aa_ident, get_le32_unaligned(&unaligned_alien_db->aa_root), deep + 1);
      if (rc)
        return rc;
    }
  }

  return ctx->mw_visitor(mp->mp_pgno, 1, ctx->mw_user, ident, type, nkeys, payload_size, header_size,
                         unused_size + align_bytes);
}
