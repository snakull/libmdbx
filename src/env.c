/*
 * Copyright 2015-2018 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * This code is derived from "LMDB engine" written by
 * Howard Chu (Symas Corporation), which itself derived from btree.c
 * written by Martin Hedenfalk.
 *
 * ---
 *
 * Portions Copyright 2011-2015 Howard Chu, Symas Corp. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * ---
 *
 * Portions Copyright (c) 2009, 2010 Martin Hedenfalk <martin@bzero.se>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. */

#include "./bits.h"
#include "./debug.h"
#include "./proto.h"
#include "./ualb.h"

#define env_extra(fmt, ...) log_extra(MDBX_LOG_ENV, fmt, ##__VA_ARGS__)
#define env_trace(fmt, ...) log_trace(MDBX_LOG_ENV, fmt, ##__VA_ARGS__)
#define env_verbose(fmt, ...) log_verbose(MDBX_LOG_ENV, fmt, ##__VA_ARGS__)
#define env_info(fmt, ...) log_info(MDBX_LOG_ENV, fmt, ##__VA_ARGS__)
#define env_notice(fmt, ...) log_notice(MDBX_LOG_ENV, fmt, ##__VA_ARGS__)
#define env_warning(fmt, ...) log_warning(MDBX_LOG_ENV, fmt, ##__VA_ARGS__)
#define env_error(fmt, ...) log_error(MDBX_LOG_ENV, fmt, ##__VA_ARGS__)
#define env_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_ENV, __func__, __LINE__, "%s, error %d", msg, err)

//-----------------------------------------------------------------------------

static void __cold env_destroy(MDBX_env_t *env) {
  if (env->me_flags32 & MDBX_ENV_ACTIVE) {
    env->me_flags32 &= ~MDBX_ENV_ACTIVE;

    for (page_t *dp; (dp = env->me_dpages) != nullptr;) {
      ASAN_UNPOISON_MEMORY_REGION(&dp->mp_next, sizeof(dp->mp_next));
      VALGRIND_MAKE_MEM_DEFINED(&dp->mp_next, sizeof(dp->mp_next));
      env->me_dpages = dp->mp_next;
      free(dp);
    }

    /* Doing this here since me_dbxs may not exist during mdbx_bk_close */
    if (env->env_ahe_array) {
      for (unsigned i = env->env_ah_max; --i >= CORE_AAH;)
        aa_release(env, &env->env_ahe_array[i]);
      free(env->env_ahe_array);
    }

    free(env->me_pagebuf);
    free(env->me_dirtylist);
    if (env->me_wpa_txn) {
      mdbx_txl_free(env->me_wpa_txn->mt_lifo_reclaimed);
      free(env->me_wpa_txn);
    }
    mdbx_pnl_free(env->me_free_pgs);

    if (env->me_flags32 & MDBX_ENV_TXKEY) {
      rthc_release(env->me_txkey);
      env->me_flags32 &= ~MDBX_ENV_TXKEY;
    }

    if (env->me_map) {
      mdbx_munmap(&env->me_dxb_mmap);
#ifdef USE_VALGRIND
      VALGRIND_DISCARD(env->me_valgrind_handle);
      env->me_valgrind_handle = -1;
#endif
    }

    if (env->me_lck) {
      if (env->me_live_reader && env->me_live_reader == mdbx_getpid())
        env->ops.locking.ops_reader_alive_clear(env, env->me_live_reader);
      mdbx_munmap(&env->me_lck_mmap);
    }
    env->me_oldest = nullptr;

    env->ops.locking.ops_detach(env);
    if (env->me_dxb_fd != MDBX_INVALID_FD) {
      (void)mdbx_closefile(env->me_dxb_fd);
      env->me_dxb_fd = MDBX_INVALID_FD;
    }
    if (env->me_lck_fd != MDBX_INVALID_FD) {
      (void)mdbx_closefile(env->me_lck_fd);
      env->me_lck_fd = MDBX_INVALID_FD;
    }

    env->me_pathname_lck = nullptr;
    env->me_pathname_dxb = nullptr;
    env->me_pathname_ovf = nullptr;
    free(env->me_pathname_buf);
    env->me_pathname_buf = nullptr;
  }

  VALGRIND_DESTROY_MEMPOOL(env);
  mdbx_ensure(env, mdbx_fastmutex_destroy(&env->me_aah_lock) == MDBX_SUCCESS);
#if defined(_WIN32) || defined(_WIN64)
  /* me_remap_guard don't have destructor (Slim Reader/Writer Lock) */
  DeleteCriticalSection(&env->me_windowsbug_lock);
#else
  mdbx_ensure(env, mdbx_fastmutex_destroy(&env->me_remap_guard) == MDBX_SUCCESS);
#endif /* Windows */
}

static __cold int env_shutdown(MDBX_env_t *env, MDBX_shutdown_mode_t mode) {
  const uint32_t snap_flags = env->me_flags32 & (MDBX_RDONLY | MDBX_ENV_ACTIVE | MDBX_ENV_TAINTED);
  int rc = (snap_flags & MDBX_ENV_TAINTED) ? MDBX_SIGN : MDBX_SUCCESS;
  if (unlikely(snap_flags != MDBX_ENV_ACTIVE || !env->me_wpa_txn)) {
    env_warning("<< environment read-only/not-active/tainted (0x%x), skip db-sync, rc %d", snap_flags, rc);
    return rc;
  }

  mdbx_assert(env, env->me_wpa_txn != nullptr);
  if (env->me_wpa_txn->mt_owner) {
    env->me_wpa_txn->mt_flags |= MDBX_TXN_FINISHED | MDBX_TXN_ERROR;
    env->me_flags32 |= MDBX_ENV_TAINTED;
    rc = MDBX_SIGN;
    env_trace("<< write-txn pending, skip db-sync, rc %d", rc);
    return rc;
  }

  bool should_downgrade = false;
  switch (mode) {
  default:
    env_trace("unknown shutdown-mode %d, fallback to default", (int)mode);
  /* fallthrough */
  case MDBX_shutdown_default:
    env_trace("shutdown-mode=default");
    if (env->me_flags32 & MDBX_EXCLUSIVE) {
      env_trace("exclusive mode, fallback to shutdown-mode=sync");
    } else {
      rc = env->ops.locking.ops_upgrade(env, MDBX_NONBLOCK);
      if (rc == MDBX_SIGN) {
        env_trace("<< at lease one other writer present, skip db-sync");
        return MDBX_SUCCESS;
      } else if (rc == MDBX_SUCCESS) {
        env_trace("got exclusive mode, db-sync needed");
        env->me_wpa_txn->mt_owner = mdbx_thread_self();
        should_downgrade = true;
      } else if (rc == MDBX_EBUSY)
        env_trace("other process uses DB, but NOT sure to skip db-sync");
      else {
        mdbx_error("failed upgrade-to-exclusive %d, assume db-sync needed", rc);
        if (unlikely(env->me_flags32 & MDBX_ENV_TAINTED)) {
          env_trace("<< environment got tainted, skip db-sync");
          return MDBX_SIGN;
        }
      }
    }
  /* fallthrough */
  case MDBX_shutdown_sync:
    env_trace("shutdown-mode=sync, perform db-sync");
    rc = mdbx_sync(env);
    if (should_downgrade)
      env->ops.locking.ops_downgrade(env);
    env_trace("<< rc %d", rc);
    return rc;
  case MDBX_shutdown_dirty:
    env_trace("<< shutdown-mode=dirty, MDBX_SUCCESS");
    return MDBX_SUCCESS;
  }
}

//-----------------------------------------------------------------------------

/* Insert pid into list if not already present.
 * return -1 if already present. */
static int __cold mdbx_pid_insert(MDBX_pid_t *ids, MDBX_pid_t pid) {
  /* binary search of pid in list */
  unsigned base = 0;
  unsigned cursor = 1;
  int val = 0;
  unsigned n = ids[0];

  while (n > 0) {
    unsigned pivot = n >> 1;
    cursor = base + pivot + 1;
    val = pid - ids[cursor];

    if (val < 0) {
      n = pivot;
    } else if (val > 0) {
      base = cursor;
      n -= pivot + 1;
    } else {
      /* found, so it's a duplicate */
      return -1;
    }
  }

  if (val > 0)
    ++cursor;

  ids[0]++;
  for (n = ids[0]; n > cursor; n--)
    ids[n] = ids[n - 1];
  ids[n] = pid;
  return 0;
}

/* Return:
 *  MDBX_SIGN - done and mutex recovered
 *  MDBX_SUCCESS     - done
 *  Otherwise errcode. */
MDBX_numeric_result_t __cold check_registered_readers(MDBX_env_t *env, int rdt_locked) {
  assert(rdt_locked >= 0);
  MDBX_numeric_result_t result;
  result.value = 0;

  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags32 |= MDBX_ENV_TAINTED;
    result.err = MDBX_PANIC;
    return result;
  }

  MDBX_lockinfo_t *const lck = env->me_lck;
  const unsigned snap_nreaders = lck->li_numreaders;
  MDBX_pid_t *pids = alloca((snap_nreaders + 1) * sizeof(MDBX_pid_t));
  pids[0] = 0;

  result.err = MDBX_SUCCESS;
  for (unsigned i = 0; i < snap_nreaders; i++) {
    const MDBX_pid_t pid = lck->li_readers[i].mr_pid;
    if (pid == 0)
      continue /* skip empty */;
    if (pid == env->me_pid)
      continue /* skip self */;
    if (mdbx_pid_insert(pids, pid) != 0)
      continue /* such pid already processed */;

    int err = env->ops.locking.ops_reader_alive_check(env, pid);
    if (err == MDBX_SUCCESS)
      continue /* reader is live */;

    if (err != MDBX_SIGN) {
      result.err = err;
      break /* mdbx_lck_reader_alive_check() failed */;
    }

    /* stale reader found */
    if (!rdt_locked) {
      err = env->ops.locking.ops_reader_registration_lock(env, env->me_flags32 & MDBX_NONBLOCK);
      if (MDBX_IS_ERROR(err)) {
        result.err = err;
        break;
      }

      rdt_locked = -1;
      if (err == MDBX_SIGN) {
        /* roubust mutex recovered,
         * the mdbx_robust_mutex_failed() checked all readers */
        result.err = MDBX_SIGN;
        break;
      }

      /* a other process may have clean and reused slot, recheck */
      if (lck->li_readers[i].mr_pid != pid)
        continue;

      err = env->ops.locking.ops_reader_alive_check(env, pid);
      if (MDBX_IS_ERROR(err)) {
        result.err = err;
        break;
      }

      if (err == MDBX_SUCCESS)
        continue /* the race with other process, slot reused */;
    }

    /* clean it */
    for (unsigned j = i; j < snap_nreaders; j++) {
      if (lck->li_readers[j].mr_pid == pid) {
        mdbx_debug("clear stale reader pid %" PRIuPTR " txn %" PRIaTXN "", (size_t)pid,
                   lck->li_readers[j].mr_txnid);
        lck->li_readers[j].mr_pid = 0;
        lck->li_readers_refresh_flag = true;
        result.value++;
      }
    }
  }

  if (rdt_locked < 0)
    env->ops.locking.ops_reader_registration_unlock(env);

  return result;
}

static txnid_t __cold rbr(MDBX_env_t *env, const txnid_t laggard) {
  mdbx_debug("databook size maxed out");

  int retry;
  for (retry = 0; retry < INT_MAX; ++retry) {
    txnid_t oldest = reclaiming_detent(env);
    mdbx_assert(env, oldest < env->me_wpa_txn->mt_txnid);
    mdbx_assert(env, oldest >= laggard);
    mdbx_assert(env, oldest >= env->me_oldest[0]);
    if (oldest == laggard)
      return oldest;

    if (MDBX_IS_ERROR(check_registered_readers(env, false).err))
      break;

    MDBX_reader_t *const rtbl = env->me_lck->li_readers;
    MDBX_reader_t *asleep = nullptr;
    for (int i = env->me_lck->li_numreaders; --i >= 0;) {
      if (rtbl[i].mr_pid) {
        jitter4testing(true);
        const txnid_t snap = rtbl[i].mr_txnid;
        if (oldest > snap && laggard <= /* ignore pending updates */ snap) {
          oldest = snap;
          asleep = &rtbl[i];
        }
      }
    }

    if (laggard < oldest || !asleep) {
      if (retry && env->me_callback_rbr) {
        /* LY: notify end of RBR-loop */
        const txnid_t gap = oldest - laggard;
        env->me_callback_rbr(env, 0, 0, laggard, (gap < UINT_MAX) ? (unsigned)gap : UINT_MAX, -retry);
      }
      env_notice("RBR-kick: update oldest %" PRIaTXN " -> %" PRIaTXN, env->me_oldest[0], oldest);
      mdbx_assert(env, env->me_oldest[0] <= oldest);
      return env->me_oldest[0] = oldest;
    }

    MDBX_tid_t tid;
    MDBX_pid_t pid;
    int rc;

    if (!env->me_callback_rbr)
      break;

    pid = asleep->mr_pid;
    tid = asleep->mr_tid;
    if (asleep->mr_txnid != laggard || pid <= 0)
      continue;

    const txnid_t gap = meta_txnid_stable(env, meta_head(env)) - laggard;
    rc = env->me_callback_rbr(env, pid, tid, laggard, (gap < UINT_MAX) ? (unsigned)gap : UINT_MAX, retry);
    if (rc <= MDBX_RBR_UNABLE)
      break;

    if (rc >= MDBX_RBR_EVICTED) {
      asleep->mr_txnid = ~(txnid_t)0;
      env->me_lck->li_readers_refresh_flag = true;
      if (rc >= MDBX_RBR_KILLED) {
        asleep->mr_tid = 0;
        asleep->mr_pid = 0;
        mdbx_coherent_barrier();
      }
    } else {
      assert(rc == MDBX_RBR_RETRY);
    }
  }

  if (retry && env->me_callback_rbr) {
    /* LY: notify end of RBR-loop */
    env->me_callback_rbr(env, 0, 0, laggard, 0, -retry);
  }
  return find_oldest(env->me_current_txn);
}

//----------------------------------------------------------------------------

#ifdef __SANITIZE_ADDRESS__
LIBMDBX_API __attribute__((weak)) const char *__asan_default_options() {
  return "symbolize=1:allow_addr2line=1:"
#ifdef _DEBUG
         "debug=1:"
#endif /* _DEBUG */
         "report_globals=1:"
         "replace_str=1:replace_intrin=1:"
         "malloc_context_size=9:"
         "detect_leaks=1:"
         "check_printf=1:"
         "detect_deadlocks=1:"
#ifndef LTO_ENABLED
         "check_initialization_order=1:"
#endif
         "detect_stack_use_after_return=1:"
         "intercept_tls_get_addr=1:"
         "decorate_proc_maps=1:"
         "abort_on_error=1";
}
#endif /* __SANITIZE_ADDRESS__ */
