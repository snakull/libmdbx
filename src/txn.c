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

#define txn_extra(fmt, ...) log_extra(MDBX_LOG_TXN, fmt, ##__VA_ARGS__)
#define txn_trace(fmt, ...) log_trace(MDBX_LOG_TXN, fmt, ##__VA_ARGS__)
#define txn_verbose(fmt, ...) log_verbose(MDBX_LOG_TXN, fmt, ##__VA_ARGS__)
#define txn_info(fmt, ...) log_info(MDBX_LOG_TXN, fmt, ##__VA_ARGS__)
#define txn_notice(fmt, ...) log_notice(MDBX_LOG_TXN, fmt, ##__VA_ARGS__)
#define txn_warning(fmt, ...) log_warning(MDBX_LOG_TXN, fmt, ##__VA_ARGS__)
#define txn_error(fmt, ...) log_error(MDBX_LOG_TXN, fmt, ##__VA_ARGS__)
#define txn_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_TXN, __func__, __LINE__, "%s, error %d", msg, err)

/*----------------------------------------------------------------------------*/

static int validate_txn_only(MDBX_txn_t *txn) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(txn->mt_owner != mdbx_thread_self()))
    return MDBX_THREAD_MISMATCH;

  if (unlikely(txn->mt_flags & MDBX_TXN_BLOCKED))
    return MDBX_BAD_TXN;

  return MDBX_SUCCESS;
}

static inline int validate_txn_ro(MDBX_txn_t *txn) {
  int err = validate_txn_only(txn);
  if (likely(err == MDBX_SUCCESS))
    err = validate_env(txn->mt_env, false);
  return err;
}

static inline int validate_txn_more4rw(MDBX_txn_t *txn) {
  if (unlikely(txn->mt_flags & MDBX_RDONLY))
    return MDBX_EACCESS;

  return MDBX_SUCCESS;
}

static inline int validate_txn_rw(MDBX_txn_t *txn) {
  int err = validate_txn_ro(txn);
  if (likely(err == MDBX_SUCCESS))
    err = validate_txn_more4rw(txn);

  return err;
}

//-----------------------------------------------------------------------------

static inline void txn_set_txnid(MDBX_txn_t *txn, txnid_t id) { *(txnid_t *)(&txn->mt_txnid) = id; }

static inline bool txn_stable4read(MDBX_txn_t *txn, meta_t *const meta) {
  MDBX_env_t *env = txn->mt_env;
  mdbx_compiler_barrier();
  if (unlikely(meta != meta_head(env)))
    return false;
  if (unlikely(txn->mt_txnid != meta_txnid_fluid(env, meta)))
    return false;
  if (unlikely(txn->mt_txnid < env->me_oldest[0]))
    return false;

  jitter4testing(false);
  return true;
}

/* Common code for mdbx_begin() and mdbx_txn_renew(). */
static int txn_renew(MDBX_txn_t *txn, unsigned flags) {
  txn_trace(">> (txn = %p, flags = 0x%x)", txn, flags);
  MDBX_env_t *env = txn->mt_env;
  int rc;

  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags32 |= MDBX_ENV_TAINTED;
    txn_trace("<< (txn = %p, flags = 0x%x): wrong-pid", txn, flags);
    return MDBX_PANIC;
  }

  pgno_t upper_pgno = 0;
  if (flags & MDBX_RDONLY) {
    txn->mt_flags = MDBX_RDONLY;
    MDBX_reader_t *r = txn->mt_ro_reader;
    if (likely(env->me_flags32 & MDBX_ENV_TXKEY)) {
      mdbx_assert(env, !(env->me_flags32 & MDBX_NOTLS));
      r = tls_get(env->me_txkey);
      if (likely(r)) {
        mdbx_assert(env, r->mr_pid == env->me_pid);
        mdbx_assert(env, r->mr_tid == mdbx_thread_self());
      }
    } else {
      mdbx_assert(env, !env->me_lck || (env->me_flags32 & MDBX_NOTLS));
    }

    if (likely(r)) {
      if (unlikely(r->mr_pid != env->me_pid || r->mr_txnid != ~(txnid_t)0)) {
        txn_trace("<< (txn = %p, flags = 0x%x): MDBX_BAD_RSLOT", txn, flags);
        return MDBX_BAD_RSLOT;
      }
    } else if (env->me_lck) {
      unsigned slot, nreaders;
      const MDBX_tid_t tid = mdbx_thread_self();
      mdbx_assert(env, env->me_lck->li_magic_and_version == MDBX_LOCK_MAGIC);
      mdbx_assert(env, env->me_lck->li_os_and_format == MDBX_LOCK_FORMAT);

      if (likely((env->me_flags32 & MDBX_EXCLUSIVE) == 0)) {
        rc = env->ops.locking.ops_reader_registration_lock(env, (env->me_flags32 | flags) & MDBX_NONBLOCK);
        if (unlikely(MDBX_IS_ERROR(rc))) {
          txn_trace("<< (txn = %p, flags = 0x%x): rc = %d", txn, flags, rc);
          return rc;
        }
      }
      rc = MDBX_SUCCESS;

      if (unlikely(env->me_live_reader != env->me_pid)) {
        rc = env->ops.locking.ops_reader_alive_set(env, env->me_pid);
        if (unlikely(rc != MDBX_SUCCESS)) {
          if (likely((env->me_flags32 & MDBX_EXCLUSIVE) == 0))
            env->ops.locking.ops_reader_registration_unlock(env);
          txn_trace("<< (txn = %p, flags = 0x%x): rc = %d", txn, flags, rc);
          return rc;
        }
        env->me_live_reader = env->me_pid;
      }

      while (1) {
        nreaders = env->me_lck->li_numreaders;
        for (slot = 0; slot < nreaders; slot++)
          if (env->me_lck->li_readers[slot].mr_pid == 0)
            break;

        if (likely(slot < env->me_maxreaders))
          break;

        rc = check_registered_readers(env, true).err;
        if (rc != MDBX_SIGN) {
          if (likely((env->me_flags32 & MDBX_EXCLUSIVE) == 0))
            env->ops.locking.ops_reader_registration_unlock(env);
          rc = (rc == MDBX_SUCCESS) ? MDBX_READERS_FULL : rc;
          txn_trace("<< (txn = %p, flags = 0x%x): rc = %d", txn, flags, rc);
          return rc;
        }
      }

      STATIC_ASSERT(sizeof(MDBX_reader_t) == MDBX_CACHELINE_SIZE);
#ifdef MDBX_OSAL_LOCK
      STATIC_ASSERT(offsetof(MDBX_lockinfo_t, li_wmutex) % MDBX_CACHELINE_SIZE == 0);
#else
      STATIC_ASSERT(offsetof(MDBX_lockinfo_t, li_oldest) % MDBX_CACHELINE_SIZE == 0);
#endif
#ifdef MDBX_OSAL_LOCK
      STATIC_ASSERT(offsetof(MDBX_lockinfo_t, li_rmutex) % MDBX_CACHELINE_SIZE == 0);
#else
      STATIC_ASSERT(offsetof(MDBX_lockinfo_t, li_numreaders) % MDBX_CACHELINE_SIZE == 0);
#endif
      STATIC_ASSERT(offsetof(MDBX_lockinfo_t, li_readers) % MDBX_CACHELINE_SIZE == 0);
      r = &env->me_lck->li_readers[slot];
      /* Claim the reader slot, carefully since other code
       * uses the reader table un-mutexed: First reset the
       * slot, next publish it in mtb.li_numreaders.  After
       * that, it is safe for mdbx_bk_close() to touch it.
       * When it will be closed, we can finally claim it. */
      r->mr_pid = 0;
      r->mr_txnid = ~(txnid_t)0;
      r->mr_tid = tid;
      mdbx_coherent_barrier();
      if (slot == nreaders)
        env->me_lck->li_numreaders = ++nreaders;
      if (env->me_close_readers < nreaders)
        env->me_close_readers = nreaders;
      r->mr_pid = env->me_pid;
      if (likely((env->me_flags32 & MDBX_EXCLUSIVE) == 0))
        env->ops.locking.ops_reader_registration_unlock(env);

      if (likely(env->me_flags32 & MDBX_ENV_TXKEY))
        tls_set(env->me_txkey, r);
    }

    while (1) {
      meta_t *const meta = meta_head(env);
      jitter4testing(false);
      const txnid_t snap = meta_txnid_fluid(env, meta);
      jitter4testing(false);
      if (r) {
        r->mr_txnid = snap;
        jitter4testing(false);
        mdbx_assert(env, r->mr_pid == mdbx_getpid());
        mdbx_assert(env, r->mr_tid == mdbx_thread_self());
        mdbx_assert(env, r->mr_txnid == snap);
        mdbx_coherent_barrier();
        env->me_lck->li_readers_refresh_flag = true;
      }
      jitter4testing(true);

      /* Snap the state from current meta-head */
      txn_set_txnid(txn, snap);
      txn->mt_next_pgno = meta->mm_geo.next;
      txn->mt_end_pgno = meta->mm_geo.now;
      upper_pgno = meta->mm_geo.upper;
      rc = aa_db2txn(env, &meta->mm_aas[MDBX_GACO_AAH], &txn->txn_aht_array[MDBX_GACO_AAH], af_gaco);
      if (likely(rc == MDBX_SUCCESS))
        rc = aa_db2txn(env, &meta->mm_aas[MDBX_MAIN_AAH], &txn->txn_aht_array[MDBX_MAIN_AAH], af_main);
      txn->mt_canary = meta->mm_canary;
      if (likely(txn_stable4read(txn, meta)))
        break;
    }

    if (unlikely(rc != MDBX_SUCCESS)) {
      txn_trace("<< (txn = %p, flags = 0x%x): fetch4stable, rc = %d", txn, flags, rc);
      goto bailout;
    }
    if (unlikely(txn->mt_txnid == 0)) {
      mdbx_error("environment corrupted by died writer, must shutdown!");
      rc = MDBX_WANNA_RECOVERY;
      goto bailout;
    }
    mdbx_assert(env, txn->mt_txnid >= *env->me_oldest);
    txn->mt_ro_reader = r;
  } else {
    /* Not yet touching txn == env->me_txn0, it may be active */
    if (likely((env->me_flags32 & MDBX_EXCLUSIVE) == 0)) {
      jitter4testing(false);
      rc = env->ops.locking.ops_writer_lock(env, (env->me_flags32 | flags) & MDBX_NONBLOCK);
      if (unlikely(rc != MDBX_SUCCESS)) {
        txn_trace("<< (txn = %p, flags = 0x%x): "
                  "env->ops.locking.ops_writer_lock(), rc = %d",
                  txn, flags, rc);
        return rc;
      }
    }

    jitter4testing(false);
    meta_t *meta = meta_head(env);
    jitter4testing(false);
    txn->mt_canary = meta->mm_canary;
    const txnid_t snap = meta_txnid_stable(env, meta);
    txn_set_txnid(txn, snap + 1);
    if (unlikely(txn->mt_txnid < snap)) {
      mdbx_debug("txnid overflow!");
      rc = MDBX_TXN_FULL;
      goto bailout;
    }

    txn->mt_flags = flags;
    txn->mt_child = nullptr;
    txn->mt_loose_pages = nullptr;
    txn->mt_loose_count = 0;
    txn->mt_dirtyroom = MDBX_PNL_UM_MAX;
    txn->mt_rw_dirtylist = env->me_dirtylist;
    txn->mt_rw_dirtylist[0].mid = 0;
    txn->mt_befree_pages = env->me_free_pgs;
    txn->mt_befree_pages[0] = 0;
    txn->mt_spill_pages = nullptr;
    if (txn->mt_lifo_reclaimed)
      txn->mt_lifo_reclaimed[0] = 0;
    env->me_current_txn = txn;

    /* Snap core AA records */
    rc = aa_db2txn(env, &meta->mm_aas[MDBX_GACO_AAH], &txn->txn_aht_array[MDBX_GACO_AAH], af_gaco);
    if (unlikely(rc != MDBX_SUCCESS)) {
      txn_trace("<< (txn = %p, flags = 0x%x): aa_db2txn(MDBX_GACO_AAH), rc = %d", txn, flags, rc);
      goto bailout;
    }
    rc = aa_db2txn(env, &meta->mm_aas[MDBX_MAIN_AAH], &txn->txn_aht_array[MDBX_MAIN_AAH], af_main);
    if (unlikely(rc != MDBX_SUCCESS)) {
      txn_trace("<< (txn = %p, flags = 0x%x): aa_db2txn(MDBX_MAIN_AAH), rc = %d", txn, flags, rc);
      goto bailout;
    }
    txn->mt_next_pgno = meta->mm_geo.next;
    txn->mt_end_pgno = meta->mm_geo.now;
    upper_pgno = meta->mm_geo.upper;
  }

  if (unlikely(env->me_flags32 & MDBX_ENV_TAINTED)) {
    txn_warning("databook had fatal error, must shutdown!");
    rc = MDBX_PANIC;
  bailout:
    assert(rc != MDBX_SUCCESS);
    txn_end(txn, MDBX_END_SLOT | MDBX_END_FAIL_BEGIN);
    return rc;
  }

  /* setup basic AA-handles */
  txn->txn_aht_array[MDBX_GACO_AAH].ahe = &env->env_ahe_array[MDBX_GACO_AAH];
  txn->txn_aht_array[MDBX_GACO_AAH].ah.seq16 = env->env_ahe_array[MDBX_GACO_AAH].ax_seqaah16;
  txn->txn_aht_array[MDBX_GACO_AAH].ah.kind_and_state16 = MDBX_AAH_VALID | MDBX_AAH_GACO;
  txn->txn_aht_array[MDBX_MAIN_AAH].ahe = &env->env_ahe_array[MDBX_MAIN_AAH];
  txn->txn_aht_array[MDBX_MAIN_AAH].ah.seq16 = env->env_ahe_array[MDBX_MAIN_AAH].ax_seqaah16;
  txn->txn_aht_array[MDBX_MAIN_AAH].ah.kind_and_state16 = MDBX_AAH_VALID | MDBX_AAH_MAIN;
  txn->txn_ah_num = CORE_AAH;

  assert(aht_valid(aht_gaco(txn)));
  assert(aht_valid(aht_main(txn)));

  const size_t size = pgno2bytes(env, txn->mt_end_pgno);
  if (unlikely(size > env->me_mapsize)) {
    if (upper_pgno > MAX_PAGENO || bytes2pgno(env, pgno2bytes(env, upper_pgno)) != upper_pgno) {
      txn_trace("<< (txn = %p, flags = 0x%x): MDBX_MAP_RESIZED", txn, flags);
      rc = MDBX_MAP_RESIZED;
      goto bailout;
    }
    rc = mdbx_mapresize(env, txn->mt_end_pgno, upper_pgno);
    if (rc != MDBX_SUCCESS) {
      txn_trace("<< (txn = %p, flags = 0x%x): mdbx_mapresize(), rc = %d", txn, flags, rc);
      goto bailout;
    }
  }
  return MDBX_SUCCESS;
}

MDBX_error_t mdbx_txn_renew(MDBX_txn_t *txn) {
  int rc;

  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(txn->mt_owner != mdbx_thread_self()))
    return MDBX_THREAD_MISMATCH;

  if (unlikely((txn->mt_flags & MDBX_RDONLY) == 0))
    return MDBX_EINVAL;

  rc = txn_renew(txn, MDBX_RDONLY);
  if (rc == MDBX_SUCCESS) {
    mdbx_debug("renew txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, txn->mt_txnid,
               (txn->mt_flags & MDBX_RDONLY) ? 'r' : 'w', (void *)txn, (void *)txn->mt_env,
               aht_main(txn)->aa.root, aht_gaco(txn)->aa.root);
  }
  return rc;
}

MDBX_txn_result_t mdbx_begin(MDBX_env_t *env, MDBX_txn_t *parent, MDBX_flags_t flags) {
  MDBX_txn_result_t result;
  result.txn = nullptr;
  result.err = validate_env(env, true);
  if (unlikely(result.err != MDBX_SUCCESS))
    return result;

  if (unlikely(flags & ~MDBX_TXN_BEGIN_FLAGS)) {
    result.err = MDBX_EINVAL;
    return result;
  }

  if (unlikely(!env->me_map)) {
    result.err = MDBX_EPERM;
    return result;
  }

  flags |= env->me_flags32 & MDBX_WRITEMAP;
  if (unlikely(env->me_flags32 & MDBX_RDONLY & ~flags)) {
    /* write txn in RDONLY env */
    result.err = MDBX_EACCESS;
    return result;
  }

  size_t txn_extra_size, txn_body_size;
  if (parent) {
    /* Nested transactions: Max 1 child, write txns only, no writemap */
    if (unlikely(flags & (MDBX_RDONLY | MDBX_WRITEMAP))) {
      /* write txns only, no writemap */
      result.err = MDBX_EINVAL;
      return result;
    }

    result.err = validate_txn_only(parent);
    if (unlikely(result.err != MDBX_SUCCESS))
      return result;

    assert(parent->mt_child == nullptr);
    flags |= parent->mt_flags;
    txn_body_size = mdbx_roundup_ptrsize(sizeof(nested_txn_t));
    /* Child txns save pgstate_t and use own copy of cursors */
    txn_extra_size = mdbx_roundup_ptrsize(env->env_ah_max * (sizeof(aht_t) + sizeof(MDBX_cursor_t *)));
  } else if (flags & MDBX_RDONLY) {
    if (env->me_wpa_txn && unlikely(env->me_wpa_txn->mt_owner == mdbx_thread_self())) {
      result.err = MDBX_EBUSY;
      return result;
    }
    txn_body_size = mdbx_roundup_ptrsize(sizeof(MDBX_txn_t));
    txn_extra_size = mdbx_roundup_ptrsize(env->env_ah_max * sizeof(aht_t));
  } else {
    /* Reuse preallocated write txn. However, do not touch it until
     * txn_renew() succeeds, since it currently may be active. */
    result.txn = env->me_wpa_txn;
    if (unlikely(result.txn->mt_owner == mdbx_thread_self())) {
      result.err = MDBX_EBUSY;
      return result;
    }
    goto renew;
  }

  result.txn = malloc(txn_body_size + txn_extra_size);
  if (unlikely(result.txn == nullptr)) {
    mdbx_debug("calloc: %s", "failed");
    result.err = MDBX_ENOMEM;
    return result;
  }
  memset(result.txn, 0, txn_body_size);
  result.txn->txn_aht_array = (aht_t *)((char *)result.txn + txn_body_size);
  result.txn->mt_flags = flags;
  *(MDBX_env_t **)&result.txn->mt_env = env;

  if (parent) {
    result.txn->mt_cursors = (MDBX_cursor_t **)(result.txn->txn_aht_array + env->env_ah_max);
    result.txn->mt_rw_dirtylist = (MDBX_ID2 *)malloc(sizeof(MDBX_ID2) * MDBX_PNL_UM_SIZE);
    result.txn->mt_befree_pages = mdbx_pnl_alloc(MDBX_PNL_UM_MAX);
    if (unlikely(!result.txn->mt_rw_dirtylist || !result.txn->mt_befree_pages)) {
      free(result.txn->mt_rw_dirtylist);
      free(result.txn);
      result.txn = nullptr;
      result.err = MDBX_ENOMEM;
      return result;
    }

    txn_set_txnid(result.txn, parent->mt_txnid);
    result.txn->mt_dirtyroom = parent->mt_dirtyroom;
    result.txn->mt_rw_dirtylist[0].mid = 0;
    result.txn->mt_spill_pages = nullptr;
    result.txn->mt_next_pgno = parent->mt_next_pgno;
    result.txn->mt_end_pgno = parent->mt_end_pgno;
    parent->mt_flags |= MDBX_TXN_HAS_CHILD;
    parent->mt_child = result.txn;
    result.txn->mt_parent = parent;
    result.txn->txn_ah_num = parent->txn_ah_num;
    memcpy(result.txn->txn_aht_array, parent->txn_aht_array, result.txn->txn_ah_num * sizeof(aht_t));
    /* Copy parent's mt_aah_flags, but clear MDBX_AAH_FRESH */
    for (unsigned i = 0; i < result.txn->txn_ah_num; ++i)
      result.txn->txn_aht_array[i].ah.state8 &= ~MDBX_AAH_CREATED;

    nested_txn_t *ntxn = (nested_txn_t *)result.txn;
    ntxn->mnt_pgstate = env->me_pgstate /* save parent reclaimed state */;
    result.err = MDBX_SUCCESS;
    if (env->me_reclaimed_pglist) {
      MDBX_PNL list_copy = mdbx_pnl_alloc(env->me_reclaimed_pglist[0]);
      if (unlikely(!list_copy))
        result.err = MDBX_ENOMEM;
      else {
        size_t bytes = MDBX_PNL_SIZEOF(env->me_reclaimed_pglist);
        env->me_reclaimed_pglist = memcpy(list_copy, ntxn->mnt_pgstate.mf_reclaimed_pglist, bytes);
      }
    }
    if (likely(result.err == MDBX_SUCCESS))
      result.err = txn_shadow_cursors(parent, result.txn);
    if (unlikely(result.err != MDBX_SUCCESS))
      txn_end(result.txn, MDBX_END_FAIL_BEGINCHILD);
  } else { /* MDBX_RDONLY */
  renew:
    result.err = txn_renew(result.txn, flags);
  }

  if (unlikely(result.err != MDBX_SUCCESS)) {
    if (result.txn != env->me_wpa_txn)
      free(result.txn);
    result.txn = nullptr;
    return result;
  }

  result.txn->mt_owner = mdbx_thread_self();
  set_signature(&result.txn->mt_signature, MDBX_MT_SIGNATURE);
  mdbx_debug("begin txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, result.txn->mt_txnid,
             (flags & MDBX_RDONLY) ? 'r' : 'w', (void *)result.txn, (void *)env, aht_main(result.txn)->aa.root,
             aht_gaco(result.txn)->aa.root);
  return result;
}

/* End a transaction, except successful commit of a nested transaction.
 * May be called twice for readonly txns: First reset it, then abort.
 * [in] txn   the transaction handle to end
 * [in] mode  why and how to end the transaction */
static int txn_end(MDBX_txn_t *txn, const unsigned mode) {
  MDBX_env_t *env = txn->mt_env;
  static const char *const names[] = MDBX_END_NAMES;

  if (unlikely(txn->mt_env->me_pid != mdbx_getpid())) {
    env->me_flags32 |= MDBX_ENV_TAINTED;
    return MDBX_PANIC;
  }

  /* Export or close AAH handles opened in this txn */
  int rc = aa_return(txn, mode);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  mdbx_debug("%s txn %" PRIaTXN "%c %p on mdbenv %p, root page %" PRIaPGNO "/%" PRIaPGNO,
             names[mode & MDBX_END_OPMASK], txn->mt_txnid, (txn->mt_flags & MDBX_RDONLY) ? 'r' : 'w',
             (void *)txn, (void *)env, aht_main(txn)->aa.root, aht_gaco(txn)->aa.root);

  if (txn->mt_flags & MDBX_RDONLY) {
    if (txn->mt_ro_reader) {
      txn->mt_ro_reader->mr_txnid = ~(txnid_t)0;
      env->me_lck->li_readers_refresh_flag = true;
      if (mode & MDBX_END_SLOT) {
        if ((env->me_flags32 & MDBX_ENV_TXKEY) == 0)
          txn->mt_ro_reader->mr_pid = 0;
        txn->mt_ro_reader = nullptr;
      }
    }
    mdbx_coherent_barrier();
    txn->txn_ah_num = 0; /* prevent further AAH activity */
    txn->mt_flags = MDBX_RDONLY | MDBX_TXN_FINISHED;
    txn->mt_owner = 0;
  } else if ((txn->mt_flags & MDBX_TXN_FINISHED) == 0) {
    pgno_t *pghead = env->me_reclaimed_pglist;

    if (!(mode & MDBX_END_EOTDONE)) /* !(already closed cursors) */
      cursors_eot(txn, 0);
    if (!(env->me_flags32 & MDBX_WRITEMAP)) {
      dlist_free(txn);
    }

    if (txn->mt_lifo_reclaimed) {
      txn->mt_lifo_reclaimed[0] = 0;
      if (txn != env->me_wpa_txn) {
        mdbx_txl_free(txn->mt_lifo_reclaimed);
        txn->mt_lifo_reclaimed = nullptr;
      }
    }
    txn->txn_ah_num = 0;
    txn->mt_flags = MDBX_TXN_FINISHED;
    txn->mt_owner = 0;

    if (!txn->mt_parent) {
      mdbx_pnl_shrink(&txn->mt_befree_pages);
      env->me_free_pgs = txn->mt_befree_pages;
      /* me_pgstate: */
      env->me_reclaimed_pglist = nullptr;
      env->me_last_reclaimed = 0;
      env->me_current_txn = nullptr;
      if (likely((env->me_flags32 & MDBX_EXCLUSIVE) == 0)) {
        /* The writer mutex was locked in mdbx_begin. */
        env->ops.locking.ops_writer_unlock(env);
      }
    } else {
      txn->mt_parent->mt_child = nullptr;
      txn->mt_parent->mt_flags &= ~MDBX_TXN_HAS_CHILD;
      env->me_pgstate = ((nested_txn_t *)txn)->mnt_pgstate;
      mdbx_pnl_free(txn->mt_befree_pages);
      mdbx_pnl_free(txn->mt_spill_pages);
      free(txn->mt_rw_dirtylist);
    }

    mdbx_pnl_free(pghead);
  }

  mdbx_assert(env, txn == env->me_wpa_txn || txn->mt_owner == 0);
  if ((mode & MDBX_END_FREE) && txn != env->me_wpa_txn) {
    set_signature(&txn->mt_signature, ~0u);
    free(txn);
  }

  return MDBX_SUCCESS;
}

MDBX_error_t mdbx_commit(MDBX_txn_t *txn) {
  int rc;

  if (unlikely(txn == nullptr))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(txn->mt_owner != mdbx_thread_self()))
    return MDBX_THREAD_MISMATCH;

  MDBX_env_t *env = txn->mt_env;
  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags32 |= MDBX_ENV_TAINTED;
    return MDBX_PANIC;
  }

  if (txn->mt_child) {
    rc = mdbx_commit(txn->mt_child);
    txn->mt_child = nullptr;
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
  }

  /* txn_end() mode for a commit which writes nothing */
  unsigned end_mode = MDBX_END_EMPTY_COMMIT | MDBX_END_SLOT | MDBX_END_FREE;
  if (unlikely(F_ISSET(txn->mt_flags, MDBX_RDONLY)))
    goto done;

  if (unlikely(txn->mt_flags & (MDBX_TXN_FINISHED | MDBX_TXN_ERROR))) {
    mdbx_debug("error flag is set, can't commit");
    if (txn->mt_parent)
      txn->mt_parent->mt_flags |= MDBX_TXN_ERROR;
    rc = MDBX_BAD_TXN;
    goto fail;
  }

  if (txn->mt_parent) {
    MDBX_txn_t *parent = txn->mt_parent;
    page_t **lp;
    MDBX_ID2L dst, src;
    unsigned i, x, y, len;

    /* Append our reclaim list to parent's */
    if (txn->mt_lifo_reclaimed) {
      if (parent->mt_lifo_reclaimed) {
        rc = mdbx_txl_append_list(&parent->mt_lifo_reclaimed, txn->mt_lifo_reclaimed);
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
        mdbx_txl_free(txn->mt_lifo_reclaimed);
      } else
        parent->mt_lifo_reclaimed = txn->mt_lifo_reclaimed;
      txn->mt_lifo_reclaimed = nullptr;
    }

    /* Append our free list to parent's */
    rc = mdbx_pnl_append_list(&parent->mt_befree_pages, txn->mt_befree_pages);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    mdbx_pnl_free(txn->mt_befree_pages);
    /* Failures after this must either undo the changes
     * to the parent or set MDBX_TXN_ERROR in the parent. */

    parent->mt_next_pgno = txn->mt_next_pgno;
    parent->mt_end_pgno = txn->mt_end_pgno;
    parent->mt_flags = txn->mt_flags;

    /* Merge our cursors into parent's and close them */
    cursors_eot(txn, 1);

    /* Update parent's AA table. */
    memcpy(parent->txn_aht_array, txn->txn_aht_array, txn->txn_ah_num * sizeof(aht_t));
    parent->txn_ah_num = txn->txn_ah_num;

    aht_t *const end = &parent->txn_aht_array[parent->txn_ah_num];
    const ptrdiff_t parent2child = txn->txn_aht_array - parent->txn_aht_array;
    for (aht_t *aht = &parent->txn_aht_array[CORE_AAH]; aht < end; ++aht) {
      /* preserve parent's MDBX_AAH_CREATED status */
      aht->ah.state8 = (aht->ah.state8 & MDBX_AAH_CREATED) | aht[parent2child].ah.state8;
    }
    rc = aa_return(txn, end_mode);

    dst = parent->mt_rw_dirtylist;
    src = txn->mt_rw_dirtylist;
    /* Remove anything in our dirty list from parent's spill list */
    if (parent->mt_spill_pages && parent->mt_spill_pages[0]) {
      MDBX_PNL const pspill = parent->mt_spill_pages;
      const unsigned ps_len = pspill[0];
      x = y = ps_len;
      pspill[0] = (pgno_t)-1;
      /* Mark our dirty pages as deleted in parent spill list */
      for (i = 0, len = src[0].mid; ++i <= len;) {
        const pgno_t pgno2 = src[i].mid << 1;
        while (pgno2 > pspill[x])
          x--;
        if (pgno2 == pspill[x]) {
          pspill[x] = 1;
          y = --x;
        }
      }
      /* Squash deleted pagenums if we deleted any */
      for (x = y; ++x <= ps_len;)
        if (!(pspill[x] & 1))
          pspill[++y] = pspill[x];
      pspill[0] = y;
    }

    /* Remove anything in our spill list from parent's dirty list */
    if (txn->mt_spill_pages && txn->mt_spill_pages[0]) {
      for (i = 1; i <= txn->mt_spill_pages[0]; i++) {
        const pgno_t pgno2 = txn->mt_spill_pages[i];
        if (pgno2 & 1)
          continue; /* deleted spillpg */
        pgno_t pgno = pgno2 >> 1;
        y = mdbx_mid2l_search(dst, pgno);
        if (y <= dst[0].mid && dst[y].mid == pgno) {
          free(dst[y].mptr);
          while (y < dst[0].mid) {
            dst[y] = dst[y + 1];
            y++;
          }
          dst[0].mid--;
        }
      }
    }

    /* Find len = length of merging our dirty list with parent's */
    x = dst[0].mid;
    dst[0].mid = 0; /* simplify loops */
    if (parent->mt_parent) {
      len = x + src[0].mid;
      y = mdbx_mid2l_search(src, dst[x].mid + 1) - 1;
      for (i = x; y && i; y--) {
        pgno_t yp = src[y].mid;
        while (yp < dst[i].mid)
          i--;
        if (yp == dst[i].mid) {
          i--;
          len--;
        }
      }
    } else { /* Simplify the above for single-ancestor case */
      len = MDBX_PNL_UM_MAX - txn->mt_dirtyroom;
    }
    /* Merge our dirty list with parent's */
    y = src[0].mid;
    for (i = len; y; dst[i--] = src[y--]) {
      pgno_t yp = src[y].mid;
      while (yp < dst[x].mid)
        dst[i--] = dst[x--];
      if (yp == dst[x].mid)
        free(dst[x--].mptr);
    }
    assert(i == x);
    dst[0].mid = len;
    free(txn->mt_rw_dirtylist);
    parent->mt_dirtyroom = txn->mt_dirtyroom;
    if (txn->mt_spill_pages) {
      if (parent->mt_spill_pages) {
        /* TODO: Prevent failure here, so parent does not fail */
        rc = mdbx_pnl_append_list(&parent->mt_spill_pages, txn->mt_spill_pages);
        if (unlikely(rc != MDBX_SUCCESS))
          parent->mt_flags |= MDBX_TXN_ERROR;
        mdbx_pnl_free(txn->mt_spill_pages);
        mdbx_pnl_sort(parent->mt_spill_pages);
      } else {
        parent->mt_spill_pages = txn->mt_spill_pages;
      }
    }

    /* Append our loose page list to parent's */
    for (lp = &parent->mt_loose_pages; *lp; lp = &NEXT_LOOSE_PAGE(*lp))
      ;
    *lp = txn->mt_loose_pages;
    parent->mt_loose_count += txn->mt_loose_count;

    parent->mt_child = nullptr;
    mdbx_pnl_free(((nested_txn_t *)txn)->mnt_pgstate.mf_reclaimed_pglist);
    set_signature(&txn->mt_signature, ~0u);
    free(txn);
    return rc;
  }

  if (unlikely(txn != env->me_current_txn)) {
    mdbx_debug("attempt to commit unknown transaction");
    rc = MDBX_EINVAL;
    goto fail;
  }

  cursors_eot(txn, 0);
  end_mode |= MDBX_END_EOTDONE | MDBX_END_FIXUPAAH;

  if (!txn->mt_rw_dirtylist[0].mid && !(txn->mt_flags & (MDBX_TXN_DIRTY | MDBX_TXN_SPILLS)))
    goto done;

  mdbx_debug("committing txn %" PRIaTXN " %p on mdbenv %p, root page %" PRIaPGNO "/%" PRIaPGNO, txn->mt_txnid,
             (void *)txn, (void *)env, aht_main(txn)->aa.root, aht_gaco(txn)->aa.root);

  /* Update AA root pointers */
  if (txn->txn_ah_num > CORE_AAH) {
    MDBX_cursor_t mc;
    rc = cursor_init(&mc, txn, aht_main(txn));
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;

    aht_t *const end = &txn->txn_aht_array[txn->txn_ah_num];
    for (aht_t *aht = &txn->txn_aht_array[CORE_AAH]; aht < end; ++aht) {
      if (aht->ah.state8 & MDBX_AAH_DIRTY) {
        ahe_t *ahe = aht->ahe;
        if (unlikely(ahe->ax_seqaah16 != aht->ah.seq16 || ahe->ax_refcounter16 < 1)) {
          rc = MDBX_BAD_AAH;
          goto fail;
        }

        MDBX_iov_t data = {nullptr, sizeof(aatree_t)};
        rc = cursor_put(&mc.primal, &ahe->ax_ident, &data, MDBX_IUD_RESERVE | NODE_SUBTREE);
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
        aa_txn2db(env, aht, (aatree_t *)data.iov_base, af_user);
      }
    }
  }

  rc = freelist_save(txn);
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;

  mdbx_pnl_free(env->me_reclaimed_pglist);
  env->me_reclaimed_pglist = nullptr;
  mdbx_pnl_shrink(&txn->mt_befree_pages);

  if (MDBX_AUDIT_ENABLED())
    audit(txn);

  rc = page_flush(txn, 0);
  if (likely(rc == MDBX_SUCCESS)) {
    meta_t meta, *head = meta_head(env);

    meta.mm_magic_and_version = head->mm_magic_and_version;
    meta.mm_reserved32 = head->mm_reserved32;

    meta.mm_geo = head->mm_geo;
    meta.mm_geo.next = txn->mt_next_pgno;
    meta.mm_geo.now = txn->mt_end_pgno;
    aa_txn2db(env, &txn->txn_aht_array[MDBX_GACO_AAH], &meta.mm_aas[MDBX_GACO_AAH], af_gaco);
    aa_txn2db(env, &txn->txn_aht_array[MDBX_MAIN_AAH], &meta.mm_aas[MDBX_MAIN_AAH], af_main);
    meta.mm_canary = txn->mt_canary;
    meta_set_txnid(env, &meta, txn->mt_txnid);

    rc = mdbx_sync_locked(env, env->me_flags32 | txn->mt_flags | MDBX_SHRINK_ALLOWED, &meta);
  }
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;
  env->me_lck->li_readers_refresh_flag = false;
  end_mode = MDBX_END_COMMITTED | MDBX_END_FIXUPAAH | MDBX_END_EOTDONE;

done:
  return txn_end(txn, end_mode);

fail:
  mdbx_abort(txn);
  return rc;
}
