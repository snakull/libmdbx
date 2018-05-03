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

#define aah_extra(fmt, ...) log_extra(MDBX_LOG_AAH, fmt, ##__VA_ARGS__)
#define aah_trace(fmt, ...) log_trace(MDBX_LOG_AAH, fmt, ##__VA_ARGS__)
#define aah_verbose(fmt, ...) log_verbose(MDBX_LOG_AAH, fmt, ##__VA_ARGS__)
#define aah_info(fmt, ...) log_info(MDBX_LOG_AAH, fmt, ##__VA_ARGS__)
#define aah_notice(fmt, ...) log_notice(MDBX_LOG_AAH, fmt, ##__VA_ARGS__)
#define aah_warning(fmt, ...) log_warning(MDBX_LOG_AAH, fmt, ##__VA_ARGS__)
#define aah_error(fmt, ...) log_error(MDBX_LOG_AAH, fmt, ##__VA_ARGS__)
#define aah_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_AAH, __func__, __LINE__, "%s, error %d", msg, err)

static const char mdbx_droppped_name_stub[8] = "";

//-----------------------------------------------------------------------------

static inline ahe_rc_t env_rh(int rc, ahe_t *ahe) {
  ahe_rc_t result = {ahe, rc};
  return result;
}

static inline aht_rc_t txn_rh(int rc, aht_t *aht) {
  aht_rc_t result = {aht, rc};
  return result;
}

static ahe_t *bk_aah2ahe(MDBX_env_t *env, MDBX_aah_t aah) {
  const size_t index = (uint16_t)aah;
  if (unlikely(index >= env->env_ah_num))
    return nullptr;

  ahe_t *ahe = &env->env_ahe_array[index];
  if (unlikely(ahe->ax_refcounter16 < 1 || ahe->ax_aah != aah))
    return nullptr;

  return ahe;
}

static inline MDBX_cursor_t **aa_txn_cursor_tracking_head(MDBX_txn_t *txn, MDBX_aah_t aah) {
  const size_t index = (uint16_t)aah;
  assert(txn->mt_env->env_ah_num > index);
  assert(txn->txn_ah_num > index);
  assert(txn->mt_env->env_ahe_array[index].ax_refcounter16 > 0);
  assert(txn->mt_env->env_ahe_array[index].ax_aah == aah);
  return &txn->mt_cursors[index];
}

static checksum_t aa_checksum(const MDBX_env_t *env, const aatree_t *entry) {
  (void)env;
  (void)entry;
  /* TODO: t1ha */
  return 0;
}

static inline int aa_db2txn(const MDBX_env_t *env, const aatree_t *src, aht_t *aht,
                            const enum aat_format format) {
#if !UNALIGNED_OK || !defined(NDEBUG) || defined(_DEBUG)
  aatree_t aligned;
  if ((uintptr_t)src & 7)
    src = (const aatree_t *)memcpy(&aligned, src, sizeof(aligned));
#endif
  if (unlikely(aa_checksum(env, src) != get_le64_aligned(&src->aa_merkle)))
    return MDBX_CORRUPTED /* checksum mismatch */;

  if (sizeof(pgno_t) == 4) {
    aht->aa.root = get_le32_unaligned(&src->aa_root);
    aht->aa.branch_pages = get_le32_unaligned(&src->aa_branch_pages);
    aht->aa.leaf_pages = get_le32_unaligned(&src->aa_leaf_pages);
    aht->aa.overflow_pages = get_le32_unaligned(&src->aa_overflow_pages);
  } else {
    aht->aa.root = (pgno_t)get_le64_unaligned(&src->aa_root);
    aht->aa.branch_pages = (pgno_t)get_le64_unaligned(&src->aa_branch_pages);
    aht->aa.leaf_pages = (pgno_t)get_le64_unaligned(&src->aa_leaf_pages);
    aht->aa.overflow_pages = (pgno_t)get_le64_unaligned(&src->aa_overflow_pages);
  }
  aht->aa.entries = get_le64_unaligned(&src->aa_entries);
  aht->aa.genseq = get_le64_unaligned(&src->aa_genseq);
  aht->aa.depth16 = get_le16_unaligned(&src->aa_depth16);
  switch (format) {
  case af_gaco:
    aht->aa.flags16 = MDBX_INTEGERKEY /* ignore mm_features16 from [MDBX_GACO_AAH].aa_flags16 */;
    aht->aa.xsize32 = 0 /* ignore mm_psize32 from [MDBX_GACO_AAH].aa_xsize32 */;
    aht->ahe = &env->env_ahe_array[MDBX_GACO_AAH];
    aht->ahe->ax_since = 0;
    aht->aa.creation_txnid = 0;
    aht->aa.creation_time.fixedpoint = 0;
    aht->aa.modification_txnid = get_le64_unaligned(&src->aa_modification_txnid);
    aht->aa.modification_time.fixedpoint = get_le64_unaligned(&src->aa_modification_time.fixedpoint);
    aht->ah.seq16 = env->env_ahe_array[MDBX_GACO_AAH].ax_seqaah16;
    break;
  case af_main:
    aht->aa.flags16 = get_le16_unaligned(&src->aa_flags16);
    aht->aa.xsize32 = get_le32_unaligned(&src->aa_xsize32);
    aht->aa.creation_txnid = get_le64_unaligned(&src->aa_creation_txnid);
    aht->aa.creation_time.fixedpoint = get_le64_unaligned(&src->aa_creation_time.fixedpoint);
    aht->aa.modification_txnid = get_le64_unaligned(&src->aa_modification_txnid);
    aht->aa.modification_time.fixedpoint = get_le64_unaligned(&src->aa_modification_time.fixedpoint);
    aht->ahe = &env->env_ahe_array[MDBX_MAIN_AAH];
    aht->ahe->ax_since = aht->aa.creation_txnid;
    aht->ah.seq16 = env->env_ahe_array[MDBX_MAIN_AAH].ax_seqaah16;
    break;
  default:
    aht->aa.flags16 = get_le16_unaligned(&src->aa_flags16);
    aht->aa.xsize32 = get_le32_unaligned(&src->aa_xsize32);
    aht->aa.creation_txnid = get_le64_unaligned(&src->aa_creation_txnid);
    aht->aa.creation_time.fixedpoint = get_le64_unaligned(&src->aa_creation_time.fixedpoint);
    aht->aa.modification_txnid = get_le64_unaligned(&src->aa_modification_txnid);
    aht->aa.modification_time.fixedpoint = get_le64_unaligned(&src->aa_modification_time.fixedpoint);
    assert(aht->ahe);
    if (likely(aht->ahe)) {
      aht->ahe->ax_since = aht->aa.creation_txnid;
      aht->ah.seq16 = aht->ahe->ax_seqaah16;
    } else {
      aht->ah.seq16 = UINT16_MAX;
    }
  }

  return MDBX_SUCCESS;
}

static inline void aa_txn2db(const MDBX_env_t *env, const aht_t *aht, aatree_t *dst,
                             const enum aat_format format) {
#if !UNALIGNED_OK || !defined(NDEBUG) || defined(_DEBUG)
  aatree_t aligned, *const target = dst;
  if (!is_aligned_uint64(dst))
    dst = &aligned;
#endif
  set_le16_aligned(&dst->aa_depth16, aht->aa.depth16);
  if (likely(format != af_gaco)) {
    set_le16_aligned(&dst->aa_flags16, aht->aa.flags16);
    set_le32_aligned(&dst->aa_xsize32, aht->aa.xsize32);
    set_le64_aligned(&dst->aa_creation_txnid, aht->aa.creation_txnid);
    set_le64_aligned(&dst->aa_creation_time.fixedpoint, aht->aa.creation_time.fixedpoint);
    set_le64_aligned(&dst->aa_modification_txnid, aht->aa.modification_txnid);
    set_le64_aligned(&dst->aa_modification_time.fixedpoint, aht->aa.modification_time.fixedpoint);
  } else {
    set_le16_aligned(
        &dst->aa_flags16,
        0 /* FIXME: copy current features from env */); /* set mm_features16 at [MDBX_GACO_AAH].aa_flags16 */
    set_le32_aligned(&dst->aa_xsize32, env->me_psize) /* set mm_psize32 at [MDBX_GACO_AAH].aa_xsize32 */;
    set_le64_aligned(&dst->aa_modification_txnid, aht->aa.modification_txnid);
    set_le64_aligned(&dst->aa_modification_time.fixedpoint, aht->aa.modification_time.fixedpoint);
  }

  STATIC_ASSERT(sizeof(pgno_t) == 4);
  set_le32_aligned(&dst->aa_root, aht->aa.root);
  set_le32_aligned(&dst->aa_branch_pages, aht->aa.branch_pages);
  set_le32_aligned(&dst->aa_leaf_pages, aht->aa.leaf_pages);
  set_le32_aligned(&dst->aa_overflow_pages, aht->aa.overflow_pages);
  /* } else {
    set_le64_aligned(&dst->aa_root, aht->aa.root);
    set_le64_aligned(&dst->aa_branch_pages, aht->aa.branch_pages);
    set_le64_aligned(&dst->aa_leaf_pages, aht->aa.leaf_pages);
    set_le64_aligned(&dst->aa_overflow_pages, aht->aa.overflow_pages);
  } */
  set_le64_aligned(&dst->aa_entries, aht->aa.entries);
  set_le64_aligned(&dst->aa_genseq, aht->aa.genseq);

  set_le64_aligned(&dst->aa_merkle, aa_checksum(env, dst));
#if !UNALIGNED_OK || !defined(NDEBUG) || defined(_DEBUG)
  if (dst == &aligned)
    memcpy(target, &aligned, sizeof(aligned));
#endif
}

static inline MDBX_aah_t bk_ahe2aah(MDBX_env_t *env, ahe_t *ahe) {
  assert(ahe >= env->env_ahe_array && ahe < &env->env_ahe_array[env->env_ah_num]);
  assert(ahe == bk_aah2ahe(env, ahe->ax_aah));
  (void)env;
  return ahe->ax_aah;
}

static inline ahe_t *ahe_main(MDBX_env_t *env) { return &env->env_ahe_array[MDBX_MAIN_AAH]; }

static inline ahe_t *ahe_gaco(MDBX_env_t *env) { return &env->env_ahe_array[MDBX_GACO_AAH]; }

static inline aht_t *aht_main(MDBX_txn_t *txn) { return &txn->txn_aht_array[MDBX_MAIN_AAH]; }

static inline aht_t *aht_gaco(MDBX_txn_t *txn) { return &txn->txn_aht_array[MDBX_GACO_AAH]; }

static inline aht_t *txn_ahe2aht(MDBX_txn_t *txn, ahe_t *ahe) {
  assert(ahe >= txn->mt_env->env_ahe_array && ahe < &txn->mt_env->env_ahe_array[txn->mt_env->env_ah_max]);
  assert(ahe->ax_ord16 < txn->txn_ah_num);
  return &txn->txn_aht_array[ahe->ax_ord16];
}

static ahe_rc_t __cold aa_lookup(MDBX_env_t *env, MDBX_txn_t *txn, const MDBX_iov_t aa_ident) {
  ahe_t *free;
  ahe_t *const begin = &env->env_ahe_array[CORE_AAH];
  ahe_t *const end = &env->env_ahe_array[env->env_ah_num];
  assert(begin <= end);
  ahe_rc_t rp;

  rp.ahe = free = nullptr;
  for (ahe_t *scan = end; --scan >= begin;) {
    if (scan->ax_refcounter16 < 1) {
      free = scan;
      continue;
    }
    if (!mdbx_iov_eq(&aa_ident, &scan->ax_ident))
      continue;

    if (txn) {
      /* mdbx_aa_open() */
      if (scan->ax_until <= txn->mt_txnid)
        /* aah dropped before current txn (or by current txn) */
        continue;
      if (scan->ax_since > txn->mt_txnid)
        /* aah created after current txn */
        continue;

      rp.ahe = scan;
      break;
    }

    /* mdbx_aa_preopen() */
    if (!rp.ahe) {
      /* first suitable */
      rp.ahe = scan;
      continue;
    }

    /* one more suitable */
    if (rp.ahe->ax_until <= scan->ax_since) {
      /* one was dropped and another was created, switch to the alive aah */
      rp.ahe = scan;
      continue;
    }

    if (rp.ahe->ax_since >= scan->ax_until) {
      /* one was created and another was dropped, keeps the alive aah */
      continue;
    }

    /* overlapping, unable to distinct preferred aah */
    rp.err = MDBX_EALREADY_OPENED;
    return rp;
  }

  if (!rp.ahe) {
    rp.ahe = free;
    if (!rp.ahe) {
      rp.ahe = end;
      if (unlikely(rp.ahe >= &env->env_ahe_array[env->env_ah_max])) {
        rp.err = MDBX_DBS_FULL;
        return rp;
      }
      VALGRIND_MAKE_MEM_UNDEFINED_ERASE(rp.ahe, sizeof(*rp.ahe));
      rp.ahe->ax_ord16 = (uint16_t)env->env_ah_num;
      rp.ahe->ax_refcounter16 = 0;
      env->env_ah_num++;
    }

    assert(rp.ahe->ax_refcounter16 < 1);
  }

  rp.err = MDBX_SUCCESS;
  return rp;
}

static int aa_fetch(MDBX_txn_t *txn, aht_t *aht) {
  assert(aht->ah.state8 == MDBX_AAH_STALE);

  MDBX_cursor_t bundle;
  int rc = cursor_init(&bundle, txn, aht_main(txn));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  ahe_t *env_aah = aht->ahe;
  rc = page_search(&bundle.primal, &env_aah->ax_ident, 0);
  if (unlikely(rc != MDBX_SUCCESS)) {
    aht->ah.state8 = MDBX_AAH_ABSENT;
    return rc;
  }

  node_rc_t rp = node_search(&bundle.primal, env_aah->ax_ident);
  if (unlikely(!rp.exact)) {
    aht->ah.state8 = MDBX_AAH_ABSENT;
    return MDBX_NOTFOUND;
  }

  if (unlikely((rp.node->node_flags8 & (NODE_DUP | NODE_SUBTREE)) != NODE_SUBTREE))
    return MDBX_INCOMPATIBLE /* not a named AA */;

  MDBX_iov_t data;
  rc = node_read(&bundle.primal, rp.node, &data);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(data.iov_len != sizeof(aatree_t)))
    return MDBX_CORRUPTED /* wrong length */;

  rc = aa_db2txn(txn->mt_env, (aatree_t *)data.iov_base, aht, af_user);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(aht->aa.flags16 != env_aah->ax_flags16))
    return MDBX_INCOMPATIBLE /* incompatible flags */;

  assert(env_aah->ax_until > aht->aa.creation_txnid);
  if (env_aah->ax_since < aht->aa.creation_txnid)
    env_aah->ax_since = aht->aa.creation_txnid;
  aht->ah.state8 = MDBX_AAH_VALID;
  return MDBX_SUCCESS;
}

static void aht_bind(aht_t *aht, ahe_t *ahe) {
  aht->ahe = ahe;
  aht->ah.seq16 = ahe->ax_seqaah16;
  aht->ah.kind_and_state16 =
      (aht->ahe->ax_flags16 & MDBX_DUPSORT) ? MDBX_AAH_STALE : MDBX_AAH_STALE | MDBX_AAH_DUPS;
  paranoia_barrier();
}

static void aht_bare4create(aht_t *aht) {
  aht->aa.flags16 = aht->ahe->ax_flags16;
  aht->aa.depth16 = 0;
  aht->aa.xsize32 = 0;
  aht->aa.root = P_INVALID;
  aht->aa.branch_pages = 0;
  aht->aa.leaf_pages = 0;
  aht->aa.overflow_pages = 0;
  aht->aa.entries = 0;
  aht->aa.genseq = 0;
}

static int __cold aa_create(MDBX_txn_t *txn, aht_t *aht) {
  assert(aht->ah.state8 == MDBX_AAH_ABSENT);

  MDBX_cursor_t cursor;
  int rc = cursor_init(&cursor, txn, aht_main(txn));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  aatree_t record;
  aht_bare4create(aht);
  aht->aa.creation_txnid = txn->mt_txnid;
  aa_txn2db(txn->mt_env, aht, &record, af_user);

  MDBX_iov_t data = {&record, sizeof(aatree_t)};
  WITH_CURSOR_TRACKING(cursor, rc = cursor_put(&cursor.primal, &aht->ahe->ax_ident, &data,
                                               NODE_SUBTREE | MDBX_IUD_NOOVERWRITE));

  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  aht->ah.kind_and_state16 = (aht->ahe->ax_flags16 & MDBX_DUPSORT)
                                 ? MDBX_AAH_VALID | MDBX_AAH_CREATED
                                 : MDBX_AAH_VALID | MDBX_AAH_CREATED | MDBX_AAH_DUPS;
  return MDBX_SUCCESS;
}

static aht_t *ahe2aht(MDBX_txn_t *txn, ahe_t *ahe) {
  aht_t *aht = &txn->txn_aht_array[ahe->ax_ord16];
  if (unlikely(txn->txn_ah_num <= ahe->ax_ord16)) {
    aht_t *last = &txn->txn_aht_array[txn->txn_ah_num];
    ptrdiff_t bytes = (char *)aht - (char *)last;
    memset(last, 0, (size_t)bytes + sizeof(aht_t));
    txn->txn_ah_num = ahe->ax_ord16 + 1u;
  }
  return aht;
}

static unsigned aa_state(MDBX_txn_t *txn, ahe_t *ahe) {
  if (unlikely(ahe->ax_ord16 >= txn->txn_ah_num))
    return MDBX_AAH_STALE;

  aht_t *aht = &txn->txn_aht_array[ahe->ax_ord16];
  return (aht->ah.seq16 == ahe->ax_seqaah16) ? aht->ah.kind_and_state16 : MDBX_AAH_BAD;
}

static aht_rc_t aa_take(MDBX_txn_t *txn, MDBX_aah_t aah) {
  ahe_t *ahe = bk_aah2ahe(txn->mt_env, aah);
  if (unlikely(!ahe))
    return txn_rh(MDBX_BAD_AAH, nullptr);

  aht_t *aht = ahe2aht(txn, ahe);
  if (likely(aht->ah.state8 & MDBX_AAH_VALID)) {
    assert(aht->ahe == ahe);
    if (likely(aht->ah.seq16 == ahe->ax_seqaah16))
      return txn_rh(MDBX_SUCCESS, aht);
    aht->ah.state8 = MDBX_AAH_BAD;
  }
  if (likely(aht->ah.state8 & MDBX_AAH_BAD))
    return txn_rh(MDBX_AAH_BAD, nullptr);
  if (likely(aht->ah.state8 & MDBX_AAH_ABSENT))
    return txn_rh(MDBX_NOTFOUND, nullptr);

  int rc = MDBX_BAD_AAH;
  if (likely(txn->mt_txnid >= ahe->ax_since && txn->mt_txnid < ahe->ax_until)) {
    aht_bind(aht, ahe);
    rc = aa_fetch(txn, aht);
    if (unlikely(aht->ah.seq16 != ahe->ax_seqaah16))
      rc = MDBX_BAD_AAH;
  }

  if (likely(rc == MDBX_SUCCESS)) {
    assert(ahe->ax_until > aht->aa.creation_txnid);
    if (ahe->ax_since < aht->aa.creation_txnid)
      ahe->ax_since = aht->aa.creation_txnid;
    assert(aht->ah.state8 == MDBX_AAH_VALID);
    return txn_rh(MDBX_SUCCESS, aht);
  }

  aht->ah.state8 = (rc == MDBX_NOTFOUND) ? MDBX_AAH_ABSENT : MDBX_AAH_BAD;
  return txn_rh(rc, nullptr);
}

static int __cold aa_drop(MDBX_txn_t *txn, mdbx_drop_flags_t flags, aht_t *aht) {
  assert(aht > txn->txn_aht_array);

  MDBX_cursor_t bc;
  int rc = cursor_open(txn, aht, &bc);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = tree_drop(&bc.primal, aht->aa.flags16 & MDBX_DUPSORT);

  /* Invalidate the dropped AA's cursors */
  for (MDBX_cursor_t *scan = *cursor_tracking_head(&bc); scan != nullptr; scan = scan->mc_next)
    scan->primal.mc_state8 &= ~(C_INITIALIZED | C_EOF);

  if (likely(rc == MDBX_SUCCESS)) {
    /* reset the AA record */
    aht_bare4create(aht);
    aht->ah.state8 |= MDBX_AAH_DIRTY;
    if (aht->ahe == ahe_main(txn->mt_env)) {
      txn->mt_flags |= MDBX_TXN_DIRTY;
    } else if (flags & MDBX_DELETE_AA) {
      /* remove the AA record */
      rc = mdbx_del_ex(txn, MDBX_MAIN_AAH, &aht->ahe->ax_ident, nullptr, (mdbx_iud_flags_t)NODE_SUBTREE);
      if (likely(rc == MDBX_SUCCESS)) {
        aht->ah.state8 =
            (aht->ah.state8 & MDBX_AAH_CREATED) ? MDBX_AAH_ABSENT : MDBX_AAH_DROPPED | MDBX_AAH_ABSENT;
      }
    }
  }

  if (unlikely(rc != MDBX_SUCCESS))
    txn->mt_flags |= MDBX_TXN_ERROR;

  cursor_close(&bc);
  return rc;
}

static int aa_return(MDBX_txn_t *txn, unsigned txn_end_flags) {
  int rc = MDBX_SUCCESS;
  bool locked = false;
  aht_t *const end = &txn->txn_aht_array[txn->txn_ah_num];
  for (aht_t *aht = &txn->txn_aht_array[CORE_AAH]; aht < end; ++aht) {
    if (aht->ah.state8 == MDBX_AAH_STALE || aht->ah.state8 == MDBX_AAH_BAD)
      continue;

    ahe_t *ahe = aht->ahe;
    if (unlikely(ahe->ax_seqaah16 != aht->ah.seq16 || ahe->ax_refcounter16 < 1)) {
      rc = MDBX_BAD_AAH;
      continue; /* should release other interim handles */
    }

    if (aht->ah.kind_and_state16 & MDBX_AAH_INTERIM) {
      if (!locked) {
        int err = mdbx_fastmutex_acquire(&txn->mt_env->me_aah_lock, 0);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
        locked = true;
      }

      if (unlikely(ahe->ax_seqaah16 != aht->ah.seq16 || ahe->ax_refcounter16 < 1)) {
        rc = MDBX_BAD_AAH;
        continue; /* should release other interim handles */
      }

      if (--ahe->ax_refcounter16 < 1) {
        ahe->ax_seqaah16++;
        mdbx_iov_free(&ahe->ax_ident);
        continue;
      }
    }

    if (likely(txn_end_flags & MDBX_END_FIXUPAAH)) {
      assert(!(txn->mt_flags & MDBX_RDONLY));
      assert(!txn->mt_parent);
      if (aht->ah.state8 & MDBX_AAH_DROPPED)
        ahe->ax_until = txn->mt_txnid;
      if (aht->ah.state8 & MDBX_AAH_CREATED)
        ahe->ax_since = txn->mt_txnid;
    }
  }
  if (locked)
    mdbx_ensure(txn->mt_env, mdbx_fastmutex_release(&txn->mt_env->me_aah_lock) == MDBX_SUCCESS);
  return rc;
}

static ahe_rc_t __cold aa_open(MDBX_env_t *env, MDBX_txn_t *txn, const MDBX_iov_t aa_ident, unsigned flags,
                               MDBX_comparer_t *keycmp, MDBX_comparer_t *datacmp) {
  if (flags & MDBX_INTERIM)
    assert(txn != nullptr);

  if (flags & MDBX_INTEGERDUP)
    flags |= MDBX_DUPFIXED;
  if (flags & MDBX_DUPFIXED)
    flags |= MDBX_DUPSORT;

  flags |= MDBX_ALIEN_KCMP;
  if (keycmp == nullptr) {
    flags -= MDBX_ALIEN_KCMP;
    keycmp = default_keycmp(flags);
  }

  flags |= MDBX_ALIEN_DCMP;
  if (datacmp == nullptr) {
    flags -= MDBX_ALIEN_DCMP;
    datacmp = default_datacmp(flags);
  }

  ahe_t *const ax_main = ahe_main(env);
  ahe_rc_t rp = env_rh(MDBX_SUCCESS, ax_main);
  if (aa_ident.iov_len) {
    rp = aa_lookup(env, txn, aa_ident);
    if (unlikely(rp.err != MDBX_SUCCESS))
      return rp;
  }

  assert(rp.ahe->ax_ord16 < env->env_ah_num);
  if (rp.ahe->ax_refcounter16 > 0) {
    if (unlikely(rp.ahe->ax_kcmp != keycmp || rp.ahe->ax_dcmp != datacmp || rp.ahe->ax_flags16 != flags)) {
      rp.err = MDBX_INCOMPATIBLE;
      return rp;
    }

    if (unlikely(rp.ahe->ax_refcounter16 >= INT16_MAX)) {
      rp.err = MDBX_EREFCNT_OVERFLOW;
      return rp;
    }

  } else {
    if (rp.ahe != ax_main) {
      assert(rp.ahe->ax_ord16 >= CORE_AAH);
      if (ax_main->ax_flags16 & (MDBX_DUPSORT | MDBX_INTEGERKEY | MDBX_ALIEN_KCMP | MDBX_ALIEN_DCMP)) {
        /* can't mix named table with some main-table flags */
        rp.err = (flags & MDBX_CREATE) ? MDBX_INCOMPATIBLE : MDBX_NOTFOUND;
        return rp;
      }
      assert(ax_main->ax_kcmp != nullptr);
    }

    rp.ahe->ax_seqaah16++;
    paranoia_barrier();
    rp.ahe->ax_ident = aa_ident;
    rp.err = mdbx_iov_dup(&rp.ahe->ax_ident);
    if (unlikely(rp.err != MDBX_SUCCESS))
      return rp;

    rp.ahe->ax_since = MIN_TXNID;
    rp.ahe->ax_until = MAX_TXNID;
    rp.ahe->ax_flags16 = flags & MDBX_AA_FLAGS;
    rp.ahe->ax_kcmp = keycmp;
    rp.ahe->ax_dcmp = datacmp;
  }

  aht_t *aht = nullptr;
  if (txn && rp.ahe > ax_main) {
    aht = ahe2aht(txn, rp.ahe);
    aht_bind(aht, rp.ahe);
    rp.err = aa_fetch(txn, aht);
    if (rp.err == MDBX_NOTFOUND && (flags & MDBX_CREATE))
      rp.err = aa_create(txn, aht);
    if (unlikely(aht->ah.seq16 != rp.ahe->ax_seqaah16)) {
      aht->ah.state8 = MDBX_AAH_BAD;
      rp.err = MDBX_BAD_AAH;
    }
  }

  if (rp.err == MDBX_SUCCESS) {
    if (aht)
      assert(aht->ah.state8 & MDBX_AAH_VALID);
    rp.ahe->ax_refcounter16++;
    if (flags & MDBX_INTERIM) {
      assert(aht != nullptr);
      aht->ah.kind_and_state16 |= MDBX_AAH_INTERIM;
    }
  }

  if (rp.ahe->ax_refcounter16 < 1) {
    if (aht)
      assert(!(aht->ah.state8 & MDBX_AAH_VALID));
    mdbx_iov_free(&rp.ahe->ax_ident);
    if (rp.ahe == &env->env_ahe_array[env->env_ah_num - 1])
      env->env_ah_num--;
  }

  return rp;
}

static int aa_addref(MDBX_env_t *env, MDBX_aah_t aah) {
  ahe_t *ahe = bk_aah2ahe(env, aah);
  if (unlikely(!ahe))
    return MDBX_BAD_AAH;

  if (unlikely(ahe->ax_refcounter16 >= INT16_MAX))
    return MDBX_EREFCNT_OVERFLOW;

  ahe->ax_refcounter16++;
  return MDBX_SUCCESS;
}

static int aa_close(MDBX_env_t *env, MDBX_aah_t aah) {
  ahe_t *ahe = bk_aah2ahe(env, aah);
  if (unlikely(!ahe || ahe <= ahe_main(env)))
    return MDBX_BAD_AAH;

  if (--ahe->ax_refcounter16 < 1) {
    ahe->ax_seqaah16++;
    paranoia_barrier();
    mdbx_iov_free(&ahe->ax_ident);
  }

  return MDBX_SUCCESS;
}

static void aa_release(MDBX_env_t *env, ahe_t *ahe) {
  (void)env;
  mdbx_iov_free(&ahe->ax_ident);
}

static MDBX_sequence_result_t aa_sequence(MDBX_txn_t *txn, aht_t *aht, uint64_t increment) {
  MDBX_sequence_result_t result;
  result.value = aht->aa.genseq;

  if (likely(increment > 0)) {
    assert((txn->mt_flags & MDBX_RDONLY) == 0);

    uint64_t altered = aht->aa.genseq + increment;
    if (unlikely(altered < increment)) {
      result.err = MDBX_SIGN /* overflow */;
      return result;
    }

    assert(altered > aht->aa.genseq);
    aht->ah.state8 |= MDBX_AAH_DIRTY;
    aht->aa.genseq = altered;
    txn->mt_flags |= MDBX_TXN_DIRTY;
  }

  result.err = MDBX_SUCCESS;
  return result;
}

static inline bool aht_valid(const aht_t *aht) {
  if (unlikely(aht->ah.state8 & MDBX_AAH_BAD))
    return false;
  if (unlikely(aht->ahe->ax_seqaah16 != aht->ah.seq16))
    return false;
  if (unlikely(aht->ahe->ax_refcounter16 < 1))
    return false;
  return true;
}

//-----------------------------------------------------------------------------

/* Add all the AA's pages to the free list.
 * [in] mc Cursor on the AA to free.
 * [in] subs non-Zero to check for sub-DBs in this AA.
 * Returns 0 on success, non-zero on failure. */
static int tree_drop(cursor_t *mc, int subs) {
  int rc = page_search(mc, nullptr, MDBX_PS_FIRST);
  if (likely(rc == MDBX_SUCCESS)) {
    MDBX_txn_t *txn = mc->mc_txn;
    node_t *ni;
    cursor_t clone;
    unsigned i;

    /* DUPSORT sub-DBs have no ovpages/DBs. Omit scanning leaves.
     * This also avoids any P_DFL pages, which have no nodes.
     * Also if the AA doesn't have sub-DBs and has no overflow
     * pages, omit scanning leaves. */
    if ((mc->mc_state8 & S_SUBCURSOR) || (!subs && !mc->mc_aht->aa.overflow_pages))
      cursor_pop(mc);

    cursor_copy(mc, &clone);
    while (mc->mc_snum > 0) {
      page_t *mp = mc->mc_pg[mc->mc_top];
      unsigned n = page_numkeys(mp);
      if (IS_LEAF(mp)) {
        for (i = 0; i < n; i++) {
          ni = node_ptr(mp, i);
          if (ni->node_flags8 & NODE_BIG) {
            page_t *omp;
            pgno_t pg = get_pgno_aligned2(NODEDATA(ni));
            rc = page_get(txn, pg, &omp, nullptr);
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
            assert(IS_OVERFLOW(omp));
            rc = mdbx_pnl_append_range(&txn->mt_befree_pages, pg, omp->mp_pages);
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
            mc->mc_aht->aa.overflow_pages -= omp->mp_pages;
            if (!mc->mc_aht->aa.overflow_pages && !subs)
              break;
          } else if (subs && (ni->node_flags8 & NODE_SUBTREE)) {
            rc = tree_drop(nested_setup(mc, ni), 0);
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
          }
        }
        if (!subs && !mc->mc_aht->aa.overflow_pages)
          goto pop;
      } else {
        if (unlikely((rc = mdbx_pnl_need(&txn->mt_befree_pages, n)) != 0))
          goto done;
        for (i = 0; i < n; i++) {
          pgno_t pg;
          ni = node_ptr(mp, i);
          pg = node_get_pgno(ni);
          /* free it */
          mdbx_pnl_xappend(txn->mt_befree_pages, pg);
        }
      }
      if (!mc->mc_top)
        break;
      assert(i <= UINT16_MAX);
      mc->mc_ki[mc->mc_top] = (indx_t)i;
      rc = cursor_sibling(mc, 1);
      if (rc) {
        if (unlikely(rc != MDBX_NOTFOUND))
          goto done;
      /* no more siblings, go back to beginning
       * of previous level. */
      pop:
        cursor_pop(mc);
        mc->mc_ki[0] = 0;
        for (i = 1; i < mc->mc_snum; i++) {
          mc->mc_ki[i] = 0;
          mc->mc_pg[i] = clone.mc_pg[i];
        }
      }
    }
    /* free it */
    rc = mdbx_pnl_append(&txn->mt_befree_pages, mc->mc_aht->aa.root);
  done:
    if (unlikely(rc != MDBX_SUCCESS))
      txn->mt_flags |= MDBX_TXN_ERROR;
  } else if (rc == MDBX_NOTFOUND) {
    rc = MDBX_SUCCESS;
  }
  mc->mc_state8 &= ~C_INITIALIZED;
  return rc;
}
