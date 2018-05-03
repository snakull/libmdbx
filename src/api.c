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

#define api_extra(fmt, ...) log_extra(MDBX_LOG_API, fmt, ##__VA_ARGS__)
#define api_trace(fmt, ...) log_trace(MDBX_LOG_API, fmt, ##__VA_ARGS__)
#define api_verbose(fmt, ...) log_verbose(MDBX_LOG_API, fmt, ##__VA_ARGS__)
#define api_info(fmt, ...) log_info(MDBX_LOG_API, fmt, ##__VA_ARGS__)
#define api_notice(fmt, ...) log_notice(MDBX_LOG_API, fmt, ##__VA_ARGS__)
#define api_warning(fmt, ...) log_warning(MDBX_LOG_API, fmt, ##__VA_ARGS__)
#define api_error(fmt, ...) log_error(MDBX_LOG_API, fmt, ##__VA_ARGS__)
#define api_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_API, __func__, __LINE__, "%s, error %d", msg, err)

//-----------------------------------------------------------------------------

__cold MDBX_id128_t mdbx_bootid(void) { return osal_bootid_value; }

__cold size_t mdbx_syspage_size(void) { return osal_syspagesize; }

__cold size_t mdbx_shared_cacheline_size(void) { return osal_cacheline_size; }

//-----------------------------------------------------------------------------

MDBX_error_t mdbx_cmp(MDBX_txn_t *txn, MDBX_aah_t aah, const MDBX_iov_t *a, const MDBX_iov_t *b) {
  int rc = validate_txn_ro(txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  if (unlikely(!a || !b))
    return MDBX_EINVAL;
  if (unlikely(a->iov_len && !a->iov_base))
    return MDBX_EINVAL;
  if (unlikely(b->iov_len && !b->iov_base))
    return MDBX_EINVAL;

  const ptrdiff_t cmp = rp.aht->ahe->ax_kcmp(*a, *b);
  if (cmp < 0)
    return MDBX_LT;
  if (cmp > 0)
    return MDBX_GT;
  return MDBX_EQ;
}

MDBX_error_t mdbx_dcmp(MDBX_txn_t *txn, MDBX_aah_t aah, const MDBX_iov_t *a, const MDBX_iov_t *b) {
  int rc = validate_txn_ro(txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  if (unlikely(!a || !b))
    return MDBX_EINVAL;
  if (unlikely(a->iov_len && !a->iov_base))
    return MDBX_EINVAL;
  if (unlikely(b->iov_len && !b->iov_base))
    return MDBX_EINVAL;

  const ptrdiff_t cmp = rp.aht->ahe->ax_dcmp(*a, *b);
  if (cmp < 0)
    return MDBX_LT;
  if (cmp > 0)
    return MDBX_GT;
  return MDBX_EQ;
}

MDBX_iov_t mdbx_str2iov(const char *str) {
  MDBX_iov_t iov = {(void *)str, str ? strlen(str) : 0};
  return iov;
}

//-----------------------------------------------------------------------------

MDBX_aah_result_t __cold mdbx_aa_preopen(MDBX_env_t *env, const MDBX_iov_t aa_ident, MDBX_flags_t flags,
                                         MDBX_comparer_t *keycmp, MDBX_comparer_t *datacmp) {
  MDBX_aah_result_t result;
  result.aah = MDBX_INVALID_AAH;

  if (unlikely(flags & ~(MDBX_AA_FLAGS | MDBX_NONBLOCK))) {
    result.err = MDBX_EINVAL;
    return result;
  }

  if (unlikely(aa_ident.iov_base == nullptr && aa_ident.iov_len != 0)) {
    result.err = MDBX_EINVAL;
    return result;
  }

  result.err = validate_env(env, true);
  if (unlikely(result.err != MDBX_SUCCESS))
    return result;

  result.err = mdbx_fastmutex_acquire(&env->me_aah_lock, flags);
  if (likely(result.err == MDBX_SUCCESS)) {
    const ahe_rc_t rp = aa_open(env, nullptr, aa_ident, flags, keycmp, datacmp);
    result.err = rp.err;
    result.aah = rp.ahe ? bk_ahe2aah(env, rp.ahe) : MDBX_INVALID_AAH;
    mdbx_ensure(env, mdbx_fastmutex_release(&env->me_aah_lock) == MDBX_SUCCESS);
  }
  return result;
}

MDBX_aah_result_t __cold mdbx_aa_open(MDBX_txn_t *txn, const MDBX_iov_t aa_ident, MDBX_flags_t flags,
                                      MDBX_comparer_t *keycmp, MDBX_comparer_t *datacmp) {
  MDBX_aah_result_t result;
  result.aah = MDBX_INVALID_AAH;

  if (unlikely(flags & ~MDBX_AA_OPEN_FLAGS)) {
    result.err = MDBX_EINVAL;
    return result;
  }

  if (unlikely(aa_ident.iov_base == nullptr && aa_ident.iov_len != 0)) {
    result.err = MDBX_EINVAL;
    return result;
  }

  result.err = validate_txn_ro(txn);
  if (unlikely(result.err != MDBX_SUCCESS))
    return result;

  if (flags & MDBX_CREATE) {
    result.err = validate_txn_more4rw(txn);
    if (unlikely(result.err != MDBX_SUCCESS))
      return result;
  }

  MDBX_env_t *env = txn->mt_env;
  result.err = validate_env(env, true);
  if (unlikely(result.err != MDBX_SUCCESS))
    return result;

  result.err = mdbx_fastmutex_acquire(&env->me_aah_lock, flags);
  if (likely(result.err == MDBX_SUCCESS)) {
    const ahe_rc_t rp = aa_open(env, txn, aa_ident, flags, keycmp, datacmp);
    result.err = rp.err;
    result.aah = likely(result.err == MDBX_SUCCESS) ? bk_ahe2aah(env, rp.ahe) : MDBX_INVALID_AAH;
    mdbx_ensure(env, mdbx_fastmutex_release(&env->me_aah_lock) == MDBX_SUCCESS);
  }
  return result;
}

MDBX_error_t mdbx_aa_addref(MDBX_env_t *env, MDBX_aah_t aah) {
  MDBX_error_t rc = validate_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = mdbx_fastmutex_acquire(&env->me_aah_lock, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = aa_addref(env, aah);
  mdbx_ensure(env, mdbx_fastmutex_release(&env->me_aah_lock) == MDBX_SUCCESS);
  return rc;
}

MDBX_error_t mdbx_aa_close(MDBX_env_t *env, MDBX_aah_t aah) {
  MDBX_error_t rc = validate_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = mdbx_fastmutex_acquire(&env->me_aah_lock, 0);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = aa_close(env, aah);
    mdbx_ensure(env, mdbx_fastmutex_release(&env->me_aah_lock) == MDBX_SUCCESS);
  }
  return rc;
}

MDBX_error_t mdbx_aa_drop(MDBX_txn_t *txn, MDBX_aah_t aah, mdbx_drop_flags_t flags) {
  if (unlikely((MDBX_PURGE_AA | MDBX_DELETE_AA | MDBX_CLOSE_HANDLE) < flags))
    return MDBX_EINVAL;

  MDBX_error_t rc = validate_txn_rw(txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  rc = aa_drop(txn, flags, rp.aht);
  if (likely(rc == MDBX_SUCCESS) && (flags & MDBX_CLOSE_HANDLE)) {
    rp.aht->ah.kind_and_state16 &= ~MDBX_AAH_INTERIM;
    rc = mdbx_fastmutex_acquire(&txn->mt_env->me_aah_lock, 0);
    if (likely(rc == MDBX_SUCCESS)) {
      rc = aa_close(txn->mt_env, aah);
      mdbx_ensure(txn->mt_env, mdbx_fastmutex_release(&txn->mt_env->me_aah_lock) == MDBX_SUCCESS);
    }
  }
  return rc;
}

MDBX_error_t mdbx_aa_info(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_aa_info_t *info, size_t info_size) {
  if (unlikely(!info || info_size != sizeof(MDBX_aa_info_t)))
    return MDBX_EINVAL;

  MDBX_error_t rc = validate_txn_ro(txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  info->ai_tree_depth = rp.aht->aa.depth16;
  info->ai_branch_pages = rp.aht->aa.branch_pages;
  info->ai_leaf_pages = rp.aht->aa.leaf_pages;
  info->ai_overflow_pages = rp.aht->aa.overflow_pages;
  info->ai_entries = rp.aht->aa.entries;
  info->ai_flags = rp.aht->aa.flags16;
  info->ai_sequence = rp.aht->aa.genseq;
  info->ai_ident = rp.aht->ahe->ax_ident;
  return MDBX_SUCCESS;
}

MDBX_state_result_t mdbx_aa_state(MDBX_txn_t *txn, MDBX_aah_t aah) {
  MDBX_state_result_t result;
  result.state = ~0u;
  result.err = validate_txn_ro(txn);
  if (unlikely(result.err != MDBX_SUCCESS))
    return result;

  ahe_t *ahe = bk_aah2ahe(txn->mt_env, aah);
  if (unlikely(!ahe)) {
    result.err = MDBX_BAD_AAH;
    return result;
  }

  result.state = aa_state(txn, ahe);
  result.err = MDBX_SUCCESS;
  return result;
}

MDBX_sequence_result_t mdbx_aa_sequence(MDBX_txn_t *txn, MDBX_aah_t aah, uint64_t increment) {
  MDBX_sequence_result_t result;
  result.value = UINT64_MAX;
  result.err = validate_txn_ro(txn);
  if (unlikely(result.err != MDBX_SUCCESS))
    return result;

  if (increment != 0) {
    result.err = validate_txn_more4rw(txn);
    if (unlikely(result.err != MDBX_SUCCESS))
      return result;
  }

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS)) {
    result.err = rp.err;
    return result;
  }

  return aa_sequence(txn, rp.aht, increment);
}

//-----------------------------------------------------------------------------

static inline MDBX_cursor_result_t cursor_result(int err, MDBX_cursor_t *cursor) {
  assert(MDBX_IS_ERROR(err) == (cursor == nullptr));
  MDBX_cursor_result_t result = {cursor, err};
  return result;
}

MDBX_cursor_result_t mdbx_cursor_open(MDBX_txn_t *txn, MDBX_aah_t aah) {
  int rc = validate_txn_ro(txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return cursor_result(rc, nullptr);

  aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return cursor_result(rp.err, nullptr);

  if (unlikely(rp.aht == aht_gaco(txn) && !F_ISSET(txn->mt_flags, MDBX_RDONLY)))
    return cursor_result(MDBX_EPERM, nullptr);

  MDBX_cursor_t *cursor = malloc(cursor_size(rp.aht));
  if (unlikely(cursor == nullptr))
    return cursor_result(MDBX_ENOMEM, nullptr);

  rc = cursor_open(txn, rp.aht, cursor);
  if (unlikely(rc != MDBX_SUCCESS)) {
    free(cursor);
    return cursor_result(rc, nullptr);
  }

  return cursor_result(MDBX_SUCCESS, cursor);
}

int mdbx_cursor_close(MDBX_cursor_t *cursor) {
  int rc = validate_cursor4close(cursor);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = cursor_close(cursor);
  switch (rc) {
  case MDBX_SUCCESS:
    free(cursor);
  /* fall through */
  case MDBX_SIGN:
    return MDBX_SUCCESS;
  default:
    return rc;
  }
}

int mdbx_cursor_renew(MDBX_txn_t *txn, MDBX_cursor_t *bundle) {
  int rc = validate_cursor4close(bundle);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = validate_txn_ro(txn);
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_BAD_TXN ||
        (txn->mt_flags & MDBX_TXN_BLOCKED) != MDBX_TXN_FINISHED /* allow untracking for finished txn */)
      return rc;
  }

  if (unlikely(bundle->mc_backup))
    return MDBX_EPERM;

  cursor_untrack(bundle);
  if (unlikely(rc != MDBX_SUCCESS))
    /* return error after untracking for finished txn */
    return rc;

  return cursor_init(bundle, txn, bundle->primal.mc_aht);
}

MDBX_txn_result_t mdbx_cursor_txn(const MDBX_cursor_t *cursor) {
  MDBX_txn_result_t result = {nullptr, validate_cursor4operation_ro(cursor)};
  if (likely(result.err == MDBX_SUCCESS))
    result.txn = cursor->primal.mc_txn;
  return result;
}

MDBX_aah_result_t mdbx_cursor_aah(const MDBX_cursor_t *cursor) {
  MDBX_aah_result_t result = {MDBX_INVALID_AAH, validate_cursor4operation_ro(cursor)};
  if (likely(result.err == MDBX_SUCCESS)) {
    if (likely(cursor_is_aah_valid(cursor)))
      result.aah = bk_ahe2aah(cursor->primal.mc_txn->mt_env, cursor->primal.mc_aht->ahe);
    else
      result.err = MDBX_BAD_AAH;
  }
  return result;
}

int mdbx_cursor_get(MDBX_cursor_t *cursor, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_cursor_op_t op) {
  MDBX_error_t err = validate_cursor4operation_ro(cursor);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  return cursor_get(&cursor->primal, key, data, op);
}

MDBX_numeric_result_t mdbx_cursor_count(MDBX_cursor_t *cursor) {
  MDBX_error_t err = validate_cursor4operation_ro(cursor);
  if (unlikely(err != MDBX_SUCCESS))
    return numeric_result(0, err);

  return cursor_count(cursor);
}

int mdbx_cursor_put(MDBX_cursor_t *cursor, MDBX_iov_t *key, MDBX_iov_t *data, unsigned flags) {
  STATIC_ASSERT(((MDBX_IUD_FLAGS & NODE_ADD_FLAGS) & MDBX_NODE_FLAGS) == 0);

  if (unlikely(cursor == nullptr || key == nullptr))
    return MDBX_EINVAL;

  if (unlikely(flags & ~MDBX_IUD_FLAGS))
    return MDBX_EINVAL;

  int rc = validate_cursor4operation_rw(cursor);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_env_t *const env = cursor->primal.mc_txn->mt_env;
  if (unlikely(key->iov_len > env->me_keymax))
    return MDBX_BAD_VALSIZE;

  aht_t *const aht = cursor->primal.mc_aht;
  if (unlikely(data->iov_len > ((aht->aa.flags16 & MDBX_DUPSORT) ? env->me_keymax : MDBX_MAXDATASIZE)))
    return MDBX_BAD_VALSIZE;

  if (flags & MDBX_IUD_RESERVE) {
    if (unlikely(aht->aa.flags16 & (MDBX_DUPSORT | MDBX_REVERSEDUP)))
      return MDBX_INCOMPATIBLE;
    data->iov_base = nullptr;
  }

  if ((aht->aa.flags16 & MDBX_INTEGERKEY) &&
      unlikely(key->iov_len != sizeof(uint32_t) && key->iov_len != sizeof(uint64_t))) {
    assert(!"key-size is invalid for MDBX_INTEGERKEY");
    return MDBX_BAD_VALSIZE;
  }

  if ((aht->aa.flags16 & MDBX_INTEGERDUP) &&
      unlikely(data->iov_len != sizeof(uint32_t) && data->iov_len != sizeof(uint64_t))) {
    assert(!"data-size is invalid MDBX_INTEGERDUP");
    return MDBX_BAD_VALSIZE;
  }

  return cursor_put(&cursor->primal, key, data, flags);
}

// int mdbx_cursor_put_multiple(MDBX_cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t
// *data,
//                unsigned flags) {
//}

MDBX_error_t mdbx_cursor_delete(MDBX_cursor_t *mc, unsigned flags) {
  if (flags & ~MDBX_IUD_NODUP)
    return MDBX_EINVAL;

  int rc = validate_cursor4operation_rw(mc);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return cursor_delete(&mc->primal, flags);
}

//-----------------------------------------------------------------------------

MDBX_error_t __cold mdbx_bk_copy2fd(MDBX_env_t *env, MDBX_filehandle_t fd, MDBX_copy_flags_t flags) {
  MDBX_error_t rc = validate_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (flags & MDBX_CP_COMPACT)
    return bk_copy_compact(env, fd);

  return bk_copy_asis(env, fd);
}

MDBX_error_t mdbx_bk_copy(MDBX_env_t *env, const char *pathname, MDBX_copy_flags_t flags) {
  MDBX_error_t rc = validate_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  char *molded_dxb_filename = nullptr;
  MDBX_filehandle_t newfd = MDBX_INVALID_FD;

  if (flags & MDBX_CP_NOSUBDIR) {
    molded_dxb_filename = (char *)pathname;
  } else {
    if (mdbx_asprintf(&molded_dxb_filename, "%s%s", pathname, MDBX_PATH_SEPARATOR MDBX_DATANAME) < 0) {
      rc = mdbx_get_errno();
      goto bailout;
    }
    pathname = molded_dxb_filename;
  }

  /* The destination path must exist, but the destination file must not.
   * We don't want the OS to cache the writes, since the source data is
   * already in the OS cache. */
  rc = mdbx_openfile(pathname, O_WRONLY | O_CREAT | O_EXCL, 0666, &newfd);
  if (rc == MDBX_SUCCESS) {
    if (env->me_psize >= osal_syspagesize) {
#ifdef F_NOCACHE /* __APPLE__ */
      (void)fcntl(newfd, F_NOCACHE, 1);
#elif defined(O_DIRECT) && defined(F_GETFL)
      /* Set O_DIRECT if the file system supports it */
      if ((rc = fcntl(newfd, F_GETFL)) != -1)
        (void)fcntl(newfd, F_SETFL, rc | O_DIRECT);
#endif
    }
    rc = mdbx_bk_copy2fd(env, newfd, flags);
  }

bailout:
  free(molded_dxb_filename);

  if (newfd != MDBX_INVALID_FD) {
    MDBX_error_t err = mdbx_closefile(newfd);
    if (rc == MDBX_SUCCESS && rc != err)
      rc = err;
  }

  return rc;
}

//-----------------------------------------------------------------------------

MDBX_numeric_result_t __cold mdbx_pagesize2maxkeylen(size_t pagesize) {
  if (pagesize == 0)
    pagesize = osal_syspagesize;

  MDBX_numeric_result_t result;
  result.value = 0;
  result.err = MDBX_EINVAL;

  const intptr_t nodemax = mdbx_nodemax(pagesize);
  if (likely(nodemax > 0)) {
    const intptr_t maxkey = mdbx_maxkey(nodemax);
    if (likely(maxkey > 0 && maxkey < INT_MAX))
      result.value = (int)maxkey;
  }
  return result;
}

MDBX_error_t mdbx_info_ex(MDBX_env_t *env, MDBX_db_info_t *info, size_t info_size, MDBX_aux_info_t *aux,
                          size_t aux_size) {
  if (info_size != sizeof(MDBX_db_info_t) && !(info == nullptr && info_size == 0))
    return MDBX_EINVAL;

  if (aux_size != sizeof(MDBX_aux_info_t) && !(aux == nullptr && aux_size == 0))
    return MDBX_EINVAL;

  MDBX_error_t rc = validate_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (info_size) {
    const meta_t *const meta0 = metapage(env, 0);
    const meta_t *const meta1 = metapage(env, 1);
    const meta_t *const meta2 = metapage(env, 2);
    const meta_t *meta;
    do {
      meta = meta_head(env);
      info->bi_recent_txnid = meta_txnid_fluid(env, meta);
      info->bi_meta[0].txnid = meta_txnid_fluid(env, meta0);
      info->bi_meta[0].sign_checksum = meta0->mm_sign_checksum;
      info->bi_meta[1].txnid = meta_txnid_fluid(env, meta1);
      info->bi_meta[1].sign_checksum = meta1->mm_sign_checksum;
      info->bi_meta[2].txnid = meta_txnid_fluid(env, meta2);
      info->bi_meta[2].sign_checksum = meta2->mm_sign_checksum;
      info->bi_dxb_last_pgno = meta->mm_dxb_geo.next - 1;
      memset(&info->bi_sld_geo, 0, sizeof(info->bi_sld_geo) /* FIXME: TODO */);
      info->bi_sld_first_pgno = 0 /* FIXME: TODO */;
      info->bi_dxb_geo.lower = pgno2bytes(env, meta->mm_dxb_geo.lower);
      info->bi_dxb_geo.upper = pgno2bytes(env, meta->mm_dxb_geo.upper);
      info->bi_dxb_geo.current = pgno2bytes(env, meta->mm_dxb_geo.now);
      info->bi_dxb_geo.shrink = (uint32_t)pgno2bytes(env, meta->mm_dxb_geo.shrink16);
      info->bi_dxb_geo.grow = (uint32_t)pgno2bytes(env, meta->mm_dxb_geo.grow16);
      info->bi_mapsize = env->me_mapsize;
      info->bi_dirty_volume = env->me_lck ? env->me_lck->li_dirty_volume : 0;
      mdbx_compiler_barrier();
    } while (unlikely(info->bi_meta[0].txnid != meta_txnid_fluid(env, meta0) ||
                      info->bi_meta[0].sign_checksum != meta0->mm_sign_checksum ||
                      info->bi_meta[1].txnid != meta_txnid_fluid(env, meta1) ||
                      info->bi_meta[1].sign_checksum != meta1->mm_sign_checksum ||
                      info->bi_meta[2].txnid != meta_txnid_fluid(env, meta2) ||
                      info->bi_meta[2].sign_checksum != meta2->mm_sign_checksum || meta != meta_head(env) ||
                      info->bi_recent_txnid != meta_txnid_fluid(env, meta)));

    info->bi_pagesize = env->me_psize;
    info->bi_sys_pagesize = osal_syspagesize;
    info->bi_dxb_geo.ioblk = 0 /* FIXME: TODO */;
    info->bi_dxb_geo.granularity = 0 /* FIXME: TODO */;
    info->bi_sld_geo.ioblk = 0 /* FIXME: TODO */;
    info->bi_sld_geo.granularity = 0 /* FIXME: TODO */;
    info->bi_maxkeysize = env->me_keymax;

    info->bi_readers_max = env->me_maxreaders;
    if (env->me_lck) {
      MDBX_reader_t *r = env->me_lck->li_readers;
      info->bi_latter_reader_txnid = info->bi_self_latter_reader_txnid = info->bi_recent_txnid;
      for (unsigned i = 0; i < info->bi_readers_num; ++i) {
        const MDBX_pid_t pid = r[i].mr_pid;
        if (pid) {
          const txnid_t txnid = r[i].mr_txnid;
          if (info->bi_latter_reader_txnid > txnid)
            info->bi_latter_reader_txnid = txnid;
          if (pid == env->me_pid && info->bi_self_latter_reader_txnid > txnid)
            info->bi_self_latter_reader_txnid = txnid;
        }
      }
      info->bi_readers_num = env->me_lck->li_numreaders;
      info->bi_autosync_threshold = env->me_lck->li_autosync_threshold;
      info->bi_regime = env->me_lck->li_regime;
    } else {
      info->bi_readers_num = INT32_MAX;
      info->bi_latter_reader_txnid = info->bi_self_latter_reader_txnid = 0;
      info->bi_autosync_threshold = 0;
      info->bi_dirty_volume = 0;
      info->bi_regime = MDBX_RDONLY;
    }
  }

  if (aux) {
    aux->ei_aah_max = env->env_ah_max;
    aux->ei_aah_num = env->env_ah_num;
// aux->ei_debug_bits = mdbx_debug_bits;
// aux->ei_userctx = env->me_userctx;
// aux->ei_debug_callback = mdbx_debug_logger;
#if MDBX_DEBUG
    aux->ei_assert_callback = env->me_assert_func;
#else
    aux->ei_assert_callback = nullptr;
#endif
    aux->ei_rbr_callback = env->me_callback_rbr;
    aux->ei_dxb_fd = env->me_dxb_fd;
    aux->ei_clk_fd = env->me_lck_fd;
    aux->ei_sld_fd = MDBX_INVALID_FD /* FIXME: TODO */;
    aux->ei_pathnames = env->me_pathname_buf;
  }

  return MDBX_SUCCESS;
}

MDBX_error_t mdbx_info(MDBX_env_t *env, MDBX_db_info_t *info, size_t info_size) {
  return mdbx_info_ex(env, info, info_size, nullptr, 0);
}

MDBX_numeric_result_t __cold mdbx_txn_flags(MDBX_txn_t *txn) {
  MDBX_numeric_result_t result;

  result.value = 0;
  result.err = validate_txn_ro(txn);
  if (likely(result.err == MDBX_SUCCESS))
    result.value = txn->mt_flags;
  return result;
}

MDBX_numeric_result_t mdbx_txn_lag(MDBX_txn_t *txn, unsigned *alloc_ratio) {
  MDBX_numeric_result_t result;
  result.value = 0;
  result.err = validate_txn_ro(txn);
  if (unlikely(result.err != MDBX_SUCCESS))
    return result;

  pgno_t limit, allocated;
  MDBX_env_t *env = txn->mt_env;
  if (likely(txn->mt_flags & MDBX_RDONLY)) {
    for (;;) {
      meta_t *meta = meta_head(env);
      txnid_t recent = meta_txnid_fluid(env, meta);
      limit = meta->mm_dxb_geo.upper;
      allocated = meta->mm_dxb_geo.next;
      if (likely(meta == meta_head(env) && recent == meta_txnid_fluid(env, meta))) {
        txnid_t gap = recent - txn->mt_txnid;
        result.value = unlikely(gap > UINTPTR_MAX) ? UINTPTR_MAX : (uintptr_t)gap;
        break;
      }
    }
  } else {
    limit = meta_head(env)->mm_dxb_geo.upper;
    allocated = txn->mt_next_pgno;
    result.err = MDBX_SIGN;
  }

  if (alloc_ratio)
    *alloc_ratio = (unsigned)(allocated * UINT64_C(65536) / limit);
  return result;
}

MDBX_error_t __cold mdbx_walk(MDBX_txn_t *txn, MDBX_walk_func_t *visitor, void *user) {
  MDBX_error_t err = validate_txn_ro(txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  walk_ctx_t ctx;
  ctx.mw_txn = txn;
  ctx.mw_user = user;
  ctx.mw_visitor = visitor;

  err = visitor(0, MDBX_NUM_METAS, user, mdbx_str2iov("meta"), "meta", MDBX_NUM_METAS,
                sizeof(meta_t) * MDBX_NUM_METAS, PAGEHDRSZ * MDBX_NUM_METAS,
                (txn->mt_env->me_psize - sizeof(meta_t) - PAGEHDRSZ) * MDBX_NUM_METAS);
  if (!err)
    err = do_walk(&ctx, mdbx_str2iov("gaco"), txn->txn_aht_array[MDBX_GACO_AAH].aa.root, 0);
  if (!err)
    err = do_walk(&ctx, mdbx_str2iov("main"), txn->txn_aht_array[MDBX_MAIN_AAH].aa.root, 0);
  if (!err)
    err = visitor(P_INVALID, 0, user, mdbx_str2iov(nullptr), nullptr, 0, 0, 0, 0);
  return err;
}

MDBX_error_t mdbx_canary_put(MDBX_txn_t *txn, const MDBX_canary_t *canary) {
  MDBX_error_t err = validate_txn_rw(txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (likely(canary)) {
    if (txn->mt_canary.x == canary->x && txn->mt_canary.y == canary->y && txn->mt_canary.z == canary->z)
      return MDBX_SUCCESS;
    txn->mt_canary.x = canary->x;
    txn->mt_canary.y = canary->y;
    txn->mt_canary.z = canary->z;
  }
  txn->mt_canary.v = txn->mt_txnid;

  if ((txn->mt_flags & MDBX_TXN_DIRTY) == 0) {
    MDBX_env_t *env = txn->mt_env;
    txn->mt_flags |= MDBX_TXN_DIRTY;
    env->me_lck->li_dirty_volume += env->me_psize;
  }

  return MDBX_SUCCESS;
}

//-----------------------------------------------------------------------------

MDBX_error_t mdbx_canary_get(MDBX_txn_t *txn, MDBX_canary_t *canary) {
  if (unlikely(!canary))
    return MDBX_EINVAL;

  MDBX_error_t err = validate_txn_ro(txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  *canary = txn->mt_canary;
  return MDBX_SUCCESS;
}

MDBX_error_t mdbx_cursor_at_first(MDBX_cursor_t *cursor) {
  MDBX_error_t err = validate_cursor4operation_ro(cursor);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (!(cursor->primal.mc_state8 & C_INITIALIZED))
    return MDBX_SUCCESS;

  for (unsigned i = 0; i < cursor->primal.mc_snum; ++i) {
    if (cursor->primal.mc_ki[i])
      return MDBX_SUCCESS;
  }

  /* if (cursor->nested.mx_cursor.mc_state8 & C_INITIALIZED) {
    for (unsigned i = 0; i < cursor->nested.mx_cursor.mc_snum; ++i) {
      if (cursor->nested.mx_cursor.mc_ki[i])
        return MDBX_SUCCESS;
    }
  } */

  return MDBX_SIGN;
}

MDBX_error_t mdbx_cursor_at_last(MDBX_cursor_t *cursor) {
  MDBX_error_t err = validate_cursor4operation_ro(cursor);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (!(cursor->primal.mc_state8 & C_INITIALIZED))
    return MDBX_SUCCESS;

  for (unsigned i = 0; i < cursor->primal.mc_snum; ++i) {
    unsigned nkeys = page_numkeys(cursor->primal.mc_pg[i]);
    if (cursor->primal.mc_ki[i] < nkeys - 1)
      return MDBX_SUCCESS;
  }

  /* if (cursor->nested.mx_cursor.mc_state8 & C_INITIALIZED) {
    for (unsigned i = 0; i < cursor->nested.mx_cursor.mc_snum; ++i) {
      unsigned nkeys = page_numkeys(cursor->nested.mx_cursor.mc_pg[i]);
      if (cursor->nested.mx_cursor.mc_ki[i] < nkeys - 1)
        return MDBX_SUCCESS;
    }
  } */

  return MDBX_SIGN;
}

MDBX_error_t mdbx_cursor_eof(MDBX_cursor_t *cursor) {
  MDBX_error_t err = validate_cursor4operation_ro(cursor);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (!(cursor->primal.mc_state8 & C_INITIALIZED) || cursor->primal.mc_snum == 0)
    return MDBX_EOF;

  if ((cursor->primal.mc_state8 & C_EOF) &&
      cursor->primal.mc_ki[cursor->primal.mc_top] >= page_numkeys(cursor->primal.mc_pg[cursor->primal.mc_top]))
    return MDBX_EOF;

  return MDBX_SUCCESS;
}

//-----------------------------------------------------------------------------

MDBX_error_t mdbx_put(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                      mdbx_iud_flags_t flags) {

  if (unlikely(!key || !data || !txn))
    return MDBX_EINVAL;

  MDBX_error_t err = validate_txn_rw(txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  MDBX_cursor_t mc;
  err = cursor_init(&mc, txn, rp.aht);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  MDBX_cursor_t **const head = aa_txn_cursor_tracking_head(txn, aah);
  mc.mc_next = *head;
  *head = &mc;

  /* LY: support for update (explicit overwrite) */
  if (flags & MDBX_IUD_CURRENT) {
    err = mdbx_cursor_get(&mc, key, nullptr, MDBX_SET);
    if (likely(err == MDBX_SUCCESS) && (rp.aht->ah.kind_and_state16 & MDBX_DUPSORT)) {
      /* LY: allows update (explicit overwrite) only for unique keys */
      node_t *leaf = node_ptr(mc.primal.mc_pg[mc.primal.mc_top], mc.primal.mc_ki[mc.primal.mc_top]);
      if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
        assert(cursor_is_nested_inited(&mc) && mc.subcursor.mx_aht_body.aa.entries > 1);
        err = MDBX_EMULTIVAL;
      }
    }
  }

  if (likely(err == MDBX_SUCCESS))
    err = cursor_put(&mc.primal, key, data, flags);
  *head = mc.mc_next;

  return err;
}

MDBX_error_t mdbx_del_ex(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                         mdbx_iud_flags_t flags) {
  if (unlikely(!key || !txn))
    return MDBX_EINVAL;

  MDBX_error_t err = validate_txn_rw(txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  DKBUF;
  mdbx_debug("====> delete AA %" PRIuFAST32 " key [%s], data [%s]", aah, DKEY(key), DVAL(data));

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  MDBX_cursor_t mc;
  err = cursor_init(&mc, txn, rp.aht);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  MDBX_cursor_op_t op;
  MDBX_iov_t rdata;
  if (data) {
    op = MDBX_GET_BOTH;
    rdata = *data;
    data = &rdata;
  } else {
    op = MDBX_SET;
    flags |= MDBX_IUD_NODUP;
  }

  int exact = 0;
  err = cursor_set(&mc.primal, key, data, op, &exact);
  if (likely(err == MDBX_SUCCESS)) {
    /* let page_split know about this cursor if needed:
     * delete will trigger a rebalance; if it needs to move
     * a node from one page to another, it will have to
     * update the parent's separator key(s). If the new sepkey
     * is larger than the current one, the parent page may
     * run out of space, triggering a split. We need this
     * cursor to be consistent until the end of the rebalance. */
    MDBX_cursor_t **const head = aa_txn_cursor_tracking_head(txn, aah);
    mc.mc_next = *head;
    *head = &mc;
    err = mdbx_cursor_delete(&mc, flags);
    *head = mc.mc_next;
  }
  return err;
}

/* Позволяет обновить или удалить существующую запись с получением
 * в old_data предыдущего значения данных. При этом если new_data равен
 * нулю, то выполняется удаление, иначе обновление/вставка.
 *
 * Текущее значение может находиться в уже измененной (грязной) странице.
 * В этом случае страница будет перезаписана при обновлении, а само старое
 * значение утрачено. Поэтому исходно в old_data должен быть передан
 * дополнительный буфер для копирования старого значения.
 * Если переданный буфер слишком мал, то функция вернет -1, установив
 * old_data->iov_len в соответствующее значение.
 *
 * Для не-уникальных ключей также возможен второй сценарий использования,
 * когда посредством old_data из записей с одинаковым ключом для
 * удаления/обновления выбирается конкретная. Для выбора этого сценария
 * во flags следует одновременно указать MDBX_IUD_CURRENT и
 * MDBX_IUD_NOOVERWRITE.
 * Именно эта комбинация выбрана, так как она лишена смысла, и этим позволяет
 * идентифицировать запрос такого сценария.
 *
 * Функция может быть замещена соответствующими операциями с курсорами
 * после двух доработок (TODO):
 *  - внешняя аллокация курсоров, в том числе на стеке (без malloc).
 *  - получения статуса страницы по адресу (знать о P_DIRTY).
 */
MDBX_error_t mdbx_replace(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *new_data,
                          MDBX_iov_t *old_data, mdbx_iud_flags_t flags) {
  if (unlikely(!key || !old_data || old_data == new_data))
    return MDBX_EINVAL;

  if (unlikely(old_data->iov_base == nullptr && old_data->iov_len))
    return MDBX_EINVAL;

  if (unlikely(new_data == nullptr && !(flags & MDBX_IUD_CURRENT)))
    return MDBX_EINVAL;

  if (unlikely(flags &
               ~(MDBX_IUD_NOOVERWRITE | MDBX_IUD_NODUP | MDBX_IUD_RESERVE | MDBX_IUD_APPEND |
                 MDBX_IUD_APPENDDUP | MDBX_IUD_CURRENT)))
    return MDBX_EINVAL;

  MDBX_error_t err = validate_txn_rw(txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  MDBX_cursor_t mc;
  err = cursor_init(&mc, txn, rp.aht);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
  MDBX_cursor_t **const head = cursor_tracking_head(&mc);
  mc.mc_next = *head;
  *head = &mc;

  MDBX_iov_t present_key = *key;
  if (F_ISSET(flags, MDBX_IUD_CURRENT | MDBX_IUD_NOOVERWRITE)) {
    /* в old_data значение для выбора конкретного дубликата */
    if (unlikely(!(rp.aht->aa.flags16 & MDBX_DUPSORT))) {
      err = MDBX_EINVAL;
      goto bailout;
    }

    /* убираем лишний бит, он был признаком запрошенного режима */
    flags -= MDBX_IUD_NOOVERWRITE;

    err = mdbx_cursor_get(&mc, &present_key, old_data, MDBX_GET_BOTH);
    if (err != MDBX_SUCCESS)
      goto bailout;

    if (new_data) {
      /* обновление конкретного дубликата */
      if (mdbx_iov_eq(old_data, new_data))
        /* если данные совпадают, то ничего делать не надо */
        goto bailout;
    }
  } else {
    /* в old_data буфер для сохранения предыдущего значения */
    if (unlikely(new_data && old_data->iov_base == new_data->iov_base))
      return MDBX_EINVAL;
    MDBX_iov_t present_data;
    err = mdbx_cursor_get(&mc, &present_key, &present_data, MDBX_SET_KEY);
    if (unlikely(err != MDBX_SUCCESS)) {
      old_data->iov_base = nullptr;
      old_data->iov_len = err;
      if (err != MDBX_NOTFOUND || (flags & MDBX_IUD_CURRENT))
        goto bailout;
    } else if (flags & MDBX_IUD_NOOVERWRITE) {
      err = MDBX_KEYEXIST;
      *old_data = present_data;
      goto bailout;
    } else {
      page_t *page = mc.primal.mc_pg[mc.primal.mc_top];
      if (rp.aht->aa.flags16 & MDBX_DUPSORT) {
        if (flags & MDBX_IUD_CURRENT) {
          /* для не-уникальных ключей позволяем update/delete только если ключ
           * один */
          node_t *leaf = node_ptr(page, mc.primal.mc_ki[mc.primal.mc_top]);
          if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
            assert((mc.subcursor.mx_cursor.mc_state8 & C_INITIALIZED) &&
                   mc.subcursor.mx_aht_body.aa.entries > 1);
            if (mc.subcursor.mx_aht_body.aa.entries > 1) {
              err = MDBX_EMULTIVAL;
              goto bailout;
            }
          }
          /* если данные совпадают, то ничего делать не надо */
          if (new_data && mdbx_iov_eq(&present_data, new_data)) {
            *old_data = *new_data;
            goto bailout;
          }
          /* В оригинальной LMDB фладок MDBX_IUD_CURRENT здесь приведет
           * к замене данных без учета MDBX_DUPSORT сортировки,
           * но здесь это в любом случае допустимо, так как мы
           * проверили что для ключа есть только одно значение. */
        } else if ((flags & MDBX_IUD_NODUP) && mdbx_iov_eq(&present_data, new_data)) {
          /* если данные совпадают и установлен MDBX_IUD_NODUP */
          err = MDBX_KEYEXIST;
          goto bailout;
        }
      } else {
        /* если данные совпадают, то ничего делать не надо */
        if (new_data && mdbx_iov_eq(&present_data, new_data)) {
          *old_data = *new_data;
          goto bailout;
        }
        flags |= MDBX_IUD_CURRENT;
      }

      if (page->mp_flags16 & P_DIRTY) {
        if (unlikely(old_data->iov_len < present_data.iov_len)) {
          old_data->iov_base = nullptr;
          old_data->iov_len = present_data.iov_len;
          err = MDBX_SIGN;
          goto bailout;
        }
        memcpy(old_data->iov_base, present_data.iov_base, present_data.iov_len);
        old_data->iov_len = present_data.iov_len;
      } else {
        *old_data = present_data;
      }
    }
  }

  if (likely(new_data))
    err = cursor_put(&mc.primal, key, new_data, flags);
  else
    err = mdbx_cursor_delete(&mc, 0);

bailout:
  *head = mc.mc_next;
  return err;
}

MDBX_error_t mdbx_get(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data) {

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  MDBX_error_t rc = validate_txn_ro(txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  DKBUF;
  mdbx_debug("===> get db %" PRIuFAST32 " key [%s]", aah, DKEY(key));

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  MDBX_cursor_t mc;
  rc = cursor_init(&mc, txn, rp.aht);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  int exact = 0;
  return cursor_set(&mc.primal, key, data, MDBX_SET, &exact);
}

MDBX_error_t mdbx_get_ex(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                         size_t *values_count) {
  DKBUF;
  mdbx_debug("===> get AA %" PRIuFAST32 " key [%s]", aah, DKEY(key));

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  MDBX_error_t err = validate_txn_rw(txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  MDBX_cursor_t mc;
  err = cursor_init(&mc, txn, rp.aht);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  int exact = 0;
  err = cursor_set(&mc.primal, key, data, MDBX_SET_KEY, &exact);
  if (unlikely(err != MDBX_SUCCESS)) {
    if (err == MDBX_NOTFOUND && values_count)
      *values_count = 0;
    return err;
  }

  if (values_count) {
    *values_count = 1;
    if (mc.primal.mc_kind8 & S_HAVESUB) {
      node_t *leaf = node_ptr(mc.primal.mc_pg[mc.primal.mc_top], mc.primal.mc_ki[mc.primal.mc_top]);
      if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
        assert(mc.subcursor.mx_cursor.mc_state8 & C_INITIALIZED);
        *values_count = (sizeof(*values_count) >= sizeof(mc.subcursor.mx_aht_body.aa.entries) ||
                         mc.subcursor.mx_aht_body.aa.entries <= SIZE_MAX)
                            ? (size_t)mc.subcursor.mx_aht_body.aa.entries
                            : SIZE_MAX;
      }
    }
  }
  return MDBX_SUCCESS;
}

//-----------------------------------------------------------------------------

/* Функция сообщает находится ли указанный адрес в "грязной" странице у
 * заданной пишущей транзакции. В конечном счете это позволяет избавиться от
 * лишнего копирования данных из НЕ-грязных страниц.
 *
 * "Грязные" страницы - это те, которые уже были изменены в ходе пишущей
 * транзакции. Соответственно, какие-либо дальнейшие изменения могут привести
 * к перезаписи таких страниц. Поэтому все функции, выполняющие изменения, в
 * качестве аргументов НЕ должны получать указатели на данные в таких
 * страницах. В свою очередь "НЕ грязные" страницы перед модификацией будут
 * скопированы.
 *
 * Другими словами, данные из "грязных" страниц должны быть либо скопированы
 * перед передачей в качестве аргументов для дальнейших модификаций, либо
 * отвергнуты на стадии проверки корректности аргументов.
 *
 * Таким образом, функция позволяет как избавится от лишнего копирования,
 * так и выполнить более полную проверку аргументов.
 *
 * ВАЖНО: Передаваемый указатель должен указывать на начало данных. Только
 * так гарантируется что актуальный заголовок страницы будет физически
 * расположен в той-же странице памяти, в том числе для многостраничных
 * P_OVERFLOW страниц с длинными данными. */
MDBX_error_t mdbx_is_dirty(MDBX_txn_t *txn, const void *ptr) {
  MDBX_error_t err = validate_txn_rw(txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const MDBX_env_t *env = txn->mt_env;
  const uintptr_t mask = ~(uintptr_t)(env->me_psize - 1);
  const page_t *page = (const page_t *)((uintptr_t)ptr & mask);

  /* LY: Тут не всё хорошо с абсолютной достоверностью результата,
   * так как флажок P_DIRTY в LMDB может означать не совсем то,
   * что было исходно задумано, детали см в логике кода page_touch().
   *
   * Более того, в режиме БЕЗ WRITEMAP грязные страницы выделяются через
   * malloc(), т.е. находятся вне mmap-диапазона и тогда чтобы отличить
   * действительно грязную страницу от указателя на данные пользователя
   * следует сканировать dirtylist, что накладно.
   *
   * Тем не менее, однозначно страница "не грязная" (не будет переписана
   * во время транзакции) если адрес находится внутри mmap-диапазона
   * и в заголовке страницы нет флажка P_DIRTY. */
  if (env->me_map < (uint8_t *)page) {
    const size_t usedbytes = pgno2bytes(env, txn->mt_next_pgno);
    if ((uint8_t *)page < env->me_map + usedbytes) {
      /* страница внутри диапазона, смотрим на флажки */
      return (page->mp_flags16 & (P_DIRTY | P_LOOSE | P_KEEP)) ? MDBX_SIGN : MDBX_SUCCESS;
    }
    /* Гипотетически здесь возможна ситуация, когда указатель адресует что-то
     * в пределах mmap, но за границей распределенных страниц. Это тяжелая
     * ошибка, к которой не возможно прийти без каких-то больших нарушений.
     * Поэтому не проверяем этот случай кроме как assert-ом, на то что
     * страница вне mmap-диаппазона. */
    assert((uint8_t *)page >= env->me_map + env->me_mapsize);
  }

  /* Страница вне используемого mmap-диапазона, т.е. либо в функцию был
   * передан некорректный адрес, либо адрес в теневой странице, которая была
   * выделена посредством malloc().
   *
   * Для WRITE_MAP режима такая страница однозначно "не грязная",
   * а для режимов без WRITE_MAP следует просматривать списки dirty
   * и spilled страниц у каких-либо транзакций (в том числе дочерних).
   *
   * Поэтому для WRITE_MAP возвращаем false, а для остальных режимов
   * всегда true. Такая логика имеет ряд преимуществ:
   *  - не тратим время на просмотр списков;
   *  - результат всегда безопасен (может быть ложно-положительным,
   *    но не ложно-отрицательным);
   *  - результат не зависит от вложенности транзакций и от относительного
   *    положения переданной транзакции в этой рекурсии. */
  return (env->me_flags32 & MDBX_WRITEMAP) ? MDBX_SUCCESS : MDBX_SIGN;
}

/*----------------------------------------------------------------------------*/

/* Dump a key in ascii or hexadecimal. */
char *mdbx_dump_iov(const MDBX_iov_t *iov, char *const buf, const size_t bufsize) {
  if (!iov)
    return "<null>";
  if (!buf || bufsize < 4)
    return nullptr;
  if (!iov->iov_len)
    return "<empty>";

  const uint8_t *const data = iov->iov_base;
  bool is_ascii = true;
  unsigned i;
  for (i = 0; is_ascii && i < iov->iov_len; i++)
    if (data[i] < ' ' || data[i] > 127)
      is_ascii = false;

  if (is_ascii) {
    int len = snprintf(buf, bufsize, "%.*s", (iov->iov_len > INT_MAX) ? INT_MAX : (int)iov->iov_len, data);
    assert(len > 0 && (unsigned)len < bufsize);
    (void)len;
  } else {
    char *const detent = buf + bufsize - 2;
    char *ptr = buf;
    *ptr++ = '<';
    for (i = 0; i < iov->iov_len; i++) {
      const ptrdiff_t left = detent - ptr;
      assert(left > 0);
      int len = snprintf(ptr, left, "%02x", data[i]);
      if (len < 0 || len >= left)
        break;
      ptr += len;
    }
    if (ptr < detent) {
      ptr[0] = '>';
      ptr[1] = '\0';
    }
  }
  return buf;
}

/*----------------------------------------------------------------------------*/
/* attribute support functions for Nexenta */

static MDBX_error_t mdbx_attr_peek(MDBX_iov_t *data, MDBX_attr_t *attrptr) {
  if (unlikely(data->iov_len < sizeof(MDBX_attr_t)))
    return MDBX_INCOMPATIBLE;

  if (likely(attrptr != nullptr))
    *attrptr = *(MDBX_attr_t *)data->iov_base;
  data->iov_len -= sizeof(MDBX_attr_t);
  data->iov_base = likely(data->iov_len > 0) ? ((MDBX_attr_t *)data->iov_base) + 1 : nullptr;

  return MDBX_SUCCESS;
}

static MDBX_error_t mdbx_attr_poke(MDBX_iov_t *reserved, MDBX_iov_t *data, MDBX_attr_t attr, unsigned flags) {
  MDBX_attr_t *space = reserved->iov_base;
  if (flags & MDBX_IUD_RESERVE) {
    if (likely(data != nullptr)) {
      data->iov_base = data->iov_len ? space + 1 : nullptr;
    }
  } else {
    *space = attr;
    if (likely(data != nullptr)) {
      memcpy(space + 1, data->iov_base, data->iov_len);
    }
  }

  return MDBX_SUCCESS;
}

MDBX_error_t mdbx_cursor_get_attr(MDBX_cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_attr_t *attrptr,
                                  MDBX_cursor_op_t op) {
  MDBX_error_t err = mdbx_cursor_get(mc, key, data, op);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  return mdbx_attr_peek(data, attrptr);
}

MDBX_error_t mdbx_get_attr(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                           uint64_t *attrptr) {
  MDBX_error_t err = mdbx_get(txn, aah, key, data);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  return mdbx_attr_peek(data, attrptr);
}

MDBX_error_t mdbx_put_attr(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                           MDBX_attr_t attr, mdbx_iud_flags_t flags) {
  MDBX_iov_t reserve = {nullptr, (data ? data->iov_len : 0) + sizeof(MDBX_attr_t)};
  MDBX_error_t err = mdbx_put(txn, aah, key, &reserve, flags | MDBX_IUD_RESERVE);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  return mdbx_attr_poke(&reserve, data, attr, flags);
}

MDBX_error_t mdbx_cursor_put_attr(MDBX_cursor_t *cursor, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_attr_t attr,
                                  mdbx_iud_flags_t flags) {
  MDBX_iov_t reserve = {nullptr, (data ? data->iov_len : 0) + sizeof(MDBX_attr_t)};
  MDBX_error_t err = mdbx_cursor_put(cursor, key, &reserve, flags | MDBX_IUD_RESERVE);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  return mdbx_attr_poke(&reserve, data, attr, flags);
}

MDBX_error_t mdbx_set_attr(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                           MDBX_attr_t attr) {
  if (unlikely(!key))
    return MDBX_EINVAL;

  MDBX_error_t err = validate_txn_rw(txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  MDBX_cursor_t mc;
  err = cursor_init(&mc, txn, rp.aht);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  MDBX_iov_t old_data;
  err = cursor_set(&mc.primal, key, &old_data, MDBX_SET, nullptr);
  if (unlikely(err != MDBX_SUCCESS)) {
    if (err == MDBX_NOTFOUND && data) {
      MDBX_cursor_t **const head = cursor_tracking_head(&mc);
      mc.mc_next = *head;
      *head = &mc;
      err = mdbx_cursor_put_attr(&mc, key, data, attr, 0);
      *head = mc.mc_next;
    }
    return err;
  }

  MDBX_attr_t old_attr = 0;
  err = mdbx_attr_peek(&old_data, &old_attr);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (old_attr == attr && (!data || mdbx_iov_eq(data, &old_data)))
    return MDBX_SUCCESS;

  MDBX_cursor_t **const head = cursor_tracking_head(&mc);
  mc.mc_next = *head;
  *head = &mc;
  assert(!mdbx_is_dirty(txn, old_data.iov_base));
  err = mdbx_cursor_put_attr(&mc, key, data ? data : &old_data, attr, MDBX_IUD_CURRENT);
  *head = mc.mc_next;
  return err;
}

/*----------------------------------------------------------------------------*/

MDBX_error_t __cold mdbx_shutdown(MDBX_env_t *env) { return mdbx_shutdown_ex(env, MDBX_shutdown_default); }

MDBX_error_t mdbx_shutdown_ex(MDBX_env_t *env, MDBX_shutdown_mode_t mode) {
  MDBX_error_t err = validate_env(env, true);
  if (likely(err == MDBX_SUCCESS)) {
    err = env_shutdown(env, mode);
    env_destroy(env);
    set_signature(&env->me_signature, ~0u);
    env->me_pid = 0;
    free(env);
  }
  return err;
}

MDBX_error_t __cold mdbx_set_rbr(MDBX_env_t *env, MDBX_rbr_callback_t *cb) {
  MDBX_error_t err = validate_env(env, true);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  env->me_callback_rbr = cb;
  return MDBX_SUCCESS;
}

MDBX_error_t __cold mdbx_set_syncbytes(MDBX_env_t *env, size_t bytes) {
  MDBX_error_t err = validate_env(env, true);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (unlikely(!env->me_lck))
    return MDBX_EPERM;

  env->me_lck->li_autosync_threshold = (bytes < UINT32_MAX) ? (uint32_t)bytes : UINT32_MAX;
  if (env->me_lck->li_dirty_volume >= env->me_lck->li_autosync_threshold)
    return mdbx_sync(env);

  return MDBX_SUCCESS;
}

MDBX_numeric_result_t __cold mdbx_readers_check(MDBX_env_t *env) {
  MDBX_numeric_result_t result;
  result.value = 0;
  result.err = validate_env(env, true);
  if (likely(result.err == MDBX_SUCCESS))
    result = check_registered_readers(env, false);
  return result;
}

MDBX_error_t __cold mdbx_readers_enum(MDBX_env_t *env, MDBX_readers_enum_func_t *func, void *ctx) {
  MDBX_error_t err = validate_env(env, true);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (unlikely(!env->me_lck))
    return MDBX_EPERM;

  if (unlikely(!func))
    return MDBX_EINVAL;

  const MDBX_lockinfo_t *const lck = env->me_lck;
  for (unsigned i = 0; likely(err == MDBX_SUCCESS) && i < lck->li_numreaders; ++i) {
    const MDBX_reader_t *const slot = &lck->li_readers[i];
    while (1) {
      const MDBX_pid_t pid = slot->mr_pid;
      if (pid == 0)
        break;

      const MDBX_tid_t tid = slot->mr_tid;
      const txnid_t txnid = slot->mr_txnid;
      if (likely(pid == slot->mr_pid && tid == slot->mr_tid && txnid == slot->mr_txnid)) {
        err = func(ctx, i, pid, tid, txnid);
        break;
      }
    }
  }

  return err;
}

MDBX_error_t __cold mdbx_init(MDBX_env_t **pbk) { return mdbx_init_ex(pbk, nullptr, nullptr); }

MDBX_error_t __cold mdbx_init_ex(MDBX_env_t **pbk, void *user_ctx, MDBX_ops_t *ops) {
  if (unlikely(!pbk))
    return MDBX_EINVAL;
  *pbk = nullptr;

  if (unlikely(!is_power_of_2(osal_syspagesize) || osal_syspagesize < MIN_PAGESIZE)) {
    mdbx_error("unsuitable system pagesize %u", osal_syspagesize);
    return MDBX_INCOMPATIBLE;
  }

  if (osal_cacheline_size > MDBX_CACHELINE_SIZE) {
    mdbx_error("unsuitable shared cacheline size %u, expect %u or less", osal_cacheline_size,
               MDBX_CACHELINE_SIZE);
    return MDBX_INCOMPATIBLE;
  }

  static void *(*calloc4use)(size_t nmemb, size_t size, MDBX_env_t * env) = mdbx_calloc;
  if (ops) {
    const bool all_mem_ops_null = !ops->memory.ops_malloc && !ops->memory.ops_free &&
                                  !ops->memory.ops_calloc && !ops->memory.ops_realloc &&
                                  !ops->memory.ops_aligned_alloc && !ops->memory.ops_aligned_free;

    const bool all_mem_ops_NOTnull = ops->memory.ops_malloc && ops->memory.ops_free &&
                                     ops->memory.ops_calloc && ops->memory.ops_realloc &&
                                     ops->memory.ops_aligned_alloc && ops->memory.ops_aligned_free;

    if (unlikely(!(all_mem_ops_null || all_mem_ops_NOTnull)))
      return MDBX_EINVAL;

    const bool all_lck_ops_null =
        !ops->locking.ops_writer_lock && !ops->locking.ops_writer_unlock && !ops->locking.ops_init &&
        !ops->locking.ops_seize && !ops->locking.ops_downgrade && !ops->locking.ops_upgrade &&
        !ops->locking.ops_detach && !ops->locking.ops_reader_registration_lock &&
        !ops->locking.ops_reader_registration_unlock && !ops->locking.ops_reader_alive_set &&
        !ops->locking.ops_reader_alive_clear && !ops->locking.ops_reader_alive_check;

    const bool all_lck_ops_NOTnull =
        ops->locking.ops_writer_lock && ops->locking.ops_writer_unlock && ops->locking.ops_init &&
        ops->locking.ops_seize && ops->locking.ops_downgrade && ops->locking.ops_upgrade &&
        ops->locking.ops_detach && ops->locking.ops_reader_registration_lock &&
        ops->locking.ops_reader_registration_unlock && ops->locking.ops_reader_alive_set &&
        ops->locking.ops_reader_alive_clear && ops->locking.ops_reader_alive_check;

    if (unlikely(!(all_lck_ops_null || all_lck_ops_NOTnull)))
      return MDBX_EINVAL;

    if (ops->memory.ops_calloc)
      calloc4use = ops->memory.ops_calloc;
  }

  MDXB_env_base_t fake_env;
  memset(&fake_env, 0, sizeof(fake_env));
  fake_env.userctx = user_ctx;
  MDBX_env_t *env = (MDBX_env_t *)calloc4use(1, sizeof(MDBX_env_t), (MDBX_env_t *)&fake_env);
  if (unlikely(!env))
    return MDBX_ENOMEM;

  env->me_userctx = user_ctx;
  env->me_pid = mdbx_getpid();

  env->ops.memory.ops_malloc = mdbx_malloc;
  env->ops.memory.ops_free = mdbx_free;
  env->ops.memory.ops_calloc = mdbx_calloc;
  env->ops.memory.ops_realloc = mdbx_realloc;
  env->ops.memory.ops_aligned_alloc = mdbx_aligned_alloc;
  env->ops.memory.ops_aligned_free = mdbx_aligned_free;
  if (ops && ops->memory.ops_malloc)
    env->ops.memory = ops->memory;

  env->ops.locking.ops_init = mdbx_lck_init;
  env->ops.locking.ops_seize = mdbx_lck_seize;
  env->ops.locking.ops_downgrade = mdbx_lck_downgrade;
  env->ops.locking.ops_upgrade = mdbx_lck_upgrade;
  env->ops.locking.ops_detach = mdbx_lck_detach;
  env->ops.locking.ops_reader_registration_lock = mdbx_lck_reader_registration_lock;
  env->ops.locking.ops_reader_registration_unlock = mdbx_lck_reader_registration_unlock;
  env->ops.locking.ops_reader_alive_set = mdbx_lck_reader_alive_set;
  env->ops.locking.ops_reader_alive_clear = mdbx_lck_reader_alive_clear;
  env->ops.locking.ops_reader_alive_check = mdbx_lck_reader_alive_check;
  env->ops.locking.ops_writer_lock = mdbx_lck_writer_lock;
  env->ops.locking.ops_writer_unlock = mdbx_lck_writer_unlock;
  if (ops && ops->locking.ops_init)
    env->ops.locking = ops->locking;

  env->me_maxreaders = DEFAULT_READERS;
  env->env_ah_max = env->env_ah_num = CORE_AAH;
  env->me_dxb_fd = MDBX_INVALID_FD;
  env->me_lck_fd = MDBX_INVALID_FD;
  setup_pagesize(env, osal_syspagesize);

  MDBX_error_t err = mdbx_fastmutex_init(&env->me_aah_lock);
  if (unlikely(err != MDBX_SUCCESS))
    goto bailout;

#if defined(_WIN32) || defined(_WIN64)
  InitializeSRWLock(&env->me_remap_guard);
  InitializeCriticalSection(&env->me_windowsbug_lock);
#else
  err = mdbx_fastmutex_init(&env->me_remap_guard);
  if (unlikely(err != MDBX_SUCCESS)) {
    mdbx_fastmutex_destroy(&env->me_aah_lock);
    goto bailout;
  }
#endif /* Windows */

  VALGRIND_CREATE_MEMPOOL(env, 0, 0);
  set_signature(&env->me_signature, MDBX_ME_SIGNATURE);
  *pbk = env;
  return MDBX_SUCCESS;

bailout:
  env->ops.memory.ops_free(env, env);
  return err;
}

/*----------------------------------------------------------------------------*/

MDBX_env_result_t __cold mdbx_txn_env(MDBX_txn_t *txn) {
  MDBX_env_result_t result = {nullptr, validate_txn_ro(txn)};
  if (likely(result.err == MDBX_SUCCESS))
    result.env = txn->mt_env;
  return result;
}

MDBX_txnid_result_t __cold mdbx_txnid(MDBX_txn_t *txn) {
  MDBX_txnid_result_t result = {0, validate_txn_ro(txn)};
  if (likely(result.err == MDBX_SUCCESS))
    result.txnid = txn->mt_txnid;
  return result;
}

MDBX_userctx_result_t __cold mdbx_set_userctx(MDBX_env_t *env, void *ctx) {
  MDBX_userctx_result_t result = {nullptr, validate_env(env, true)};
  if (likely(result.err == MDBX_SUCCESS)) {
    result.userctx = env->me_userctx;
    env->me_userctx = ctx;
  }
  return result;
}

MDBX_userctx_result_t __cold mdbx_get_userctx(MDBX_env_t *env) {
  MDBX_userctx_result_t result = {nullptr, validate_env(env, true)};
  if (likely(result.err == MDBX_SUCCESS))
    result.userctx = env->me_userctx;
  return result;
}

MDBX_error_t mdbx_set_loglevel(MDBX_debuglog_subsystem_t subsystem, MDBX_debuglog_level_t level) {
  STATIC_ASSERT(MDBX_LOG_ALL < MDBX_LOG_MISC && MDXB_LOG_MAX > MDBX_LOG_ALL);
  if (unlikely(subsystem < MDBX_LOG_ALL || subsystem >= MDXB_LOG_MAX))
    return MDBX_EINVAL;
  if (unlikely(level < MDBX_LOGLEVEL_EXTRA || level > MDBX_LOGLEVEL_FATAL))
    return MDBX_EINVAL;

#if MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_DBG_LOGGING
  if (subsystem < 0) {
    for (MDBX_debuglog_subsystem_t i = 0; i < MDXB_LOG_MAX; ++i)
      mdbx_dbglog_levels[i] = level;
  } else {
    mdbx_dbglog_levels[subsystem] = level;
  }
  return MDBX_SUCCESS;
#else
  return MDBX_ENOSYS;
#endif
}

MDBX_debug_result_t __cold mdbx_set_debug(MDBX_debugbits_t bits, MDBX_debuglog_callback_t *logger) {
  const MDBX_debug_result_t result = {mdbx_debug_logger, mdbx_debug_bits};
  mdbx_debug_logger = logger;
  bits = (bits | MDBX_CONFIG_DBG_LOGGING | MDBX_CONFIG_DBG_VALGRIND) & MDBX_CONFIGURED_DEBUG_ABILITIES;

#ifdef __linux__
  if (bits & MDBX_DBG_DUMP) {
    const int core_filter_fd = open("/proc/self/coredump_filter", O_TRUNC | O_RDWR);
    if (core_filter_fd >= 0) {
      char buf[32];
      const ssize_t r = pread(core_filter_fd, buf, sizeof(buf), 0);
      if (r > 0 && sizeof(buf) > (size_t)r) {
        buf[r] = 0;
        unsigned long mask = strtoul(buf, nullptr, 16);
        if (mask != ULONG_MAX) {
          mask |= 1 << 3 /* Dump file-backed shared mappings */;
          mask |= 1 << 6 /* Dump shared huge pages */;
          mask |= 1 << 8 /* Dump shared DAX pages */;
          ssize_t w = snprintf(buf, sizeof(buf), "0x%lx\n", mask);
          if (w > 0 && sizeof(buf) > (size_t)w) {
            w = pwrite(core_filter_fd, buf, (size_t)w, 0);
            (void)w;
          }
        }
      }
      close(core_filter_fd);
    }
  }
  bits |= MDBX_DBG_DUMP & mdbx_debug_bits;
#else
  bits &= ~MDBX_DBG_DUMP;
#endif /* __linux__ */

  mdbx_debug_bits = bits;
  return result;
}

MDBX_error_t __cold mdbx_set_assert(MDBX_env_t *env, MDBX_assert_callback_t *func) {
  MDBX_error_t rc = validate_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

#if MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_DBG_ASSERTIONS
  env->me_assert_func = func;
  return MDBX_SUCCESS;
#else
  (void)func;
  return MDBX_ENOSYS;
#endif
}
