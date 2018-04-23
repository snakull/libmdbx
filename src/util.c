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

// static uint64_t mdbx_iov_hash(const MDBX_iov_t iov) {
//  /* TODO: t1ha */
//  (void)iov;
//  return 0;
//}

MDBX_error_t mdbx_iov_dup(MDBX_iov_t *iov) {
  if (likely(iov->iov_len)) {
    void *dup = malloc(iov->iov_len);
    if (unlikely(!dup))
      return MDBX_ENOMEM;
    iov->iov_base = memcpy(dup, iov->iov_base, iov->iov_len);
  }
  return MDBX_SUCCESS;
}

static void mdbx_iov_free(MDBX_iov_t *iov) {
  if (likely(iov->iov_base != mdbx_droppped_name_stub)) {
    void *tmp = iov->iov_base;
    iov->iov_len = 0;
    iov->iov_base = (void *)mdbx_droppped_name_stub;
    paranoia_barrier();
    free(tmp);
  }
}

/*----------------------------------------------------------------------------*/

static inline void paranoia_barrier(void) { mdbx_compiler_barrier(); }

static inline bool is_power_of_2(size_t x) { return (x & (x - 1)) == 0; }

static inline size_t mdbx_roundup2(size_t value, size_t granularity) {
  assert(is_power_of_2(granularity));
  return (value + granularity - 1) & ~(granularity - 1);
}

static size_t mdbx_roundup_ptrsize(size_t value) { return mdbx_roundup2(value, sizeof(void *)); }

static size_t bytes_align2os_bytes(const MDBX_env_t *env, size_t bytes) {
  return mdbx_roundup2(mdbx_roundup2(bytes, env->me_psize), env->me_os_psize);
}

static unsigned uint_log2_ceil(size_t value) {
  assert(is_power_of_2(value));

  unsigned log = 0;
  while (value > 1) {
    log += 1;
    value >>= 1;
  }
  return log;
}

static inline void jitter4testing(bool tiny) {
#ifndef NDEBUG
  if (MDBX_DBG_JITTER & mdbx_debug_bits)
    mdbx_jitter(tiny);
#else
  (void)tiny;
#endif
}

static inline size_t pgno2bytes(const MDBX_env_t *env, pgno_t pgno) {
  mdbx_assert(env, (1u << env->me_psize2log) == env->me_psize);
  return ((size_t)pgno) << env->me_psize2log;
}

static inline page_t *pgno2page(const MDBX_env_t *env, pgno_t pgno) {
  return (page_t *)(env->me_map + pgno2bytes(env, pgno));
}

static inline pgno_t bytes2pgno(const MDBX_env_t *env, size_t bytes) {
  mdbx_assert(env, (env->me_psize >> env->me_psize2log) == 1);
  return (pgno_t)(bytes >> env->me_psize2log);
}

static inline size_t pgno_align2os_bytes(const MDBX_env_t *env, pgno_t pgno) {
  return mdbx_roundup2(pgno2bytes(env, pgno), env->me_os_psize);
}

static inline pgno_t pgno_align2os_pgno(const MDBX_env_t *env, pgno_t pgno) {
  return bytes2pgno(env, pgno_align2os_bytes(env, pgno));
}

static inline void set_pgno_aligned2(void *ptr, pgno_t pgno) {
  if (sizeof(pgno_t) == 4)
    set_le32_aligned2(ptr, (uint32_t)pgno);
  else
    set_le64_aligned2(ptr, (uint64_t)pgno);
}

static inline pgno_t get_pgno_aligned2(void *ptr) {
  if (sizeof(pgno_t) == 4)
    return (pgno_t)get_le32_aligned2(ptr);
  else
    return (pgno_t)get_le64_aligned2(ptr);
}

/*----------------------------------------------------------------------------*/
/* Validation functions */

static inline int validate_env(MDBX_env_t *env, bool check_pid) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (check_pid && unlikely(env->me_pid != mdbx_getpid()))
    env->me_flags32 |= MDBX_ENV_TAINTED;

  if (unlikely(env->me_flags32 & MDBX_ENV_TAINTED))
    return MDBX_PANIC;

  return MDBX_SUCCESS;
}

static inline int validate_cursor4close(const MDBX_cursor_t *cursor) {
  if (unlikely(!cursor))
    return MDBX_EINVAL;

  if (unlikely(cursor->mc_signature != MDBX_MC_SIGNATURE && cursor->mc_signature != MDBX_MC_READY4CLOSE))
    return MDBX_EBADSIGN;

  return MDBX_SUCCESS;
}

static inline int validate_cursor4operation_ro(const MDBX_cursor_t *cursor) {
  if (unlikely(!cursor))
    return MDBX_EINVAL;

  if (unlikely(cursor->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  return validate_txn_ro(cursor->primal.mc_txn);
}

static inline int validate_cursor4operation_rw(const MDBX_cursor_t *cursor) {
  int rc = validate_cursor4operation_ro(cursor);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = validate_txn_more4rw(cursor->primal.mc_txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return MDBX_SUCCESS;
}
