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

#define lck_extra(fmt, ...) log_extra(MDBX_LOG_LCK, fmt, ##__VA_ARGS__)
#define lck_trace(fmt, ...) log_trace(MDBX_LOG_LCK, fmt, ##__VA_ARGS__)
#define lck_verbose(fmt, ...) log_verbose(MDBX_LOG_LCK, fmt, ##__VA_ARGS__)
#define lck_info(fmt, ...) log_info(MDBX_LOG_LCK, fmt, ##__VA_ARGS__)
#define lck_notice(fmt, ...) log_notice(MDBX_LOG_LCK, fmt, ##__VA_ARGS__)
#define lck_warning(fmt, ...) log_warning(MDBX_LOG_LCK, fmt, ##__VA_ARGS__)
#define lck_error(fmt, ...) log_error(MDBX_LOG_LCK, fmt, ##__VA_ARGS__)
#define lck_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_LCK, __func__, __LINE__, "%s, error %d", msg, err)

/*----------------------------------------------------------------------------*/

#if defined(_WIN32) || defined(_WIN64)
#include "lck-windows.c"
#else
#include "lck-posix.c"
#endif

/*----------------------------------------------------------------------------*/

static void lck_writer_ensure_owned(MDBX_env_t *env) {
  mdbx_ensure(env, env->me_lck->li_wowner_pid == env->me_pid);
  mdbx_ensure(env, env->me_lck->li_wowner_tid == mdbx_thread_self());
}

static void lck_writer_ensure_free(MDBX_env_t *env) {
  mdbx_ensure(env, env->me_lck->li_wowner_pid == 0);
  mdbx_ensure(env, env->me_lck->li_wowner_tid == 0);
}

MDBX_INTERNAL void lck_writer_set_owned(MDBX_env_t *env) {
  env->me_lck->li_wowner_pid = env->me_pid;
  env->me_lck->li_wowner_tid = mdbx_thread_self();
}

static void lck_writer_set_free(MDBX_env_t *env) {
  env->me_lck->li_wowner_pid = 0;
  env->me_lck->li_wowner_tid = 0;
}

MDBX_INTERNAL MDBX_error_t lck_writer_acquire(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  MDBX_error_t rc = env->ops.locking.ops_writer_lock(env, flags);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (env->me_lck) {
#if 0 /* LY: excessive, because (in general) previous owner could dead */
  lck_writer_ensure_free(env);
  if (unlikely(MDBX_DBG_JITTER & mdbx_debug_bits)) {
    mdbx_jitter(false);
    lck_writer_ensure_free(env);
  }
#endif

    lck_writer_set_owned(env);
    if (unlikely(MDBX_DBG_JITTER & mdbx_debug_bits)) {
      mdbx_jitter(false);
      lck_writer_ensure_owned(env);
    }
  }
  return MDBX_SUCCESS;
}

MDBX_INTERNAL void lck_writer_release(MDBX_env_t *env) {
  if (env->me_lck) {
    lck_writer_ensure_owned(env);
    lck_writer_set_free(env);
  }
  env->ops.locking.ops_writer_unlock(env);
}

/*----------------------------------------------------------------------------*/

static void lck_reader_registration_ensure_owned(MDBX_env_t *env) {
  mdbx_ensure(env, env->me_lck != NULL);
  mdbx_ensure(env, env->me_lck->li_rowner_pid == env->me_pid);
  mdbx_ensure(env, env->me_lck->li_rowner_tid == mdbx_thread_self());
}

static void lck_reader_registration_ensure_free(MDBX_env_t *env) {
  mdbx_ensure(env, (env->me_flags32 & MDBX_EXCLUSIVE) == 0 && env->me_lck != NULL);
  mdbx_ensure(env, env->me_lck->li_rowner_pid == 0);
  mdbx_ensure(env, env->me_lck->li_rowner_tid == 0);
}

MDBX_INTERNAL void lck_reader_registration_set_owned(MDBX_env_t *env) {
  mdbx_ensure(env, env->me_lck != NULL);
  env->me_lck->li_rowner_pid = env->me_pid;
  env->me_lck->li_rowner_tid = mdbx_thread_self();
}

static void lck_reader_registration_set_free(MDBX_env_t *env) {
  mdbx_ensure(env, (env->me_flags32 & MDBX_EXCLUSIVE) == 0 && env->me_lck != NULL);
  env->me_lck->li_rowner_pid = 0;
  env->me_lck->li_rowner_tid = 0;
}

MDBX_INTERNAL MDBX_error_t lck_reader_registration_acquire(MDBX_env_t *env,
                                                           MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  mdbx_assert(env, (env->me_flags32 & MDBX_EXCLUSIVE) == 0 && env->me_lck != NULL);
  MDBX_error_t rc = env->ops.locking.ops_reader_registration_lock(env, flags);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

#if 0 /* LY: excessive, because (in general) previous owner could dead */
  lck_reader_registration_ensure_free(env);
  if (unlikely(MDBX_DBG_JITTER & mdbx_debug_bits)) {
    mdbx_jitter(false);
    lck_reader_registration_ensure_free(env);
  }
#endif

  lck_reader_registration_set_owned(env);
  if (unlikely(MDBX_DBG_JITTER & mdbx_debug_bits)) {
    mdbx_jitter(false);
    lck_reader_registration_ensure_owned(env);
  }

  return MDBX_SUCCESS;
}

MDBX_INTERNAL void lck_reader_registration_release(MDBX_env_t *env) {
  lck_trace(">>");
  mdbx_assert(env, (env->me_flags32 & MDBX_EXCLUSIVE) == 0 && env->me_lck != NULL);
  lck_reader_registration_ensure_owned(env);
  lck_reader_registration_set_free(env);
  env->ops.locking.ops_reader_registration_unlock(env);
  lck_trace("<<");
}

MDBX_INTERNAL MDBX_error_t lck_reader_alive_set(MDBX_env_t *env, MDBX_pid_t pid) {
  lck_trace(">>");
  lck_reader_registration_ensure_owned(env);
  return env->ops.locking.ops_reader_alive_set(env, pid);
  lck_trace("<<");
}

MDBX_INTERNAL void lck_reader_alive_clear(MDBX_env_t *env, MDBX_pid_t pid) {
  lck_trace(">>");
  mdbx_assert(env, env->me_lck != NULL);
  int err = env->ops.locking.ops_reader_alive_clear(env, pid);
  if (unlikely(err != MDBX_SUCCESS))
    lck_warning("unexpected reader_alive_clear() error %d", err);
  lck_trace("<<");
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL void lck_seized_exclusive(MDBX_env_t *env) {
  lck_trace(">>");
  mdbx_assert(env, env->me_lck != NULL);
  lck_writer_set_owned(env);
  lck_reader_registration_set_owned(env);

  if (unlikely(MDBX_DBG_JITTER & mdbx_debug_bits)) {
    mdbx_jitter(false);
    lck_writer_ensure_owned(env);
    lck_reader_registration_ensure_owned(env);
  }
  lck_trace("<<");
}

__cold MDBX_INTERNAL MDBX_error_t lck_upgrade(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  mdbx_assert(env, (env->me_flags32 & MDBX_EXCLUSIVE) == 0 && env->me_lck);
  int rc = env->ops.locking.ops_upgrade(env, flags);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  lck_writer_set_free(env);
  lck_reader_registration_set_free(env);
  if (unlikely(MDBX_DBG_JITTER & mdbx_debug_bits)) {
    mdbx_jitter(false);
    lck_writer_ensure_free(env);
    lck_reader_registration_ensure_free(env);
  }

  lck_seized_exclusive(env);
  return MDBX_SUCCESS;
}

__cold MDBX_INTERNAL MDBX_error_t lck_downgrade(MDBX_env_t *env) {
  mdbx_assert(env, (env->me_flags32 & MDBX_EXCLUSIVE) == 0 && env->me_lck);
  lck_writer_ensure_owned(env);
  lck_reader_registration_ensure_owned(env);
  if (unlikely(MDBX_DBG_JITTER & mdbx_debug_bits)) {
    mdbx_jitter(false);
    lck_writer_ensure_owned(env);
    lck_reader_registration_ensure_owned(env);
  }

  lck_writer_set_free(env);
  lck_reader_registration_set_free(env);
  if (unlikely(MDBX_DBG_JITTER & mdbx_debug_bits)) {
    mdbx_jitter(false);
    lck_writer_ensure_free(env);
    lck_reader_registration_ensure_free(env);
  }

  int rc = env->ops.locking.ops_downgrade(env);
  if (unlikely(rc != MDBX_SUCCESS)) {
    lck_error("lck-downgrade: rc %i ", rc);
    return rc;
  }

  return MDBX_SUCCESS;
}
