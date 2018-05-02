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

/* Some platforms define the EOWNERDEAD error code
 * even though they don't support Robust Mutexes.
 * Compile with -DMDBX_USE_ROBUST=0. */
#ifndef MDBX_USE_ROBUST
#if defined(EOWNERDEAD) &&                                                                                    \
    __GLIBC_PREREQ(2, 10) /* LY: glibc before 2.10 has a troubles with Robust Mutex too. */
#define MDBX_USE_ROBUST 1
#else
#define MDBX_USE_ROBUST 0
#endif
#endif /* MDBX_USE_ROBUST */

#define LCK_DXB_WHOLE MAX_MAPSIZE

static int fcntl_op(const MDBX_filehandle_t fd, const int op, const int lck, const size_t offset,
                    const size_t len) {
  STATIC_ASSERT(sizeof(off_t) >= sizeof(size_t));
  for (;;) {
    mdbx_jitter4testing(true);
    struct flock lock_op;
    memset(&lock_op, 0, sizeof(lock_op));
    lock_op.l_type = lck;
    lock_op.l_whence = SEEK_SET;
    lock_op.l_start = offset;
    lock_op.l_len = len;
    int rc = fcntl(fd, op, &lock_op);
    mdbx_jitter4testing(true);
    if (rc == -1) {
      rc = errno;
      if (rc != EINTR) {
        if (rc != EACCES && rc != EAGAIN)
          lck_error("fcntl(fd %d, op %d, lck %d, offset %zu, len %zu), error %d", fd, op, lck, offset, len,
                    rc);
        return rc /* EACCES, EAGAIN */;
      }
      continue;
    } else if (rc == 0) {
      if (op == F_GETLK && lock_op.l_type != F_UNLCK)
        rc = -lock_op.l_pid;
    }
    return rc;
  }
}

static inline int lck_exclusive(int fd, const MDBX_flags_t flags) {
  assert(fd != MDBX_INVALID_FD);
  mdbx_jitter4testing(true);
  while (flock(fd, (flags & MDBX_NONBLOCK) ? LOCK_EX | LOCK_NB : LOCK_EX)) {
    int rc = errno;
    if (rc != EINTR) {
      if (rc != EWOULDBLOCK)
        lck_error("flock(fd %d, LOCK_EX%s), error %d", fd, (flags & MDBX_NONBLOCK) ? "|LOCK_NB" : "", rc);
      return rc /* EWOULDBLOCK */;
    }
    mdbx_jitter4testing(true);
  }

  /* exlusive lock first byte */
  return fcntl_op(fd, (flags & MDBX_NONBLOCK) ? F_SETLK : F_SETLKW, F_WRLCK, 0, 1);
}

static inline int lck_shared(int fd, const MDBX_flags_t flags) {
  assert(fd != MDBX_INVALID_FD);
  mdbx_jitter4testing(true);
  while (flock(fd, (flags & MDBX_NONBLOCK) ? LOCK_SH | LOCK_NB : LOCK_SH)) {
    int rc = errno;
    if (rc != EINTR) {
      if (rc != EWOULDBLOCK)
        lck_error("flock(fd %d, LOCK_SH%s), error %d", fd, (flags & MDBX_NONBLOCK) ? "|LOCK_NB" : "", rc);
      return rc /* EWOULDBLOCK */;
    }
    mdbx_jitter4testing(true);
  }

  /* shared lock first byte */
  return fcntl_op(fd, (flags & MDBX_NONBLOCK) ? F_SETLK : F_SETLKW, F_RDLCK, 0, 1);
}

static inline bool lck_is_collision(int err) {
  return err == EAGAIN || err == EACCES || (EWOULDBLOCK != EAGAIN && err == EWOULDBLOCK);
}

/*----------------------------------------------------------------------------*/

MDBX_error_t mdbx_lck_downgrade(MDBX_env_t *env) {
  lck_trace(">>");
  int err = lck_shared(env->me_lck_fd, MDBX_NONBLOCK);
  if (env->me_wpa_txn)
    mdbx_lck_writer_unlock(env);

  if (unlikely(err != MDBX_SUCCESS)) {
    lck_trace("<< %s, %d", lck_is_collision(err) ? "busy" : "error", err);
  } else {
    lck_trace("<< ok");
  }
  return err;
}

MDBX_error_t mdbx_lck_upgrade(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_trace(">> flags 0x%x%s", flags, (flags & MDBX_NONBLOCK) ? " (MDBX_NONBLOCK)" : "");
  /* lck_upgrade MUST returns:
   *   MDBX_SUCCESS - exclusive lock acquired,
   *                  NO other processes use DB.
   *   MDBX_BUSY    - exclusive lock NOT acquired, other processes use DB,
   *                  but not known readers, writers or mixed.
   *   MDBX_SIGN    - exclusive lock NOT acquired and SURE known that
   *                  other writer(s) is present, i.e. db-sync not required on close */
  int err = mdbx_lck_writer_lock(env, flags);
  if (err == EBUSY && (flags & MDBX_NONBLOCK)) {
    /* Other process running a write-txn */
    lck_trace("<< MDBX_SIGN");
    return MDBX_SIGN;
  }
  if (unlikely(MDBX_IS_ERROR(err))) {
    lck_trace("<< ERROR");
    return err;
  }

  err = lck_exclusive(env->me_lck_fd, flags);
  mdbx_jitter4testing(true);
  if (err != MDBX_SUCCESS) {
    mdbx_lck_writer_unlock(env);
    if (lck_is_collision(err)) {
      /* TODO: return MDBX_SIGN if at least one writer is present */
      lck_trace("<< MDBX_EBUSY");
      return MDBX_EBUSY;
    }
    lck_trace("<< ERROR");
    return err;
  }

  lck_trace("<< MDBX_SUCCESS");
  return MDBX_SUCCESS;
}

MDBX_error_t mdbx_lck_reader_alive_set(MDBX_env_t *env, MDBX_pid_t pid) {
  int err = fcntl_op(env->me_lck_fd, F_SETLK, F_WRLCK, pid, 1);
  lck_trace("pid %ld, err %d", (long)pid, err);
  return err;
}

MDBX_error_t mdbx_lck_reader_alive_clear(MDBX_env_t *env, MDBX_pid_t pid) {
  int err = fcntl_op(env->me_lck_fd, F_SETLKW, F_UNLCK, pid, 1);
  lck_trace("pid %ld, err %d", (long)pid, err);
  return err;
}

/* Checks reader by pid.
 * Returns:
 *   MDBX_SUCCESS, if pid is live (unable to acquire lock)
 *   MDBX_SIGN, if pid is dead (lock acquired) or otherwise the errcode. */
MDBX_error_t mdbx_lck_reader_alive_check(MDBX_env_t *env, MDBX_pid_t pid) {
  lck_trace(">> pid %ld", (long)pid);
  int rc = fcntl_op(env->me_lck_fd, F_GETLK, F_WRLCK, pid, 1);
  if (rc == 0) {
    lck_trace("<< pid %ld, %s", (long)pid, "MDBX_SIGN (seem invalid)");
    return MDBX_SIGN;
  }
  if (rc < 0 && -rc == pid) {
    lck_trace("<< pid %ld, %s", (long)pid, "MDBX_SUCCESS (running)");
    return MDBX_SUCCESS;
  }
  lck_error("<< pid %ld, failure %d", (long)pid, rc);
  return rc;
}

/*----------------------------------------------------------------------------*/

MDBX_error_t mdbx_lck_init(MDBX_env_t *env) {
  pthread_mutexattr_t ma;
  int err = pthread_mutexattr_init(&ma);
  if (err)
    return err;

  err = pthread_mutexattr_setpshared(&ma, PTHREAD_PROCESS_SHARED);
  if (err)
    goto bailout;

#if MDBX_USE_ROBUST
#if __GLIBC_PREREQ(2, 12)
  err = pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
#else
  err = pthread_mutexattr_setrobust_np(&ma, PTHREAD_MUTEX_ROBUST_NP);
#endif
  if (err)
    goto bailout;
#endif /* MDBX_USE_ROBUST */

#if _POSIX_C_SOURCE >= 199506L
  err = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_INHERIT);
  if (err == ENOTSUP)
    err = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_NONE);
  if (err)
    goto bailout;
#endif /* PTHREAD_PRIO_INHERIT */

  err = pthread_mutex_init(&env->me_lck->li_rmutex, &ma);
  if (err)
    goto bailout;
  err = pthread_mutex_init(&env->me_lck->li_wmutex, &ma);

bailout:
  pthread_mutexattr_destroy(&ma);
  return err;
}

void mdbx_lck_detach(MDBX_env_t *env) {
  if (env->me_lck_fd != MDBX_INVALID_FD) {
    /* try get exclusive access */
    if (env->me_lck && lck_exclusive(env->me_lck_fd, MDBX_NONBLOCK) == 0) {
      lck_info("%s: got exclusive, drown mutexes", __func__);
      int rc = pthread_mutex_destroy(&env->me_lck->li_rmutex);
      assert(rc == 0);
      rc = pthread_mutex_destroy(&env->me_lck->li_wmutex);
      assert(rc == 0);
      (void)rc;
    }
    /* files locks would be released (by kernel) while the me_lfd will be closed */
  }
}

MDBX_seize_result_t mdbx_lck_seize(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_EXCLUSIVE, MDBX_NONBLOCK */) {
  lck_trace(">> flags 0x%x%s", flags, (flags & MDBX_NONBLOCK) ? " (MDBX_NONBLOCK)" : "");
  assert(env->me_dxb_fd != MDBX_INVALID_FD);

  if (env->me_lck_fd == MDBX_INVALID_FD) {
    /* LY: without-lck mode (e.g. on read-only filesystem) */
    int err = fcntl_op(env->me_dxb_fd, F_SETLK, F_RDLCK, 0, LCK_DXB_WHOLE);
    if (err != 0) {
      lck_trace("<< without-lck.lock %s, %d", lck_is_collision(err) ? "busy" : "error", err);
      return seize_failed(err);
    }
    lck_trace("<< %s", "SEIZE_NOLCK");
    return seize_done(MDBX_SEIZE_NOLCK);
  }

  if ((env->me_flags32 & MDBX_RDONLY) == 0) {
    /* Check that another process don't operates in without-lck mode. */
    int err = fcntl_op(env->me_dxb_fd, F_SETLK, F_WRLCK, env->me_pid, 1);
    if (err != 0) {
      lck_trace("<< against-without-lck.lock %s, %d", lck_is_collision(err) ? "busy" : "error", err);
      return seize_failed(err);
    }
  }

  assert(env->me_lck_fd != MDBX_INVALID_FD);
  /* try exclusive access for first-time initialization */
  int err = lck_exclusive(env->me_lck_fd, ((flags & (MDBX_EXCLUSIVE | MDBX_NONBLOCK)) == MDBX_EXCLUSIVE)
                                              ? flags /* wait for exclusive if requested */
                                              : MDBX_NONBLOCK /* avoid non-necessary blocking */);
  if (err == 0) {
    /* got exclusive */
    lck_trace("<< %s", "MDBX_SEIZE_EXCLUSIVE_FIRST");
    return seize_done(MDBX_SEIZE_EXCLUSIVE_FIRST);
  }

  if (lck_is_collision(err)) {
    /* get shared access */
    err = lck_shared(env->me_lck_fd, flags);
    if (err == 0) {
      /* got shared */
      if (!MDBX_OPT_TEND_LCK_RESET && (flags & MDBX_EXCLUSIVE) == 0) {
        lck_trace("<< %s", "MDBX_SEIZE_SHARED");
        return seize_done(MDBX_SEIZE_SHARED);
      }

      lck_trace("got shared, try exclusive again");
      err = lck_exclusive(env->me_lck_fd, MDBX_NONBLOCK /* avoid non-necessary blocking */);
      if (err == 0) {
        /* now got exclusive */
        lck_trace("<< %s", "MDBX_SEIZE_EXCLUSIVE_LIVE");
        return seize_done(MDBX_SEIZE_EXCLUSIVE_CONTINUE);
      }

      if (lck_is_collision(err)) {
        /* unable exclusive, but stay shared */
        lck_trace("<< %s", "MDBX_SEIZE_SHARED");
        return seize_done(MDBX_SEIZE_SHARED);
      }
    }
  }

  lck_trace("<< ERROR");
  return seize_failed(err);
}

/*----------------------------------------------------------------------------*/

static int mdbx_robust_mutex_failed(MDBX_env_t *env, pthread_mutex_t *mutex, int err);

static inline int mdbx_robust_lock(MDBX_env_t *env, pthread_mutex_t *mutex,
                                   MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  int rc = (flags & MDBX_NONBLOCK) ? pthread_mutex_trylock(mutex) : pthread_mutex_lock(mutex);
  STATIC_ASSERT(EBUSY == MDBX_EBUSY);
  if (unlikely(rc != 0 && rc != EBUSY))
    rc = mdbx_robust_mutex_failed(env, mutex, rc);
  return rc;
}

static inline int mdbx_robust_unlock(MDBX_env_t *env, pthread_mutex_t *mutex) {
  int rc = pthread_mutex_unlock(mutex);
  if (unlikely(rc != 0))
    rc = mdbx_robust_mutex_failed(env, mutex, rc);
  return rc;
}

MDBX_error_t mdbx_lck_reader_registration_lock(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_trace(">> flags 0x%x%s", flags, (flags & MDBX_NONBLOCK) ? " (MDBX_NONBLOCK)" : "");
  int rc = mdbx_robust_lock(env, &env->me_lck->li_rmutex, flags);
  if (unlikely(MDBX_IS_ERROR(rc))) {
    lck_trace("<< %s, %d", (rc == MDBX_EBUSY) ? "busy" : "error", rc);
  } else {
    lck_trace("<< ok%s", (rc == MDBX_SIGN) ? " (mutex recovered)" : "");
  }
  return rc;
}

void mdbx_lck_reader_registration_unlock(MDBX_env_t *env) {
  lck_trace(">>");
  int rc = mdbx_robust_unlock(env, &env->me_lck->li_rmutex);
  if (unlikely(MDBX_IS_ERROR(rc)))
    lck_panic(env, "robust-r.unlock", rc);
  lck_trace("<<");
}

MDBX_error_t mdbx_lck_writer_lock(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_trace(">> flags 0x%x%s", flags, (flags & MDBX_NONBLOCK) ? " (MDBX_NONBLOCK)" : "");
  assert(env->me_wpa_txn->mt_owner != mdbx_thread_self());
  int rc = mdbx_robust_lock(env, &env->me_lck->li_wmutex, flags);
  if (unlikely(MDBX_IS_ERROR(rc))) {
    lck_trace("<< %s, %d", (rc == MDBX_EBUSY) ? "busy" : "error", rc);
    return rc;
  } else {
    lck_trace("<< ok%s", (rc == MDBX_SIGN) ? " (mutex recovered)" : "");
    return MDBX_SUCCESS;
  }
}

void mdbx_lck_writer_unlock(MDBX_env_t *env) {
  lck_trace(">>");
  int rc = mdbx_robust_unlock(env, &env->me_lck->li_wmutex);
  if (unlikely(MDBX_IS_ERROR(rc)))
    lck_panic(env, "robust-w.unlock", rc);
  lck_trace("<<");
}

#if !__GLIBC_PREREQ(2, 12) && !defined(pthread_mutex_consistent)
#define pthread_mutex_consistent(mutex) pthread_mutex_consistent_np(mutex)
#endif

static int __cold mdbx_robust_mutex_failed(MDBX_env_t *env, pthread_mutex_t *mutex, int err) {
  assert(err != EBUSY);
#if MDBX_USE_ROBUST
  if (err == EOWNERDEAD /* We own the mutex. Clean up after dead previous owner */) {
    const bool rlocked = (mutex == &env->me_lck->li_rmutex);
    err = MDBX_SUCCESS;
    if (!rlocked) {
      if (unlikely(env->me_current_txn)) {
        /* env is hosed if the dead thread was ours */
        env->me_flags32 |= MDBX_ENV_TAINTED;
        env->me_current_txn = NULL;
        err = MDBX_PANIC;
      }
    }
    lck_notice("%c-mutex owner died, %s", (rlocked ? 'r' : 'w'),
               (err ? "this process' env is hosed" : "recovering"));

    int check_rc = check_registered_readers(env, rlocked).err;
    check_rc = (check_rc == MDBX_SUCCESS) ? MDBX_SIGN : check_rc;

    mdbx_jitter4testing(true);
    int mreco_rc = pthread_mutex_consistent(mutex);
    check_rc = (mreco_rc == 0) ? check_rc : mreco_rc;

    mdbx_jitter4testing(true);
    if (unlikely(mreco_rc))
      lck_error("mutex recovery failed, %s", mdbx_strerror(mreco_rc));

    err = (err == MDBX_SUCCESS) ? check_rc : err;
    if (MDBX_IS_ERROR(err)) {
      pthread_mutex_unlock(mutex);
    } else {
      if (rlocked)
        lck_reader_registration_set_owned(env);
      else
        lck_writer_set_owned(env);
    }

    mdbx_jitter4testing(true);
    return err;
  }
#else
  (void)mutex;
#endif /* MDBX_USE_ROBUST */

  lck_error("robust-mutex (un)lock failed, %s", mdbx_strerror(err));
  if (err != EDEADLK) {
    env->me_flags32 |= MDBX_ENV_TAINTED;
    err = MDBX_PANIC;
  }
  return err;
}
