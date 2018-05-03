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

/* PREAMBLE FOR WINDOWS:
 *
 * We are not concerned for performance here.
 * If you are running Windows a performance could NOT be the goal.
 * Otherwise please use Linux.
 *
 * Regards,
 * LY
 */

#define lck_transition_begin(from, to) lck_trace("now %s, going-down %s", from, to)
#define lck_transition_end(from, to, err)                                                                     \
  do {                                                                                                        \
    if (err == MDBX_SUCCESS)                                                                                  \
      lck_trace("done, now %s", to);                                                                          \
    else if (lck_is_collision(err))                                                                           \
      lck_trace("BUSY, still %s", from);                                                                      \
    else                                                                                                      \
      lck_error("ERROR %d, still %s", err, from);                                                             \
  } while (0)

static inline BOOL lck_lock(MDBX_filehandle_t fd, DWORD flags, uint64_t offset, size_t bytes) {
  mdbx_jitter4testing(true);
  OVERLAPPED ov;
  ov.hEvent = 0;
  ov.Offset = (DWORD)offset;
  ov.OffsetHigh = HIGH_DWORD(offset);
  const BOOL rc = LockFileEx(fd, flags, 0, (DWORD)bytes, HIGH_DWORD(bytes), &ov);
  mdbx_jitter4testing(true);
  return rc;
}

static inline BOOL lck_unlock(MDBX_filehandle_t fd, uint64_t offset, size_t bytes) {
  mdbx_jitter4testing(true);
  const BOOL rc = UnlockFile(fd, (DWORD)offset, HIGH_DWORD(offset), (DWORD)bytes, HIGH_DWORD(bytes));
  mdbx_jitter4testing(true);
  return rc;
}

static inline MDBX_error_t lck_exclusive(MDBX_filehandle_t fd, const MDBX_flags_t flags, uint64_t offset,
                                         size_t bytes) {
  return lck_lock(fd, (flags & MDBX_NONBLOCK) ? LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY
                                              : LOCKFILE_EXCLUSIVE_LOCK,
                  offset, bytes)
             ? MDBX_SUCCESS
             : GetLastError();
}

static inline MDBX_error_t lck_shared(MDBX_filehandle_t fd, const MDBX_flags_t flags, uint64_t offset,
                                      size_t bytes) {
  return lck_lock(fd, (flags & MDBX_NONBLOCK) ? LOCKFILE_FAIL_IMMEDIATELY : 0, offset, bytes) ? MDBX_SUCCESS
                                                                                              : GetLastError();
}

static inline BOOL lck_is_collision(MDBX_error_t err) {
  return (err == ERROR_LOCK_VIOLATION || err == ERROR_SHARING_VIOLATION);
}

/*----------------------------------------------------------------------------*/
/* global lock for write-transactions */

#define DXB_HEAD_OFFSET 0
#define DXB_HEAD_LEN (MDBX_NUM_METAS * (size_t)MAX_PAGESIZE)
#define DXB_BODY_OFFSET DXB_HEAD_LEN
#define DXB_BODY_LEN (MAX_MAPSIZE - DXB_BODY_OFFSET)
#define LCK_DXB_BODY DXB_BODY_OFFSET, DXB_BODY_LEN
#define LCK_DXB_WHOLE 0, MAX_MAPSIZE

MDBX_error_t mdbx_lck_writer_lock(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_trace(">> flags 0x%x %s", flags, (flags & MDBX_NONBLOCK) ? " (MDBX_NONBLOCK)" : "");
  if (flags & MDBX_NONBLOCK) {
    if (!TryEnterCriticalSection(&env->me_windowsbug_lock)) {
      lck_trace("<< MDBX_EBUSY");
      return MDBX_EBUSY;
    }
  } else {
    EnterCriticalSection(&env->me_windowsbug_lock);
  }

  const MDBX_error_t err = lck_exclusive(env->me_dxb_fd, flags, LCK_DXB_BODY);
  if (err == MDBX_SUCCESS) {
    lck_trace("<< MDBX_SUCCESS");
    return MDBX_SUCCESS;
  }

  LeaveCriticalSection(&env->me_windowsbug_lock);
  lck_trace("<< %s, %d", lck_is_collision(err) ? "busy" : "error", err);
  return err;
}

void mdbx_lck_writer_unlock(MDBX_env_t *env) {
  BOOL ok = lck_unlock(env->me_dxb_fd, LCK_DXB_BODY);
  if (!ok)
    lck_panic(env, "dxb-body.unlock", GetLastError());
  LeaveCriticalSection(&env->me_windowsbug_lock);
  lck_trace("<<");
}

/*----------------------------------------------------------------------------*/
/* thread suspend/resume for remapping */

static MDBX_error_t suspend_and_append_handle(mdbx_handle_array_t **array, const HANDLE hThread) {
  MDBX_error_t err = MDBX_ENOMEM;
  const unsigned limit = (*array)->limit;
  if ((*array)->count == limit) {
    void *ptr = realloc(
        (limit > ARRAY_LENGTH((*array)->handles)) ? *array : /* don't free initial array on the stack */ NULL,
        sizeof(mdbx_handle_array_t) + sizeof(HANDLE) * (limit * 2 - ARRAY_LENGTH((*array)->handles)));
    if (!ptr)
      goto bailout;
    (*array) = (mdbx_handle_array_t *)ptr;
    (*array)->limit = limit * 2;
  }

  if (SuspendThread(hThread) != (DWORD)-1) {
    (*array)->handles[(*array)->count++] = hThread;
    return MDBX_SUCCESS;
  }
  err = GetLastError();

bailout:
  CloseHandle(hThread);
  (void)mdbx_resume_threads_after_remap(*array);
  return err;
}

static MDBX_error_t suspend_and_append_tid(mdbx_handle_array_t **array, const DWORD ThreadId) {
  HANDLE hThread = OpenThread(ThreadId, FALSE, THREAD_SUSPEND_RESUME);
  if (hThread == NULL) {
    MDBX_error_t err = GetLastError();
    (void)mdbx_resume_threads_after_remap(*array);
    return err;
  }

  return suspend_and_append_handle(array, hThread);
}

MDBX_INTERNAL int ntstatus2errcode(NTSTATUS status);

#ifndef STATUS_NO_MORE_ENTRIES
#define STATUS_NO_MORE_ENTRIES 0x8000001A
#endif

extern NTSTATUS NTAPI NtGetNextThread(_In_ HANDLE ProcessHandle, _In_ HANDLE ThreadHandle,
                                      _In_ ACCESS_MASK DesiredAccess, _In_ ULONG HandleAttributes,
                                      _In_ ULONG Flags, _Out_ PHANDLE NewThreadHandle);

static MDBX_error_t mdbx_suspend_threads_before_remap(MDBX_env_t *env, mdbx_handle_array_t **array) {
  const MDBX_pid_t CurrentTid = GetCurrentThreadId();
  if (env->me_lck) {
    /* Scan LCK for threads of the current process */
    const MDBX_reader_t *const begin = env->me_lck->li_readers;
    const MDBX_reader_t *const end = begin + env->me_lck->li_numreaders;
    const MDBX_tid_t WriteTxnOwner = env->me_wpa_txn ? env->me_wpa_txn->mt_owner : 0;
    for (const MDBX_reader_t *reader = begin; reader < end; ++reader) {
      if (reader->mr_pid != env->me_pid || !reader->mr_tid) {
      skip_lck:
        continue;
      }
      if (reader->mr_tid == CurrentTid || reader->mr_tid == WriteTxnOwner)
        goto skip_lck;
      if (env->me_flags32 & MDBX_NOTLS) {
        /* Skip duplicates in no-tls mode */
        for (const MDBX_reader_t *scan = reader; --scan >= begin;)
          if (scan->mr_tid == reader->mr_tid)
            goto skip_lck;
      }

      MDBX_error_t err = suspend_and_append_tid(array, reader->mr_tid);
      if (err != MDBX_SUCCESS)
        return err;
    }

    if (WriteTxnOwner && WriteTxnOwner != CurrentTid) {
      MDBX_error_t err = suspend_and_append_tid(array, WriteTxnOwner);
      if (err != MDBX_SUCCESS)
        return err;
    }
  } else {
    /* Without LCK (i.e. read-only mode).
     * Walk thougth a snapshot of all running threads */
    mdbx_assert(env, env->me_wpa_txn == nullptr);
    HANDLE PrevThreadHandle = NULL;
    while (1) {
      HANDLE NextThreadHandle = INVALID_HANDLE_VALUE;
      NTSTATUS status =
          NtGetNextThread(INVALID_HANDLE_VALUE /* NtGetCurrentProcess() */, PrevThreadHandle,
                          THREAD_SUSPEND_RESUME | THREAD_QUERY_INFORMATION, 0, 0, &NextThreadHandle);
      if (status == STATUS_NO_MORE_ENTRIES)
        break;

      if (!NT_SUCCESS(status)) {
        (void)mdbx_resume_threads_after_remap(*array);
        MDBX_error_t err = ntstatus2errcode(status);
        return err;
      }

      if (GetThreadId(NextThreadHandle) != CurrentTid) {
        MDBX_error_t err = suspend_and_append_handle(array, NextThreadHandle);
        if (err != MDBX_SUCCESS)
          return err;
      }
      PrevThreadHandle = NextThreadHandle;
    }
  }

  return MDBX_SUCCESS;
}

static MDBX_error_t mdbx_resume_threads_after_remap(mdbx_handle_array_t *array) {
  MDBX_error_t err = MDBX_SUCCESS;
  for (unsigned i = 0; i < array->count; ++i) {
    if (ResumeThread(array->handles[i]) == -1)
      err = GetLastError();
    CloseHandle(array->handles[i]);
  }
  return err;
}

/*----------------------------------------------------------------------------*/
/* global lock for readers registration and initial seize */

MDBX_error_t mdbx_lck_init(MDBX_env_t *env) {
  (void)env;
  return MDBX_SUCCESS;
}

static void lck_recede(MDBX_env_t *env);

void mdbx_lck_detach(MDBX_env_t *env) {
  if (env->me_dxb_fd != INVALID_HANDLE_VALUE) {
    lck_recede(env);
  } else {
    assert(env->me_lck_fd == INVALID_HANDLE_VALUE);
  }
}

#define RIT_HEAD_OFFSET 0
#define RIT_HEAD_LEN offsetof(MDBX_lockinfo_t, li_numreaders)
#define RIT_BODY_OFFSET RIT_HEAD_LEN
#define RIT_BODY_LEN (MAX_MAPSIZE - RIT_BODY_OFFSET)
#define LCK_RIT_HEAD RIT_HEAD_OFFSET, RIT_HEAD_LEN
#define LCK_RIT_BODY RIT_BODY_OFFSET, RIT_BODY_LEN

/* FIXME: locking schema/algo descritpion.
 *
 *     H B
 *     ?-?  = free
 *     S-?  = shared
 *     E-?
 *     ?-S
 *     ?-E  = middle (transitional position)
 *     S-S
 *     S-E  = locked (i.e. for reader add/remove)
 *     E-S
 *     E-E  = exclusive
 */

static MDBX_error_t lck_free2middle(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_transition_begin("?-? (free)", "?-E (middle)");
  const MDBX_error_t err = lck_exclusive(env->me_lck_fd, flags, LCK_RIT_BODY);
  lck_transition_end("?-? (free)", "?-E (middle)", err);
  return err;
}

static MDBX_error_t lck_free2shared(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_transition_begin("?-? (free)", "S-? (shared)");
  const MDBX_error_t err = lck_shared(env->me_lck_fd, flags, LCK_RIT_HEAD);
  lck_transition_end("?-? (free)", "S-? (shared)", err);
  return err;
}

static MDBX_error_t lck_shared2locked(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_transition_begin("S-? (shared)", "S-E (locked)");
  const MDBX_error_t err = lck_exclusive(env->me_lck_fd, flags, LCK_RIT_BODY);
  lck_transition_end("S-E (locked)", "S-? (shared)", err);
  return err;
}

static void lck_locked2shared(MDBX_env_t *env) {
  lck_transition_begin("S-E (locked)", "S-? (shared)");
  if (!lck_unlock(env->me_lck_fd, LCK_RIT_BODY))
    lck_panic(env, "lck-body.unlock", GetLastError());
  lck_transition_end("S-E (locked)", "S-? (shared)", MDBX_SUCCESS);
}

static void lck_locked2middle(MDBX_env_t *env) {
  lck_transition_begin("S-E (locked)", "?-E (middle)");
  if (!lck_unlock(env->me_lck_fd, LCK_RIT_HEAD))
    lck_panic(env, "lck-body.unlock", GetLastError());
  lck_transition_end("S-E (locked)", "?-E (middle)", MDBX_SUCCESS);
}

static MDBX_error_t lck_middle2exclusive(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_transition_begin("?-E (middle)", "E-E (exclusive)");

  MDBX_error_t err =
      lck_exclusive(env->me_lck_fd, (flags & MDBX_EXCLUSIVE) ? flags : MDBX_NONBLOCK, LCK_RIT_HEAD);

  if (err == MDBX_SUCCESS) {
    lck_trace("got E-E (exclusive), continue lock-dxb-body");
    /* Check that another process don't operates in without-lck mode.
     * Doing such check by exclusive locking the body-part of dxb. Should be
     * noted:
     *  - we need an exclusive lock for do so;
     *  - we can't lock head, otherwise other process could get an error
     *    while opening db in valid (non-conflict) mode. */
    err = mdbx_lck_writer_lock(env, (flags & MDBX_EXCLUSIVE) ? flags : MDBX_NONBLOCK);
    if (err != MDBX_SUCCESS) {
      lck_trace("unable lock-dxb-body, rollabck to %s", "?-E (middle)");
      if (!lck_unlock(env->me_lck_fd, LCK_RIT_HEAD))
        lck_panic(env, "lck-head.unlock", GetLastError());
    }
  }

  lck_transition_end("?-E (middle)", "E-E (exclusive)", err);
  return err;
}

static MDBX_error_t lck_middle2locked(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_transition_begin("?-E (middle)", "S-E (locked)");
  const MDBX_error_t err = lck_shared(env->me_lck_fd, flags, LCK_RIT_HEAD);
  lck_transition_end("?-E (middle)", "S-E (locked)", err);
  return err;
}

static void lck_middle2free(MDBX_env_t *env) {
  lck_transition_begin("?-E (middle)", "?-? (free)");
  if (!lck_unlock(env->me_lck_fd, LCK_RIT_BODY))
    lck_panic(env, "lck-body.unlock", GetLastError());
  lck_transition_end("?-E (middle)", "?-? (free)", MDBX_SUCCESS);
}

static void lck_exclusive2middle(MDBX_env_t *env) {
  lck_transition_begin("E-E (exclusive)", "?-E (middle)");
  mdbx_lck_writer_unlock(env);
  if (!lck_unlock(env->me_lck_fd, LCK_RIT_HEAD))
    lck_panic(env, "lck-head.unlock", GetLastError());
  lck_transition_end("E-E (exclusive)", "?-E (middle)", MDBX_SUCCESS);
}

/*----------------------------------------------------------------------------*/

MDBX_error_t mdbx_lck_reader_registration_lock(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_trace(">> flags 0x%x %s", flags, (flags & MDBX_NONBLOCK) ? " (MDBX_NONBLOCK)" : "");

  AcquireSRWLockShared(&env->me_remap_guard);
  if (env->me_lck_fd == INVALID_HANDLE_VALUE) {
    lck_trace("<< MDBX_SUCCESS, without-lck");
    return MDBX_SUCCESS /* readonly database in readonly filesystem */;
  }

  const MDBX_error_t err = lck_shared2locked(env, flags);
  if (err != MDBX_SUCCESS) {
    ReleaseSRWLockShared(&env->me_remap_guard);
    lck_trace("<< %s, error %d", lck_is_collision(err) ? "busy" : "failed", err);
  } else
    lck_trace("<< ok");
  return err;
}

void mdbx_lck_reader_registration_unlock(MDBX_env_t *env) {
  if (env->me_lck_fd != INVALID_HANDLE_VALUE)
    lck_locked2shared(env);
  ReleaseSRWLockShared(&env->me_remap_guard);
  lck_trace("<< ok");
}

MDBX_seize_result_t mdbx_lck_seize(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_trace(">> flags 0x%x %s", flags, (flags & MDBX_NONBLOCK) ? " (MDBX_NONBLOCK)" : "",
            (flags & MDBX_EXCLUSIVE) ? " (MDBX_EXCLUSIVE)" : "");
  MDBX_error_t err;
  assert(env->me_dxb_fd != INVALID_HANDLE_VALUE);
  if (env->me_lck_fd == INVALID_HANDLE_VALUE) {
    /* LY: without-lck mode (e.g. on read-only filesystem) */
    err = lck_shared(env->me_dxb_fd, flags, LCK_DXB_WHOLE);
    if (err != MDBX_SUCCESS) {
      lck_trace("unable got without-lck, error %d", err);
      goto recede;
    }
    lck_trace("<< %s", "SEIZE_NOLCK");
    return seize_done(MDBX_SEIZE_NOLCK);
  }

  assert(env->me_lck_fd != INVALID_HANDLE_VALUE);
  /* Seize state as 'exclusive' (E-E and returns MDBX_SEIZE_EXCLUSIVE_FIRST)
   * or as 'shared' (S-? and returns MDBX_SUCCESS), otherwise returns an error */

  MDBX_seize_t exclusive_result = MDBX_SEIZE_EXCLUSIVE_FIRST;
  err = lck_free2middle(env, flags);
  if (err != MDBX_SUCCESS) {
    if (!lck_is_collision(err))
      goto recede;

    if ((flags & (MDBX_NONBLOCK | MDBX_EXCLUSIVE)) == (MDBX_NONBLOCK | MDBX_EXCLUSIVE)) {
      lck_trace("<< %s", "BUSY");
      return seize_failed(err);
    }

    err = lck_free2shared(env, flags);
    if (err != MDBX_SUCCESS)
      goto recede;

    if (!MDBX_OPT_TEND_LCK_RESET && (flags & MDBX_EXCLUSIVE) == 0) {
      lck_trace("<< %s", "SHARED");
      return seize_done(MDBX_SEIZE_SHARED);
    }

    err = lck_shared2locked(env, flags);
    if (err != MDBX_SUCCESS) {
      if (!lck_is_collision(err))
        goto recede;
      if ((flags & (MDBX_NONBLOCK | MDBX_EXCLUSIVE)) == MDBX_NONBLOCK) {
        lck_trace("<< %s", "SHARED");
        return seize_done(MDBX_SEIZE_SHARED);
      }
      goto recede;
    }

    lck_locked2middle(env);
    exclusive_result = MDBX_SEIZE_EXCLUSIVE_CONTINUE;
  }

  err = lck_middle2exclusive(env, flags);
  if (err == MDBX_SUCCESS) {
    assert(IS_SEIZE_EXCLUSIVE(exclusive_result));
    lck_trace("<< %s", (exclusive_result == MDBX_SEIZE_EXCLUSIVE_FIRST) ? "MDBX_SEIZE_EXCLUSIVE_FIRST"
                                                                        : "MDBX_SEIZE_EXCLUSIVE_LIVE");
    return seize_done(exclusive_result);
  }

  if (!lck_is_collision(err))
    goto recede;

  err = lck_middle2locked(env, flags);
  if (err != MDBX_SUCCESS)
    goto recede;

  lck_locked2shared(env);
  lck_trace("<< %s", "SHARED");
  return seize_done(MDBX_SEIZE_SHARED);

recede:
  lck_recede(env);
  lck_trace("<< %s", lck_is_collision(err) ? "busy" : "failed");
  return seize_failed(err);
}

MDBX_error_t mdbx_lck_downgrade(MDBX_env_t *env) {
  lck_trace(
      ">> transite-lck: from exclusive (E-E) to shared (S-?), transite-dxb: body-exclusive to body-free");
  assert(env->me_dxb_fd != INVALID_HANDLE_VALUE);
  assert(env->me_lck_fd != INVALID_HANDLE_VALUE);

  lck_exclusive2middle(env);
  const MDBX_error_t err = lck_middle2locked(env, MDBX_NONBLOCK);
  if (err != MDBX_SUCCESS) {
    lck_recede(env);
    lck_trace("<< failed, unlocked, error %d", err);
    return err;
  }

  lck_locked2shared(env);
  lck_trace("<< now at S-? (shared), done");
  return MDBX_SUCCESS;
}

MDBX_error_t mdbx_lck_upgrade(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */) {
  lck_trace(">> transite-lck: locked (S-E) to exclusive (E-E), flags 0x%x %s", flags,
            (flags & MDBX_NONBLOCK) ? " (MDBX_NONBLOCK)" : "");
  assert(env->me_dxb_fd != INVALID_HANDLE_VALUE);
  assert(env->me_lck_fd != INVALID_HANDLE_VALUE);
  /* lck_upgrade MUST returns:
  *   MDBX_SUCCESS - exclusive lock acquired,
  *                  NO other processes use DB.
  *   MDBX_BUSY    - exclusive lock NOT acquired, other processes use DB,
  *                  but not known readers, writers or mixed.
  *   MDBX_SIGN    - exclusive lock NOT acquired and SURE known that
  *                  other writer(s) is present, i.e. db-sync not required on close */

  MDBX_error_t err = lck_shared2locked(env, flags);
  if (err != MDBX_SUCCESS) {
    if (!lck_is_collision(err))
      goto recede;
    lck_trace("<< %s", "busy");
    return MDBX_EBUSY;
  }

  lck_locked2middle(env);
  err = lck_middle2exclusive(env, flags);
  if (err == MDBX_SUCCESS) {
    lck_trace("<< MDBX_SUCCESS");
    return MDBX_SUCCESS;
  }
  if (!lck_is_collision(err))
    goto recede;

  /* unable exclusive, rollback to shared */
  err = lck_middle2locked(env, flags);
  if (err != MDBX_SUCCESS)
    goto recede;
  lck_locked2shared(env);

  /* TODO: return MDBX_SIGN if at least one writer is present */
  lck_trace("<< MDBX_EBUSY");
  return MDBX_EBUSY;

recede:
  lck_recede(env);
  lck_trace("<< failed, unlocked, err %d", err);
  return err;
}

static void lck_recede(MDBX_env_t *env) {
  /* explicitly unlock to avoid latency for other processes (windows kernel
   * releases such locks via deferred queues) */

  assert(env->me_dxb_fd != INVALID_HANDLE_VALUE);
  lck_trace(">>");
  MDBX_error_t err;
  if (env->me_lck_fd != INVALID_HANDLE_VALUE) {
    /* double `unlock` for robustly remove overlapped shared/exclusive locks */
    while (lck_unlock(env->me_lck_fd, LCK_RIT_HEAD))
      ;
    err = GetLastError();
    if (err != ERROR_NOT_LOCKED)
      lck_trace("%s, %d", "lck-head.unlock", err);
    SetLastError(ERROR_SUCCESS);

    while (lck_unlock(env->me_lck_fd, LCK_RIT_BODY))
      ;
    err = GetLastError();
    if (err != ERROR_NOT_LOCKED)
      lck_trace("%s, %d", "lck-head.body", err);
  }

  while (lck_unlock(env->me_dxb_fd, LCK_DXB_BODY))
    ;
  err = GetLastError();
  if (err != ERROR_NOT_LOCKED)
    lck_trace("%s, %d", "lck-dxb.body", err);

  while (lck_unlock(env->me_dxb_fd, LCK_DXB_WHOLE))
    ;
  err = GetLastError();
  if (err != ERROR_NOT_LOCKED)
    lck_trace("%s, %d", "lck-dxb.whole", err);

  lck_trace("<<");
}

/*----------------------------------------------------------------------------*/
/* reader checking (by pid) */

MDBX_error_t mdbx_lck_reader_alive_set(MDBX_env_t *env, MDBX_pid_t pid) {
  (void)env;
  (void)pid;
  lck_trace("pid %ld", (long)pid);
  return MDBX_SUCCESS;
}

MDBX_error_t mdbx_lck_reader_alive_clear(MDBX_env_t *env, MDBX_pid_t pid) {
  (void)env;
  (void)pid;
  lck_trace("pid %ld", (long)pid);
  return MDBX_SUCCESS;
}

/* Checks reader by pid.
 *
 * Returns:
 *   MDBX_SUCCESS, if pid is live;
 *   MDBX_SIGN, if pid is dead;
 *   or otherwise the errcode. */
MDBX_error_t mdbx_lck_reader_alive_check(MDBX_env_t *env, MDBX_pid_t pid) {
  (void)env;
  lck_trace(">> pid %ld", (long)pid);
  HANDLE hProcess = OpenProcess(SYNCHRONIZE, FALSE, pid);
  MDBX_error_t rc;
  if (likely(hProcess)) {
    rc = WaitForSingleObject(hProcess, 0);
    if (unlikely(rc == WAIT_FAILED))
      rc = GetLastError();
    CloseHandle(hProcess);
  } else {
    rc = GetLastError();
  }

  switch (rc) {
  case ERROR_INVALID_PARAMETER:
    /* pid seem invalid */
    lck_trace("<< pid %ld, %s", (long)pid, "MDBX_SIGN (seem invalid)");
    return MDBX_SIGN;
  case WAIT_OBJECT_0:
    /* process just exited */
    lck_trace("<< pid %ld, %s", (long)pid, "MDBX_SIGN (just exited)");
    return MDBX_SIGN;
  case WAIT_TIMEOUT:
    /* pid running */
    lck_trace("<< pid %ld, %s", (long)pid, "MDBX_SUCCESS (running)");
    return MDBX_SUCCESS;
  default:
    /* failure */
    lck_trace("<< pid %ld, failure %d", (long)pid, rc);
    return rc;
  }
}
