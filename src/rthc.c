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

#define rthc_extra(fmt, ...) log_extra(MDBX_LOG_RTHC, fmt, ##__VA_ARGS__)
#define rthc_trace(fmt, ...) log_trace(MDBX_LOG_RTHC, fmt, ##__VA_ARGS__)
#define rthc_verbose(fmt, ...) log_verbose(MDBX_LOG_RTHC, fmt, ##__VA_ARGS__)
#define rthc_info(fmt, ...) log_info(MDBX_LOG_RTHC, fmt, ##__VA_ARGS__)
#define rthc_notice(fmt, ...) log_notice(MDBX_LOG_RTHC, fmt, ##__VA_ARGS__)
#define rthc_warning(fmt, ...) log_warning(MDBX_LOG_RTHC, fmt, ##__VA_ARGS__)
#define rthc_error(fmt, ...) log_error(MDBX_LOG_RTHC, fmt, ##__VA_ARGS__)
#define rthc_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_RTHC, __func__, __LINE__, "%s, error %d", msg, err)

/*----------------------------------------------------------------------------*/
/* rthc (tls keys and destructors) */

typedef struct rthc_entry_t {
  MDBX_reader_t *begin;
  MDBX_reader_t *end;
  mdbx_thread_key_t key;
} rthc_entry_t;

#if MDBX_DEBUG
#define RTHC_INITIAL_LIMIT 1
#else
#define RTHC_INITIAL_LIMIT 16
#endif

#if defined(_WIN32) || defined(_WIN64)
static CRITICAL_SECTION rthc_critical_section;
#else
int __cxa_thread_atexit_impl(void (*dtor)(void *), void *obj, void *dso_symbol) __attribute__((weak));
static pthread_mutex_t mdbx_rthc_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t mdbx_rthc_cond = PTHREAD_COND_INITIALIZER;
static mdbx_thread_key_t mdbx_rthc_key;
static volatile uint32_t mdbx_rthc_pending;

static void __cold mdbx_workaround_glibc_bug21031(void) {
  /* Workaround for https://sourceware.org/bugzilla/show_bug.cgi?id=21031
   *
   * Due race between pthread_key_delete() and __nptl_deallocate_tsd()
   * The destructor(s) of thread-local-storate object(s) may be running
   * in another thread(s) and be blocked or not finished yet.
   * In such case we get a SEGFAULT after unload this library DSO.
   *
   * So just by yielding a few timeslices we give a chance
   * to such destructor(s) for completion and avoids segfault. */
  sched_yield();
  sched_yield();
  sched_yield();
}
#endif

static unsigned rthc_count, rthc_limit;
static rthc_entry_t *rthc_table;
static rthc_entry_t rthc_table_static[RTHC_INITIAL_LIMIT];

static __cold void rthc_lock(void) {
#if defined(_WIN32) || defined(_WIN64)
  EnterCriticalSection(&rthc_critical_section);
#else
  mdbx_ensure(nullptr, pthread_mutex_lock(&mdbx_rthc_mutex) == 0);
#endif
}

static __cold void rthc_unlock(void) {
#if defined(_WIN32) || defined(_WIN64)
  LeaveCriticalSection(&rthc_critical_section);
#else
  mdbx_ensure(nullptr, pthread_mutex_unlock(&mdbx_rthc_mutex) == 0);
#endif
}

static inline int tls_alloc(mdbx_thread_key_t *key) {
  int rc;
#if defined(_WIN32) || defined(_WIN64)
  *key = TlsAlloc();
  rc = (*key != TLS_OUT_OF_INDEXES) ? MDBX_SUCCESS : GetLastError();
#else
  rc = pthread_key_create(key, nullptr);
#endif
  rthc_info("&key = %p, value 0x%x, rc %d", key, (unsigned)*key, rc);
  return rc;
}

static inline void tls_release(mdbx_thread_key_t key) {
  rthc_info("key = 0x%x", (unsigned)key);
#if defined(_WIN32) || defined(_WIN64)
  mdbx_ensure(nullptr, TlsFree(key));
#else
  mdbx_ensure(nullptr, pthread_key_delete(key) == 0);
  mdbx_workaround_glibc_bug21031();
#endif
}

static inline void *tls_get(mdbx_thread_key_t key) {
#if defined(_WIN32) || defined(_WIN64)
  return TlsGetValue(key);
#else
  return pthread_getspecific(key);
#endif
}

static void tls_set(mdbx_thread_key_t key, const void *value) {
#if defined(_WIN32) || defined(_WIN64)
  mdbx_ensure(nullptr, TlsSetValue(key, (void *)value));
#else
#define MDBX_THREAD_RTHC_ZERO 0
#define MDBX_THREAD_RTHC_REGISTERD 1
#define MDBX_THREAD_RTHC_COUNTED 2
  static __thread uint32_t thread_registration_state;
  if (value && unlikely(thread_registration_state == MDBX_THREAD_RTHC_ZERO)) {
    thread_registration_state = MDBX_THREAD_RTHC_REGISTERD;
    rthc_info("thread registered 0x%" PRIxPTR, (uintptr_t)mdbx_thread_self());
    if (&__cxa_thread_atexit_impl == nullptr ||
        __cxa_thread_atexit_impl(rthc_thread_dtor, &thread_registration_state,
                                 (void *)&mdbx_version /* dso_anchor */)) {
      mdbx_ensure(nullptr, pthread_setspecific(mdbx_rthc_key, &thread_registration_state) == 0);
      thread_registration_state = MDBX_THREAD_RTHC_COUNTED;
      const unsigned count_before = mdbx_atomic_add32(&mdbx_rthc_pending, 1);
      mdbx_ensure(nullptr, count_before < INT_MAX);
      rthc_info("fallback to pthreads' tsd, key 0x%x, count %u", (unsigned)mdbx_rthc_key, count_before);
      (void)count_before;
    }
  }
  mdbx_ensure(nullptr, pthread_setspecific(key, value) == 0);
#endif
}

__cold void rthc_global_ctor(void) {
  rthc_limit = RTHC_INITIAL_LIMIT;
  rthc_table = rthc_table_static;
#if defined(_WIN32) || defined(_WIN64)
  InitializeCriticalSection(&rthc_critical_section);
#else
  mdbx_ensure(nullptr, pthread_key_create(&mdbx_rthc_key, rthc_thread_dtor) == 0);
  rthc_info("pid %d, &mdbx_rthc_key = %p, value 0x%x", mdbx_getpid(), &mdbx_rthc_key, (unsigned)mdbx_rthc_key);
#endif
}

/* dtor called for thread, i.e. for all mdbx's environment objects */
void rthc_thread_dtor(void *ptr) {
  rthc_lock();
  rthc_trace(">> pid %d, thread 0x%" PRIxPTR ", rthc %p", mdbx_getpid(), (uintptr_t)mdbx_thread_self(), ptr);

  const MDBX_pid_t self_pid = mdbx_getpid();
  for (unsigned i = 0; i < rthc_count; ++i) {
    const mdbx_thread_key_t key = rthc_table[i].key;
    MDBX_reader_t *const rthc = tls_get(key);
    if (rthc < rthc_table[i].begin || rthc >= rthc_table[i].end)
      continue;
#if !defined(_WIN32) && !defined(_WIN64)
    if (pthread_setspecific(key, nullptr) != 0) {
      rthc_trace("== thread 0x%" PRIxPTR ", rthc %p: ignore race with tsd-key deletion",
                 (uintptr_t)mdbx_thread_self(), ptr);
      continue /* ignore race with tsd-key deletion by mdbx_env_close() */;
    }
#endif

    rthc_info("== thread 0x%" PRIxPTR ", rthc %p, [%i], %p ... %p (%+i), rtch-pid %i, "
              "current-pid %i",
              (uintptr_t)mdbx_thread_self(), rthc, i, rthc_table[i].begin, rthc_table[i].end,
              (int)(rthc - rthc_table[i].begin), rthc->mr_pid, self_pid);
    if (rthc->mr_pid == self_pid) {
      rthc_trace("==== thread 0x%" PRIxPTR ", rthc %p, cleanup", (uintptr_t)mdbx_thread_self(), rthc);
      rthc->mr_pid = 0;
    }
  }

#if defined(_WIN32) || defined(_WIN64)
  rthc_trace("<< thread 0x%" PRIxPTR ", rthc %p", (uintptr_t)mdbx_thread_self(), ptr);
  rthc_unlock();
#else
  const char self_registration = *(char *)ptr;
  *(char *)ptr = MDBX_THREAD_RTHC_ZERO;
  rthc_trace("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %d", (uintptr_t)mdbx_thread_self(), ptr,
             mdbx_getpid(), self_registration);
  if (self_registration == MDBX_THREAD_RTHC_COUNTED)
    mdbx_ensure(nullptr, mdbx_atomic_sub32(&mdbx_rthc_pending, 1) > 0);

  if (mdbx_rthc_pending == 0) {
    rthc_trace("== thread 0x%" PRIxPTR ", rthc %p, pid %d, wake", (uintptr_t)mdbx_thread_self(), ptr,
               mdbx_getpid());
    mdbx_ensure(nullptr, pthread_cond_broadcast(&mdbx_rthc_cond) == 0);
  }

  rthc_trace("<< thread 0x%" PRIxPTR ", rthc %p", (uintptr_t)mdbx_thread_self(), ptr);
  /* Allow tail call optimization, i.e. gcc should generate the jmp instruction
   * instead of a call for pthread_mutex_unlock() and therefore CPU could not
   * return to current DSO's code section, which may be unloaded immediately
   * after the mutex got released. */
  pthread_mutex_unlock(&mdbx_rthc_mutex);
#endif
}

__cold void rthc_global_dtor(void) {
  rthc_trace(">> pid %d, &rthc_global_dtor %p, &rthc_thread_dtor = %p, "
             "&rthc_release = %p",
             mdbx_getpid(), &rthc_global_dtor, &rthc_thread_dtor, &rthc_release);

  rthc_lock();
#if !defined(_WIN32) && !defined(_WIN64)
  char *rthc = (char *)pthread_getspecific(mdbx_rthc_key);
  rthc_trace("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %d", (uintptr_t)mdbx_thread_self(), rthc,
             mdbx_getpid(), rthc ? *rthc : -1);
  if (rthc) {
    const char self_registration = *(char *)rthc;
    *rthc = MDBX_THREAD_RTHC_ZERO;
    if (self_registration == MDBX_THREAD_RTHC_COUNTED)
      mdbx_ensure(nullptr, mdbx_atomic_sub32(&mdbx_rthc_pending, 1) > 0);
  }

  struct timespec abstime;
  mdbx_ensure(nullptr, clock_gettime(CLOCK_REALTIME, &abstime) == 0);
  abstime.tv_nsec += 1000000000l / 10;
  if (abstime.tv_nsec >= 1000000000l) {
    abstime.tv_nsec -= 1000000000l;
    abstime.tv_sec += 1;
  }
#if MDBX_DEBUG > 0
  abstime.tv_sec += 600;
#endif

  for (unsigned left; (left = mdbx_rthc_pending) > 0;) {
    rthc_trace("pid %d, pending %u, wait for...", mdbx_getpid(), left);
    const int rc = pthread_cond_timedwait(&mdbx_rthc_cond, &mdbx_rthc_mutex, &abstime);
    if (rc && rc != EINTR)
      break;
  }
  tls_release(mdbx_rthc_key);
#endif

  const MDBX_pid_t self_pid = mdbx_getpid();
  for (unsigned i = 0; i < rthc_count; ++i) {
    const mdbx_thread_key_t key = rthc_table[i].key;
    tls_release(key);
    for (MDBX_reader_t *rthc = rthc_table[i].begin; rthc < rthc_table[i].end; ++rthc) {
      rthc_trace("== [%i] = key %u, %p ... %p, rthc %p (%+i), "
                 "rthc-pid %i, current-pid %i",
                 i, key, rthc_table[i].begin, rthc_table[i].end, rthc, (int)(rthc - rthc_table[i].begin),
                 rthc->mr_pid, self_pid);
      if (rthc->mr_pid == self_pid) {
        rthc->mr_pid = 0;
        rthc_trace("== cleanup %p", rthc);
      }
    }
  }

  rthc_limit = rthc_count = 0;
  if (rthc_table != rthc_table_static)
    free(rthc_table);
  rthc_table = nullptr;
  rthc_unlock();

#if defined(_WIN32) || defined(_WIN64)
  DeleteCriticalSection(&rthc_critical_section);
#else
  /* LY: yielding a few timeslices to give a more chance
   * to racing destructor(s) for completion. */
  mdbx_workaround_glibc_bug21031();
#endif

  rthc_trace("<< pid %d\n", mdbx_getpid());
}

__cold int rthc_alloc(mdbx_thread_key_t *key, MDBX_reader_t *begin, MDBX_reader_t *end) {
#ifndef NDEBUG
  *key = (mdbx_thread_key_t)0xBADBADBAD;
#endif /* NDEBUG */
  int rc = tls_alloc(key);
  if (rc != MDBX_SUCCESS)
    return rc;

  rthc_lock();
  rthc_trace(">> key 0x%x, rthc_count %u, rthc_limit %u", *key, rthc_count, rthc_limit);
  if (rthc_count == rthc_limit) {
    rthc_entry_t *new_table = realloc((rthc_table == rthc_table_static) ? nullptr : rthc_table,
                                      sizeof(rthc_entry_t) * rthc_limit * 2);
    if (new_table == nullptr) {
      rc = MDBX_ENOMEM;
      goto bailout;
    }
    if (rthc_table == rthc_table_static)
      memcpy(new_table, rthc_table_static, sizeof(rthc_table_static));
    rthc_table = new_table;
    rthc_limit *= 2;
  }
  rthc_trace("== [%i] = key %u, %p ... %p", rthc_count, *key, begin, end);
  rthc_table[rthc_count].key = *key;
  rthc_table[rthc_count].begin = begin;
  rthc_table[rthc_count].end = end;
  ++rthc_count;
  rthc_trace("<< key 0x%x, rthc_count %u, rthc_limit %u", *key, rthc_count, rthc_limit);
  rthc_unlock();
  return MDBX_SUCCESS;

bailout:
  tls_release(*key);
  rthc_unlock();
  return rc;
}

__cold void rthc_release(const mdbx_thread_key_t key) {
  tls_release(key);
  rthc_lock();
  rthc_trace(">> key 0x%x, rthc_count %u, rthc_limit %u", key, rthc_count, rthc_limit);

  for (unsigned i = 0; i < rthc_count; ++i) {
    if (key == rthc_table[i].key) {
      const MDBX_pid_t self_pid = mdbx_getpid();
      rthc_trace("== [%i], %p ...%p, current-pid %d", i, rthc_table[i].begin, rthc_table[i].end, self_pid);

      for (MDBX_reader_t *rthc = rthc_table[i].begin; rthc < rthc_table[i].end; ++rthc) {
        if (rthc->mr_pid == self_pid) {
          rthc->mr_pid = 0;
          rthc_trace("== cleanup %p", rthc);
        }
      }
      if (--rthc_count > 0)
        rthc_table[i] = rthc_table[rthc_count];
      else if (rthc_table != rthc_table_static) {
        free(rthc_table);
        rthc_table = rthc_table_static;
        rthc_limit = RTHC_INITIAL_LIMIT;
      }
      break;
    }
  }

  rthc_trace("<< key 0x%x, rthc_count %u, rthc_limit %u", key, rthc_count, rthc_limit);
  rthc_unlock();
}

/*----------------------------------------------------------------------------*/

#if defined(_WIN32) || defined(_WIN64)

MDBX_INTERNAL void osal_ctor(void);

static void NTAPI tls_callback(PVOID module, DWORD reason, PVOID reserved) {
  (void)reserved;
  switch (reason) {
  case DLL_PROCESS_ATTACH:
    /* mdbx_global_ctor */ {
      osal_ctor();
      rthc_global_ctor();
    }
    break;
  case DLL_PROCESS_DETACH:
    /* mdbx_global_dtor */ { rthc_global_dtor(); }
    break;

  case DLL_THREAD_ATTACH:
    break;
  case DLL_THREAD_DETACH:
    rthc_thread_dtor(module);
    break;
  }
}

/* *INDENT-OFF* */
/* clang-format off */
#if defined(_MSC_VER)
#  pragma const_seg(push)
#  pragma data_seg(push)

#  ifdef _WIN64
     /* kick a linker to create the TLS directory if not already done */
#    pragma comment(linker, "/INCLUDE:_tls_used")
     /* Force some symbol references. */
#    pragma comment(linker, "/INCLUDE:mdbx_tls_callback")
     /* specific const-segment for WIN64 */
#    pragma const_seg(".CRT$XLB")
     const
#  else
     /* kick a linker to create the TLS directory if not already done */
#    pragma comment(linker, "/INCLUDE:__tls_used")
     /* Force some symbol references. */
#    pragma comment(linker, "/INCLUDE:_mdbx_tls_callback")
     /* specific data-segment for WIN32 */
#    pragma data_seg(".CRT$XLB")
#  endif

   PIMAGE_TLS_CALLBACK mdbx_tls_callback = tls_callback;
#  pragma data_seg(pop)
#  pragma const_seg(pop)

#elif defined(__GNUC__)
#  ifdef _WIN64
     const
#  endif
   PIMAGE_TLS_CALLBACK mdbx_tls_callback __attribute__((section(".CRT$XLB"), used))
     = tls_callback;
#else
#  error FIXME
#endif
/* *INDENT-ON* */
/* clang-format on */

#else

static __cold __attribute__((constructor)) void mdbx_global_ctor(void) {
  osal_ctor();
  rthc_global_ctor();
}

static __cold __attribute__((destructor)) void mdbx_global_dtor(void) { rthc_global_dtor(); }

#endif
