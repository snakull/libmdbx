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

#pragma once

#include "../mdbx.h"
#include "config.h"
#include "defs.h"

/*----------------------------------------------------------------------------*/
/* Debug and Logging stuff */

#undef NDEBUG
#undef _NDEBUG
#if MDBX_DEBUG
#define _DEBUG 1
#else
#define NDEBUG 1
#endif

#if MDBX_CONFIGURED_DEBUG_ABILITIES
MDBX_INTERNAL MDBX_debugbits_t mdbx_debug_bits =
    (MDBX_DBG_LOGGING | MDBX_DBG_ASSERT) & MDBX_CONFIGURED_DEBUG_ABILITIES;
#define MDBX_DEBUG_BITS (MDBX_CONFIGURED_DEBUG_ABILITIES & mdbx_debug_bits)
#else
#define MDBX_DEBUG_BITS 0
#define mdbx_debug_bits 0
#endif /* MDBX_CONFIGURED_DEBUG_ABILITIES */

#define MDBX_ASSERTIONS_ENABLED() (MDBX_DEBUG_BITS & MDBX_DBG_ASSERT)
#define MDBX_AUDIT_ENABLED() (MDBX_DEBUG_BITS & MDBX_DBG_AUDIT)
#define MDBX_JITTER_ENABLED() (MDBX_DEBUG_BITS & MDBX_DBG_JITTER)

MDBX_INTERNAL __printf_args(5, 6) void mdbx_panic(const MDBX_env_t *env, MDBX_debuglog_subsystem_t subsystem,
                                                  const char *func, unsigned line, const char *fmt, ...);

#define mdbx_fatal(env, subsys, fmt, ...) mdbx_panic(env, subsys, __func__, __LINE__, fmt, ##__VA_ARGS__)

/*----------------------------------------------------------------------------*/

#if MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_VALGRIND
#include <valgrind/memcheck.h>
#ifndef VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE
/* LY: available since Valgrind 3.10 */
#define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a, s)
#define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a, s)
#endif
#elif !defined(RUNNING_ON_VALGRIND)
#define VALGRIND_CREATE_MEMPOOL(h, r, z)
#define VALGRIND_DESTROY_MEMPOOL(h)
#define VALGRIND_MEMPOOL_TRIM(h, a, s)
#define VALGRIND_MEMPOOL_ALLOC(h, a, s)
#define VALGRIND_MEMPOOL_FREE(h, a)
#define VALGRIND_MEMPOOL_CHANGE(h, a, b, s)
#define VALGRIND_MAKE_MEM_NOACCESS(a, s)
#define VALGRIND_MAKE_MEM_DEFINED(a, s)
#define VALGRIND_MAKE_MEM_UNDEFINED(a, s)
#define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a, s)
#define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a, s)
#define VALGRIND_CHECK_MEM_IS_ADDRESSABLE(a, s) (0)
#define VALGRIND_CHECK_MEM_IS_DEFINED(a, s) (0)
#define RUNNING_ON_VALGRIND (0)
#endif /* MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_VALGRIND */

#if !defined(NDEBUG) || defined(_DEBUG)
#define MEMSET4DEBUG(addr, value, size) memset(addr, value, size)
#else
#define MEMSET4DEBUG(addr, value, size) __noop()
#endif

#define VALGRIND_MAKE_MEM_UNDEFINED_ERASE(a, s)                                                               \
  do {                                                                                                        \
    MEMSET4DEBUG(a, 0xCC, s);                                                                                 \
    VALGRIND_MAKE_MEM_UNDEFINED(a, s);                                                                        \
  } while (0)

#define VALGRIND_MAKE_MEM_NOACCESS_ERASE(a, s)                                                                \
  do {                                                                                                        \
    MEMSET4DEBUG(a, 0xDD, s);                                                                                 \
    VALGRIND_MAKE_MEM_NOACCESS(a, s);                                                                         \
  } while (0)

#ifdef __SANITIZE_ADDRESS__
#include <sanitizer/asan_interface.h>
#elif !defined(ASAN_POISON_MEMORY_REGION)
#define ASAN_POISON_MEMORY_REGION(addr, size) ((void)(addr), (void)(size))
#define ASAN_UNPOISON_MEMORY_REGION(addr, size) ((void)(addr), (void)(size))
#endif /* __SANITIZE_ADDRESS__ */

/*----------------------------------------------------------------------------*/
MDBX_INTERNAL void __cold mdbx_assert_fail(const MDBX_env_t *env, const char *msg, const char *func,
                                           unsigned line);

#define mdbx_ensure_msg(env, expr, msg)                                                                       \
  do {                                                                                                        \
    if (unlikely(!(expr)))                                                                                    \
      mdbx_assert_fail(env, msg, __func__, __LINE__);                                                         \
  } while (0)

#define mdbx_ensure(env, expr) mdbx_ensure_msg(env, expr, #expr)

/*----------------------------------------------------------------------------*/

#define mdbx_assert_msg(env, expr, msg)                                                                       \
  do {                                                                                                        \
    if (unlikely(!(expr)))                                                                                    \
      mdbx_assert_fail(env, msg, __func__, __LINE__);                                                         \
  } while (0)

#define mdbx_assert(env, expr) mdbx_assert_msg(env, expr, #expr)
#undef assert
#define assert(expr) mdbx_assert(NULL, expr)

/*----------------------------------------------------------------------------*/

typedef struct {
  MDBX_debuglog_subsystem_t subsystem : 8;
  MDBX_debuglog_level_t level : 8;
  unsigned line : 16;
} debug_event_t;

static inline debug_event_t debug_event(MDBX_debuglog_subsystem_t subsys, MDBX_debuglog_level_t level,
                                        unsigned line) {
  const debug_event_t result = {subsys, level, line};
  return result;
}

typedef struct MDBX_debug_cookie {
  void *ptr;
} MDBX_debug_cookie_t;

#if MDBX_LOGGING
MDBX_INTERNAL __printf_args(3, 4) void mdbx_dbglog(const debug_event_t event, const char *function,
                                                   const char *fmt, ...);
MDBX_INTERNAL MDBX_debug_cookie_t mdbx_dbglog_begin(const debug_event_t event, const char *function);
MDBX_INTERNAL __printf_args(2, 3) void mdbx_dbglog_continue(MDBX_debug_cookie_t cookie, const char *fmt, ...);
MDBX_INTERNAL void mdbx_dbglog_continue_ap(MDBX_debug_cookie_t cookie, const char *fmt, va_list ap);
MDBX_INTERNAL void mdbx_dbglog_end(MDBX_debug_cookie_t cookie, const char *optional_final_msg);
#else
static inline __printf_args(3, 4) void mdbx_dbglog(const debug_event_t event, const char *function,
                                                   const char *fmt, ...) {
  (void)event;
  (void)function;
  (void)fmt;
}
static inline __printf_args(2, 3) void mdbx_dbglog_continue(MDBX_debug_cookie_t cookie, const char *fmt, ...) {
  (void)cookie;
  (void)fmt;
}
static inline MDBX_debug_cookie_t mdbx_dbglog_begin(const debug_event_t event, const char *function) {
  MDBX_debug_cookie_t cookie = {0};
  (void)event;
  (void)function;
  return cookie;
}
static inline void mdbx_dbglog_continue_ap(MDBX_debug_cookie_t cookie, const char *fmt, va_list ap) {
  (void)cookie;
  (void)fmt;
  (void)ap;
}
static inline void mdbx_dbglog_end(MDBX_debug_cookie_t cookie, const char *optional_final_msg) {
  (void)cookie;
  (void)optional_final_msg;
}
#endif /* ! MDBX_LOGGING */

#define dbglog_begin(subsys, level)                                                                           \
  do {                                                                                                        \
  const MDBX_debug_cookie_t _mdbx_debug_cookie =                                                              \
      mdbx_dbglog_begin(debug_event(subsys, level, __LINE__), __func__)
#define dbglog_continue(fmt, ...) mdbx_dbglog_continue(_mdbx_debug_cookie, fmt, ##__VA_ARGS__)
#define dbglog_continue_ap(fmt, ap) mdbx_dbglog_continue_ap(_mdbx_debug_cookie, fmt, ap)
#define dbglog_end(...)                                                                                       \
  mdbx_dbglog_end(_mdbx_debug_cookie, __VA_ARGS__);                                                           \
  }                                                                                                           \
  while (0)

#if MDBX_LOGGING
MDBX_INTERNAL uint8_t mdbx_dbglog_levels[MDBX_LOG_MAX];
MDBX_INTERNAL MDBX_debuglog_callback_t *mdbx_debug_logger;
#define MDBX_DBGLOG_ENABLED(subsys, level) (level >= (MDBX_debuglog_level_t)mdbx_dbglog_levels[subsys])
#else
#define mdbx_debug_logger nullptr
#define MDBX_DBGLOG_ENABLED(subsys, level) false
#endif /* MDBX_CONFIGURED_DEBUG_ABILITIES */

static inline bool mdbx_dbglog_enabled(MDBX_debuglog_subsystem_t subsys, MDBX_debuglog_level_t level) {
  assert(subsys >= MDBX_LOG_MISC && subsys < MDBX_LOG_MAX);
  assert(level >= MDBX_LOGLEVEL_EXTRA && level < MDBX_LOGLEVEL_FATAL);
  return MDBX_DBGLOG_ENABLED(subsys, level);
}

#define mdbx_log(subsys, level, fmt, ...)                                                                     \
  do {                                                                                                        \
    if (unlikely(mdbx_dbglog_enabled(subsys, level)))                                                         \
      mdbx_dbglog(debug_event(subsys, level, __LINE__), __func__, fmt "\n", ##__VA_ARGS__);                   \
  } while (0)

#define log_extra(subsys, fmt, ...) mdbx_log(subsys, MDBX_LOGLEVEL_EXTRA, fmt, ##__VA_ARGS__)
#define log_trace(subsys, fmt, ...) mdbx_log(subsys, MDBX_LOGLEVEL_TRACE, fmt, ##__VA_ARGS__)
#define log_verbose(subsys, fmt, ...) mdbx_log(subsys, MDBX_LOGLEVEL_VERBOSE, fmt, ##__VA_ARGS__)
#define log_info(subsys, fmt, ...) mdbx_log(subsys, MDBX_LOGLEVEL_INFO, fmt, ##__VA_ARGS__)
#define log_notice(subsys, fmt, ...) mdbx_log(subsys, MDBX_LOGLEVEL_NOTICE, fmt, ##__VA_ARGS__)
#define log_warning(subsys, fmt, ...) mdbx_log(subsys, MDBX_LOGLEVEL_WARNING, fmt, ##__VA_ARGS__)
#define log_error(subsys, fmt, ...) mdbx_log(subsys, MDBX_LOGLEVEL_ERROR, fmt, ##__VA_ARGS__)
#define log_fatal(subsys, fmt, ...) mdbx_log(subsys, MDBX_LOGLEVEL_FATAL, fmt, ##__VA_ARGS__)

#define mdbx_debug_extra(fmt, ...) log_extra(MDBX_LOG_MISC, fmt, ##__VA_ARGS__)
#define mdbx_trace(fmt, ...) log_trace(MDBX_LOG_MISC, fmt, ##__VA_ARGS__)
#define mdbx_debug(fmt, ...) log_verbose(MDBX_LOG_MISC, fmt, ##__VA_ARGS__)
#define mdbx_verbose(fmt, ...) log_verbose(MDBX_LOG_MISC, fmt, ##__VA_ARGS__)
//#define mdbx_info(fmt, ...) log_info(MDBX_LOG_MISC, fmt, ##__VA_ARGS__)
#define mdbx_notice(fmt, ...) log_notice(MDBX_LOG_MISC, fmt, ##__VA_ARGS__)
#define mdbx_warning(fmt, ...) log_warning(MDBX_LOG_MISC, fmt, ##__VA_ARGS__)
#define mdbx_error(fmt, ...) log_error(MDBX_LOG_MISC, fmt, ##__VA_ARGS__)
