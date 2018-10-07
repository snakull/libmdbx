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

#if MDBX_CONFIGURED_DEBUG_ABILITIES
MDBX_INTERNAL MDBX_debugbits_t mdbx_debug_bits =
    (MDBX_DBG_LOGGING | MDBX_DBG_ASSERT) & MDBX_CONFIGURED_DEBUG_ABILITIES;
#define MDBX_DEBUG_BITS (MDBX_CONFIGURED_DEBUG_ABILITIES & mdbx_debug_bits)
#else
#define MDBX_DEBUG_BITS 0
#endif /* MDBX_CONFIGURED_DEBUG_ABILITIES */

#define MDBX_ASSERTIONS_ENABLED() (MDBX_DEBUG_BITS & MDBX_DBG_ASSERT)
#define MDBX_AUDIT_ENABLED() (MDBX_DEBUG_BITS & MDBX_DBG_AUDIT)
#define MDBX_JITTER_ENABLED() (MDBX_DEBUG_BITS & MDBX_DBG_JITTER)

MDBX_INTERNAL void mdbx_panic(const MDBX_env_t *env, MDBX_debuglog_subsystem_t subsystem, const char *func,
                              unsigned line, const char *fmt, ...)
#if defined(__GNUC__) || __has_attribute(format)
    __attribute__((format(printf, 5, 6)))
#endif
    ;

#define mdbx_fatal(env, subsys, fmt, ...) mdbx_panic(env, subsys, __func__, __LINE__, fmt, ##__VA_ARGS__)

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

MDBX_INTERNAL void mdbx_dbglog(const debug_event_t event, const char *function, const char *fmt, ...)
#if defined(__GNUC__) || __has_attribute(format)
    __attribute__((format(printf, 3, 4)))
#endif
    ;

typedef struct MDBX_debug_cookie {
  void *ptr;
} MDBX_debug_cookie_t;

MDBX_INTERNAL MDBX_debug_cookie_t mdbx_dbglog_begin(const debug_event_t event, const char *function);
MDBX_INTERNAL void mdbx_dbglog_continue(MDBX_debug_cookie_t cookie, const char *fmt, ...)
#if defined(__GNUC__) || __has_attribute(format)
    __attribute__((format(printf, 2, 3)))
#endif
    ;
MDBX_INTERNAL void mdbx_dbglog_continue_ap(MDBX_debug_cookie_t cookie, const char *fmt, va_list ap);
MDBX_INTERNAL void mdbx_dbglog_end(MDBX_debug_cookie_t cookie, const char *optional_final_msg);

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

#if MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_DBG_LOGGING
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
