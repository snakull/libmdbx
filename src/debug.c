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

#include "./debug.h"
#include "./bits.h"
#include "./proto.h"
#include "./ualb.h"

/*----------------------------------------------------------------------------*/

#ifndef _MSC_VER
/* Prototype should match libc runtime. ISO POSIX (2003) & LSB 3.1 */
__nothrow __noreturn void __assert_fail(const char *assertion, const char *file, unsigned line,
                                        const char *function);
#else
__extern_C __declspec(dllimport) void __cdecl _assert(char const *message, char const *filename,
                                                      unsigned line);
#endif /* _MSC_VER */

void __cold mdbx_assert_fail(const MDBX_env_t *env, const char *msg, const char *func, unsigned line) {
#if MDBX_CONFIGURED_DEBUG_ABILITIES
  if ((MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_DBG_ASSERT) && env && env->me_assert_func) {
    env->me_assert_func(env, msg, func, line);
    return;
  }
#else
  (void)env;
#endif /* MDBX_CONFIGURED_DEBUG_ABILITIES */

  mdbx_dbglog(debug_event(MDBX_LOG_MISC, MDBX_LOGLEVEL_FATAL, line), func, "assert: %s\n", msg);
#ifndef _MSC_VER
  __assert_fail(msg, "mdbx", line, func);
#else
  _assert(msg, func, line);
#endif /* _MSC_VER */
}

void __cold mdbx_panic(const MDBX_env_t *env, MDBX_debuglog_subsystem_t subsystem, const char *func,
                       unsigned line, const char *fmt, ...) {
  /* TODO */
  (void)env;
  (void)subsystem;

  va_list ap;
  va_start(ap, fmt);

  char *message = nullptr;
  const int num = mdbx_vasprintf(&message, fmt, ap);
  va_end(ap);

  if (num < 1 || !message)
    message = "<troubles with message preparation>";

#if defined(_WIN32) || defined(_WIN64)
  if (IsDebuggerPresent()) {
    OutputDebugStringA(message);
    DebugBreak();
  }
  FatalExit(ERROR_UNHANDLED_ERROR);
#endif

#if MDBX_CONFIGURED_DEBUG_ABILITIES
  mdbx_dbglog(debug_event(subsystem, MDBX_LOGLEVEL_FATAL, line), func, "\n\rMDBX_PANIC: %s\n\r", message);
#else
  dprintf(STDERR_FILENO, "\n\rMDBX_PANIC: %s (%s.%d)\n\r", message, func, line);
#endif

  abort();
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL void mdbx_dbglog(const debug_event_t event, const char *function, const char *fmt, ...) {
  STATIC_ASSERT(MDBX_LOGLEVEL_EXTRA == 0 && MDBX_LOGLEVEL_FATAL == 7);
  STATIC_ASSERT(MDBX_LOG_MISC == 0 && MDBX_LOG_ALL < 0);

  const MDBX_debuglog_subsystem_t subsystem = event.subsystem;
  const MDBX_debuglog_level_t level = event.level;
  const unsigned line = event.line;

  va_list args;
  va_start(args, fmt);
  if (mdbx_debug_logger)
    mdbx_debug_logger(subsystem, level, function, line, fmt, args);
  else {

    /* #if _XOPEN_SOURCE >= 700 || _POSIX_C_SOURCE >= 200809L || \
        (__GLIBC_PREREQ(1, 0) && !__GLIBC_PREREQ(2, 10) && defined(_GNU_SOURCE))
    vdprintf(STDERR_FILENO, fmt, ap);
    #endif */

    if (function && line > 0)
      fprintf(stderr, "%s:%d ", function, line);
    else if (function)
      fprintf(stderr, "%s: ", function);
    else if (line > 0)
      fprintf(stderr, "%d: ", line);
    vfprintf(stderr, fmt, args);
  }
  va_end(args);
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL void mdbx_dbglog_continue(MDBX_debug_cookie_t cookie, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  mdbx_dbglog_continue_ap(cookie, fmt, ap);
  va_end(ap);
}

MDBX_INTERNAL MDBX_debug_cookie_t mdbx_dbglog_begin(const debug_event_t event, const char *function) {
  /* FIXME: TODO */
  MDBX_debug_cookie_t cookie = {0};
  (void)event;
  (void)function;
  return cookie;
}

MDBX_INTERNAL void mdbx_dbglog_continue_ap(MDBX_debug_cookie_t cookie, const char *fmt, va_list ap) {
  /* FIXME: TODO */
  (void)cookie;
  (void)fmt;
  (void)ap;
}

MDBX_INTERNAL void mdbx_dbglog_end(MDBX_debug_cookie_t cookie, const char *optional_final_msg) {
  /* FIXME: TODO */
  (void)cookie;
  (void)optional_final_msg;
}
