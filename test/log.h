/*
 * Copyright 2017-2018 Leonid Yuriev <leo@yuriev.ru>
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

#include "base.h"

void __noreturn usage(void);

void __noreturn __printf_args(1, 2) failure(const char *fmt, ...);

void __noreturn failure_perror(const char *what, MDBX_error_t errnum);
static inline void failure_perror(const char *what, int errnum) { failure_perror(what, (MDBX_error_t)errnum); }

const char *test_strerror(MDBX_error_t errnum);
static inline const char *test_strerror(int errnum) { return test_strerror((MDBX_error_t)errnum); }

namespace logging {

enum loglevel {
  extra = MDBX_LOGLEVEL_EXTRA,
  trace = MDBX_LOGLEVEL_TRACE,
  verbose = MDBX_LOGLEVEL_VERBOSE,
  info = MDBX_LOGLEVEL_INFO,
  notice = MDBX_LOGLEVEL_NOTICE,
  warning = MDBX_LOGLEVEL_WARNING,
  error = MDBX_LOGLEVEL_ERROR,
  failure = MDBX_LOGLEVEL_FATAL,
};

const char *level2str(const loglevel level);
void setup(loglevel level, const std::string &prefix);
void setup(const std::string &prefix);
void setlevel(loglevel level);

bool output(const loglevel priority, const char *format, va_list ap);
bool __printf_args(2, 3) output(const loglevel priority, const char *format, ...);
bool feed_ap(const char *format, va_list ap);
bool __printf_args(1, 2) feed(const char *format, ...);

class local_suffix {
protected:
  size_t trim_pos;
  int indent;

public:
  local_suffix(const local_suffix &) = delete;
  local_suffix(const local_suffix &&) = delete;
  const local_suffix &operator=(const local_suffix &) = delete;

  local_suffix(const char *c_str);
  local_suffix(const std::string &str);
  void push();
  void pop();
  ~local_suffix();
};

} // namespace logging

void __printf_args(1, 2) log_extra(const char *msg, ...);
void __printf_args(1, 2) log_trace(const char *msg, ...);
void __printf_args(1, 2) log_verbose(const char *msg, ...);
void __printf_args(1, 2) log_info(const char *msg, ...);
void __printf_args(1, 2) log_notice(const char *msg, ...);
void __printf_args(1, 2) log_warning(const char *msg, ...);
void __printf_args(1, 2) log_error(const char *msg, ...);

void log_trouble(const char *where, const char *what, int errnum);
void log_flush(void);
bool log_enabled(const logging::loglevel priority);

#ifdef _DEBUG
#define TRACE(...) log_trace(__VA_ARGS__)
#else
#define TRACE(...) __noop(__VA_ARGS__)
#endif
