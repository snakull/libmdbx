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

static const char *__mdbx_strerr(int errnum) {
  static const char *const tbl[MDBX_LAST_ERRCODE - MDBX_KEYEXIST] = {
      "MDBX_KEYEXIST: Key/data pair already exists",
      "MDBX_NOTFOUND: No matching key/data pair found",
      "MDBX_PAGE_NOTFOUND: Requested page not found",
      "MDBX_CORRUPTED: Databook is corrupted",
      "MDBX_PANIC: Update of meta page failed or databook had fatal error",
      "MDBX_VERSION_MISMATCH: Databook version mismatch libmdbx",
      "MDBX_INVALID: File is not an MDBX file",
      "MDBX_MAP_FULL: Databook mapsize limit reached",
      "MDBX_DBS_FULL: Too may AAH (maxdbs reached)",
      "MDBX_READERS_FULL: Too many readers (maxreaders reached)",
      "MDBX_TXN_FULL: Transaction has too many dirty pages - transaction too "
      "big",
      "MDBX_CURSOR_FULL: Internal error - cursor stack limit reached",
      "MDBX_PAGE_FULL: Internal error - page has no more space",
      "MDBX_MAP_RESIZED: Databook contents grew beyond mapsize limit",
      "MDBX_INCOMPATIBLE: Operation and AA incompatible, or AA flags changed",
      "MDBX_BAD_RSLOT: Invalid reuse of reader locktable slot",
      "MDBX_BAD_TXN: Transaction must abort, has a child, or is invalid",
      "MDBX_BAD_VALSIZE: Unsupported size of key/AA name/data, or wrong "
      "DUPFIXED size",
      "MDBX_BAD_AAH: The specified AAH handle was closed/changed unexpectedly",
      "MDBX_PROBLEM: Unexpected problem - txn should abort",
      "MDBX_EMULTIVAL: Unable to update multi-value for the given key",
      "MDBX_EBADSIGN: Wrong signature of a runtime object(s)",
      "MDBX_WANNA_RECOVERY: Databook should be recovered, but this could "
      "NOT be done in a read-only mode",
      "MDBX_EKEYMISMATCH: The given key value is mismatched to the "
      "current cursor position",
      "MDBX_TOO_LARGE: Databook is too large for current system, "
      "e.g. could NOT be mapped into RAM",
      "MDBX_THREAD_MISMATCH: A thread has attempted to use a not "
      "owned object, e.g. a transaction that started by another thread"};

  if (errnum >= MDBX_KEYEXIST && errnum < MDBX_LAST_ERRCODE)
    return tbl[errnum - MDBX_KEYEXIST];
  switch (errnum) {
  case MDBX_SUCCESS:
    return "MDBX_SUCCESS: Successful";
  case MDBX_EOF:
    return "MDBX_EOF/MDBX_SIGN: Not a failure, but significant condition";
  default:
    return nullptr;
  }
}

const char *__cold mdbx_strerror_r(MDBX_error_t errnum, char *buf, size_t buflen) {
  const char *msg = __mdbx_strerr(errnum);
  if (!msg) {
    if (!buflen || buflen > INT_MAX)
      return nullptr;
#ifdef _MSC_VER
    size_t size = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr, errnum,
                                 MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, (DWORD)buflen, nullptr);
    return size ? buf : nullptr;
#elif defined(_GNU_SOURCE)
    /* GNU-specific */
    msg = strerror_r(errnum, buf, buflen);
#elif (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)
    /* XSI-compliant */
    int rc = strerror_r(errnum, buf, buflen);
    if (rc) {
      rc = snprintf(buf, buflen, "error %d", errnum);
      assert(rc > 0);
    }
    return buf;
#else
    strncpy(buf, strerror(errnum), buflen);
    buf[buflen - 1] = '\0';
    return buf;
#endif
  }
  return msg;
}

const char *__cold mdbx_strerror(MDBX_error_t errnum) {
  const char *msg = __mdbx_strerr(errnum);
  if (!msg) {
#ifdef _MSC_VER
    static __thread char buffer[1024];
    size_t size = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr, errnum,
                                 MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buffer, sizeof(buffer), nullptr);
    if (size)
      msg = buffer;
#else
    msg = strerror(errnum);
#endif
  }
  return msg;
}
