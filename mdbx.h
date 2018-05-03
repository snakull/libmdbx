/* LICENSE AND COPYRUSTING *****************************************************
 *
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
 *
 * ---
 *
 * This code is derived from "LMDB engine" written by
 * Howard Chu (Symas Corporation), which itself derived from btree.c
 * written by Martin Hedenfalk.
 *
 * ---
 *
 * Portions Copyright 2011-2015 Howard Chu, Symas Corp. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * ---
 *
 * Portions Copyright (c) 2009, 2010 Martin Hedenfalk <martin@bzero.se>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. */

/* ACKNOWLEDGEMENTS ************************************************************
 *
 * Howard Chu (Symas Corporation) - the author of LMDB,
 * from which originated the MDBX in 2015.
 *
 * Martin Hedenfalk <martin@bzero.se> - the author of `btree.c` code,
 * which was used for begin development of LMDB. */

#pragma once

/*******************************************************************************

  TERMINOLOGY PACT
  and related suggestions:

  1) Use "ASSOCIATIVE ARRAY" instead of "MAP".

     MDBX is a storage that operates on a memory-mapped files so the
     term "mapped" would be overambiguous/vague.  Thus, it is better
     to strictly avoid the term "map" while referring to an associative
     container to prevent confusion with memory-mapped files.

     Next, in line with https://en.wikipedia.org/wiki/Associative_array
     generalization, choose the "ASSOCIATIVE ARRAY" rather than "dictionary"
     or "associative container".


  2) Use "DATA BOOK" instead of "DATABASE".

     Unfortunately, the term "database" is badly overloaded and can assume
     too many different things.  Particularly, it can equally well apply
     to both a collection of key-value pairs and the entire container the
     former resides; as such, it is better to be avoided to prevent ambiguity.

     Since MDBX container is essentially a set of sophisticatedly handled
     data pages, the term "DATA BOOK" is appropriate.

*******************************************************************************/

#ifndef LIBMDBX_H
#define LIBMDBX_H

/* IMPENDING CHANGES WARNING ***************************************************
 *
 * Now MDBX is under active development and until 2018Q3 is expected a
 * big change both of API and database format.  Unfortunately those update will
 * lead to loss of compatibility with previous versions.
 *
 * The aim of this revolution in providing a clearer robust API and adding new
 * features, including the database properties. */

/*------------------------------------------------------------------------------
 * Platform-dependent defines and types */

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;                                     \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind                                     \
                                 * semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling                                 \
                                 * mode specified; termination on exception is                                \
                                 * not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#include <assert.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* *INDENT-OFF* */
/* clang-format off */

#if defined(_WIN32) || defined(_WIN64)
# include <windows.h>
# include <winnt.h>
# ifndef __mode_t_defined
    typedef unsigned short mode_t;
# endif
  typedef HANDLE MDBX_filehandle_t;
  typedef DWORD MDBX_pid_t;
  typedef DWORD MDBX_tid_t;
#else
# include <errno.h>     /* for error codes */
# include <pthread.h>   /* for pthread_t */
# include <sys/types.h> /* for pid_t */
# include <sys/uio.h>   /* for truct iovec */
# define HAVE_STRUCT_IOVEC 1
  typedef int MDBX_filehandle_t;
  typedef pid_t MDBX_pid_t;
  typedef pthread_t MDBX_tid_t;
#endif

#ifdef _MSC_VER
# pragma warning(pop)
#endif

#ifndef __has_attribute
# define __has_attribute(x) (0)
#endif

#ifndef __dll_export
# if defined(_WIN32) || defined(__CYGWIN__)
#   if defined(__GNUC__) || __has_attribute(dllexport)
#     define __dll_export __attribute__((dllexport))
#   elif defined(_MSC_VER)
#     define __dll_export __declspec(dllexport)
#   else
#     define __dll_export
#   endif
# elif defined(__GNUC__) || __has_attribute(visibility)
#   define __dll_export __attribute__((visibility("default")))
# else
#   define __dll_export
# endif
#endif /* __dll_export */

#ifndef __dll_import
# if defined(_WIN32) || defined(__CYGWIN__)
#   if defined(__GNUC__) || __has_attribute(dllimport)
#       define __dll_import __attribute__((dllimport))
#   elif defined(_MSC_VER)
#       define __dll_import __declspec(dllimport)
#   else
#       define __dll_import
#   endif
# else
#   define __dll_import
# endif
#endif /* __dll_import */

#ifdef __cplusplus
  // Define operator overloads to enable bit operations on enum values that are
  // used to define flags (based on Microsoft's DEFINE_ENUM_FLAG_OPERATORS).
  // In MDBX we sure to that uint_fast32_t is enough for casting.
# define MDBX_ENUM_FLAG_OPERATORS(ENUMTYPE)                                    \
  extern "C++" {                                                               \
    inline ENUMTYPE operator|(ENUMTYPE a, ENUMTYPE b) {                        \
      return ENUMTYPE(((uint_fast32_t)a) | ((uint_fast32_t)b));                \
    }                                                                          \
    inline ENUMTYPE &operator|=(ENUMTYPE &a, ENUMTYPE b) {                     \
      return (ENUMTYPE &)(((uint_fast32_t &)a) |= ((uint_fast32_t)b));         \
    }                                                                          \
    inline ENUMTYPE operator&(ENUMTYPE a, ENUMTYPE b) {                        \
      return ENUMTYPE(((uint_fast32_t)a) & ((uint_fast32_t)b));                \
    }                                                                          \
    inline ENUMTYPE &operator&=(ENUMTYPE &a, ENUMTYPE b) {                     \
      return (ENUMTYPE &)(((uint_fast32_t &)a) &= ((uint_fast32_t)b));         \
    }                                                                          \
    inline ENUMTYPE operator~(ENUMTYPE a) {                                    \
      return ENUMTYPE(~((uint_fast32_t)a));                                    \
    }                                                                          \
    inline ENUMTYPE operator^(ENUMTYPE a, ENUMTYPE b) {                        \
      return ENUMTYPE(((uint_fast32_t)a) ^ ((uint_fast32_t)b));                \
    }                                                                          \
    inline ENUMTYPE &operator^=(ENUMTYPE &a, ENUMTYPE b) {                     \
      return (ENUMTYPE &)(((uint_fast32_t &)a) ^= ((uint_fast32_t)b));         \
    }                                                                          \
  }
#else /* __cplusplus */
# define MDBX_ENUM_FLAG_OPERATORS(ENUMTYPE) /* nope, C allows these operators */
#endif /* !__cplusplus */

/*------------------------------------------------------------------------------
 * Top-level MDBX defines and types */

#if defined(LIBMDBX_EXPORTS)
# define LIBMDBX_API __dll_export
#elif defined(LIBMDBX_IMPORTS)
# define LIBMDBX_API __dll_import
#else
# define LIBMDBX_API
#endif /* LIBMDBX_API */

#define LIBMDBX_API_INLINE static __inline

/* *INDENT-ON* */
/* clang-format on */

#ifdef __cplusplus
extern "C" {
#endif

/* The name of the lock file of the databook */
#define MDBX_LOCKNAME "mdbx.lck"
/* The name of the data file of the databook */
#define MDBX_DATANAME "mdbx.dat"
/* The suffix of the lock file when no subdir is used */
#define MDBX_LOCK_SUFFIX "-lck"

/* Opaque structure for a databook handle.
 *
 * A databook supports multiple associative arrays, all residing
 * in the same container (shared-memory map). */
typedef struct MDBX_env MDBX_env_t;

/* Fixed known beginning part of MDBX_env_t.
 * Pointer to MDBX_env_t could be safely casted to MDBX_env_base_t and back.
 * So, the userctx is accessible via the MDBX_env_t pointer without any
 * overhead.
 */
typedef struct MDBX_env_base {
  const size_t signature;
  void *userctx; /* User-settable context */
} MDXB_env_base_t;

/* Opaque structure for a transaction handle.
 *
 * All databook operations require a transaction handle. Transactions may be
 * read-only or read-write. */
typedef struct MDBX_txn MDBX_txn_t;

/* Fixed known beginning part of MDBX_txn_t.
 * Pointer to MDBX_txn_t could be safely casted to MDBX_txn_base_t and back. */
typedef struct MDBX_txn_base {
  const size_t signature;
  MDBX_env_t *const env;
  const uint64_t txnid;
  /* TODO: mdbx_clock_t data_timestamp; */
} MDXB_txn_base_t;

/* Opaque structure for navigating through an associative array */
typedef struct MDBX_cursor MDBX_cursor_t;

/* Fixed known beginning part of MDBX_cursor_t.
 * Pointer to MDBX_cursor_t could be safely casted to MDBX_cursor_base_t and back. */
typedef struct MDBX_cursor_base {
  const size_t signature;
  MDBX_txn_t *const txn;
} MDBX_cursor_base_t;

/*----------------------------------------------------------------------------*/
/* Error reporting, logging and Debug */

/* MDBX return codes */
typedef enum MDBX_error {
  /*--------------------------------------------------- Principal error codes */
  MDBX_SUCCESS = 0 /* Successful result */,
  MDBX_SIGN = -1 /* Not a failure, but significant condition */,
  MDBX_EOF = MDBX_SIGN,
#define MDBX_IS_ERROR(rc) ((rc) != MDBX_SUCCESS && (rc) != MDBX_SIGN)

  MDBX_LT = INT32_MIN,
  MDBX_EQ = 0,
  MDBX_GT = INT32_MAX,

  /*------------------------------------------------------ MDBX's error codes */
  MDBX_ERROR_BASE = -30765 /* BerkeleyDB uses -30800 to -30999, we'll go under them */,

  /* key/data pair already exists */
  MDBX_KEYEXIST,
  /* key/data pair not found (EOF) */
  MDBX_NOTFOUND,
  /* Requested page not found - this usually indicates corruption */
  MDBX_PAGE_NOTFOUND,
  /* Located page was wrong type */
  MDBX_CORRUPTED,
  /* Update of meta page failed or databook had fatal error */
  MDBX_PANIC,
  /* Databook file version mismatch with libmdbx */
  MDBX_VERSION_MISMATCH,
  /* File is not a valid MDBX file */
  MDBX_INVALID,
  /* Databook mapsize reached */
  MDBX_MAP_FULL,
  /* Databook maxdbs reached */
  MDBX_DBS_FULL,
  /* Databook maxreaders reached */
  MDBX_READERS_FULL,
  /* Txn has too many dirty pages */
  MDBX_TXN_FULL,
  /* Cursor stack too deep - internal error */
  MDBX_CURSOR_FULL,
  /* Page has not enough space - internal error */
  MDBX_PAGE_FULL,
  /* Databook contents grew beyond mapsize limit */
  MDBX_MAP_RESIZED,
  /* Operation and AA incompatible, or AA type changed. This can mean:
   *  - The operation expects an MDBX_DUPSORT / MDBX_DUPFIXED associative array.
   *  - Opening a named AA when the unnamed AA has MDBX_DUPSORT/MDBX_INTEGERKEY.
   *  - Accessing a data record as an associative arrays, or vice versa.
   *  - The associative arrays was dropped and recreated with different flags.
   */
  MDBX_INCOMPATIBLE,
  /* Invalid reuse of reader locktable slot */
  MDBX_BAD_RSLOT,
  /* Transaction must abort, has a child, or is invalid */
  MDBX_BAD_TXN,
  /* Unsupported size of key/AA name/data, or wrong DUPFIXED size */
  MDBX_BAD_VALSIZE,
  /* The specified AAH was changed unexpectedly */
  MDBX_BAD_AAH,
  /* Unexpected problem - txn should abort */
  MDBX_PROBLEM,

  /* The mdbx_put() or mdbx_replace() was called for key,
      that has more that one associated value. */
  MDBX_EMULTIVAL,

  /* Bad signature of a runtime object(s), this can mean:
   *  - memory corruption or double-free;
   *  - ABI version mismatch (rare case); */
  MDBX_EBADSIGN,

  /* Databook should be recovered, but this could NOT be done automatically
   * right now (e.g. in readonly mode and so forth). */
  MDBX_WANNA_RECOVERY,

  /* The given key value is mismatched to the current cursor position,
   * when mdbx_cursor_put() called with MDBX_IUD_CURRENT option. */
  MDBX_EKEYMISMATCH,

  /* Databook is too large for current system,
   * e.g. could NOT be mapped into RAM. */
  MDBX_TOO_LARGE,

  /* A thread has attempted to use a not owned object,
   * e.g. a transaction that started by another thread. */
  MDBX_THREAD_MISMATCH,

  /* FIXME: description */
  MDBX_EALREADY_OPENED,
  MDBX_EREFCNT_OVERFLOW,

  /* The last MDBX's error code */
  MDBX_LAST_ERRCODE,

/*---------------------------------------------- System-related error codes */
#if defined(_WIN32) || defined(_WIN64)
  MDBX_ENODATA = ERROR_HANDLE_EOF,
  MDBX_EINVAL = ERROR_INVALID_PARAMETER,
  MDBX_EACCESS = ERROR_ACCESS_DENIED,
  MDBX_ENOMEM = ERROR_OUTOFMEMORY,
  MDBX_EROFS = ERROR_FILE_READ_ONLY,
  MDBX_ENOSYS = ERROR_NOT_SUPPORTED,
  MDBX_EIO = ERROR_WRITE_FAULT,
  MDBX_EPERM = ERROR_INVALID_FUNCTION,
  MDBX_EINTR = ERROR_CANCELLED,
  MDBX_ENOENT = ERROR_FILE_NOT_FOUND,
  MDBX_EBUSY = ERROR_BUSY,
  MDBX_EWOULDBLOCK = ERROR_CANT_WAIT,
#else
  MDBX_ENODATA = ENODATA,
  MDBX_EINVAL = EINVAL,
  MDBX_EACCESS = EACCES,
  MDBX_ENOMEM = ENOMEM,
  MDBX_EROFS = EROFS,
  MDBX_ENOSYS = ENOSYS,
  MDBX_EIO = EIO,
  MDBX_EPERM = EPERM,
  MDBX_EINTR = EINTR,
  MDBX_ENOENT = ENOENT,
  MDBX_EBUSY = EBUSY,
  MDBX_EWOULDBLOCK = EWOULDBLOCK,
#endif
} MDBX_error_t;

/* Return a string describing a given error code.
 *
 * This function is a superset of the ANSI C X3.159-1989 (ANSI C) strerror(3)
 * function. If the error code is greater than or equal to 0, then the string
 * returned by the system function strerror(3) is returned. If the error code
 * is less than 0, an error string corresponding to the MDBX library error is
 * returned. See errors for a list of MDBX-specific error codes.
 *
 * [in] err The error code
 *
 * Returns "error message" The description of the error */
LIBMDBX_API const char *mdbx_strerror(MDBX_error_t errnum);
LIBMDBX_API const char *mdbx_strerror_r(MDBX_error_t errnum, char *buf, size_t buflen);

/* Logging levels for debugging */
typedef enum MDBX_debuglog_level {
  MDBX_LOGLEVEL_EXTRA = 0,
  MDBX_LOGLEVEL_TRACE,
  MDBX_LOGLEVEL_VERBOSE,
  MDBX_LOGLEVEL_INFO,
  MDBX_LOGLEVEL_NOTICE,
  MDBX_LOGLEVEL_WARNING,
  MDBX_LOGLEVEL_ERROR,
  MDBX_LOGLEVEL_FATAL,
} MDBX_debuglog_level_t;

/* Subsystems for logging */
typedef enum MDBX_debuglog_subsystem {
  MDBX_LOG_ALL = -1 /* special value, only for mdbx_set_loglevel() */,
  MDBX_LOG_MISC = 0 /* unspecified */,
  MDBX_LOG_RTHC /* rthc & thread-local strorage */,
  MDBX_LOG_LCK /* locking */,
  MDBX_LOG_META /* mega-pages & sync-points */,
  MDBX_LOG_GEO /* DB shrink/grown */,
  MDBX_LOG_IO /* I/O and sync-to-disk */,
  MDBX_LOG_AAH /* AA-handles */,
  MDBX_LOG_PAGE /* pages */,
  MDBX_LOG_GACO /* garbace collector */,
  MDBX_LOG_TXN /* transactions */,
  MDBX_LOG_CURSOR /* cursors */,
  MDBX_LOG_AUDIT /* audit */,
  MDBX_LOG_ENV,
  MDBX_LOG_MEM,
  MDBX_LOG_API,
  MDBX_LOG_CMP,
  MDBX_LOG_LIST,
  MDXB_LOG_MAX
} MDBX_debuglog_subsystem_t;

LIBMDBX_API MDBX_error_t mdbx_set_loglevel(MDBX_debuglog_subsystem_t subsystem, MDBX_debuglog_level_t level);

/* Debugging options */
typedef enum MDBX_debugbits {
  MDBX_DBG_ASSERT = 1u << 0 /* assertions */,
  MDBX_DBG_LOGGING = 1u << 1 /* logging */,
  MDBX_DBG_DUMP = 1u << 2 /* include DB content into code dump (i.e. include shared mapping) */,
  MDBX_DBG_JITTER = 1u << 3 /* random delay on sync-objects */,
  MDBX_DBG_AUDIT = 1u << 4 /* internal audit */,
} MDBX_debugbits_t;
MDBX_ENUM_FLAG_OPERATORS(MDBX_debugbits)

/* FIXME: Describe */
typedef void MDBX_debuglog_callback_t(MDBX_debuglog_subsystem_t subsystem, MDBX_debuglog_level_t level,
                                      const char *function, int line, const char *msg, va_list args);

/* FIXME: Describe */
typedef struct MDBX_debug_result {
  MDBX_debuglog_callback_t *logger;
  MDBX_debugbits_t bits;
} MDBX_debug_result_t;
LIBMDBX_API MDBX_debug_result_t mdbx_set_debug(MDBX_debugbits_t bits, MDBX_debuglog_callback_t *logger);

/* A callback function for most MDBX assertion failures.
*
* [in] env  An databook handle returned by mdbx_init(),
*           or NULL in case assertion at global scope.
* [in] msg  The assertion message, not including newline. */
typedef void MDBX_assert_callback_t(const MDBX_env_t *env, const char *msg, const char *function,
                                    unsigned line);
/* FIXME: Describe */
LIBMDBX_API MDBX_error_t mdbx_set_assert(MDBX_env_t *env, MDBX_assert_callback_t *func);

/*----------------------------------------------------------------------------*/
/* Basic types and core functions */

/* A handle for an individual associative array in the databook.
 *
 * Briefly, AA-handles is a runtime shortcuts that provide references to a
 * named special B-tree nodes and thus allows operating with more than single
 * associative array.
 *
 * The AA-handles opens by mdbx_aa_open() and then may be closed by
 * mdbx_aa_close() or mdbx_aa_drop(MDBX_CLOSE_HANDLE) with reference counting.
 * In other words, handle still live, until mdbx_aa_close() or
 * mdbx_aa_drop(MDBX_CLOSE_HANDLE) will be called for each corresponding
 * mdbx_aa_open() for given named associative array and the it handle.
 *
 * The mdbx_aa_open() could be called within or without transaction to increase
 * reference counter for the handle of given associative array name:
 *  - When be called WITHOUT TRANSACTION, mdbx_aa_open() just create the
 *    handle for given name or increase reference counter in case such handle
 *    is already present and valid.
 *  - When be called WITHIN TRANSACTION, prior to create a handle mdbx_aa_open()
 *    will check the presence of associative array with given name or create it
 *    in case write transaction and MDBX_CREATE option.
 *
 * The MDBX_SUCCESS returned from mdbx_aa_open() indicated that the reference
 * counter for a returned handle was increased and correspondingly call
 * mdbx_aa_close() is required to dispose it. Additionally, MDBX_INTERIM flag
 * could be used for mdbx_aa_open() with write transaction to automatically
 * call mdbx_aa_close() for returned handle at end of transaction regardless
 * of it will be committed or aborted.
 *
 * In most cases is no reason to closing a handles, excepting to free a slot
 * in the handle table keeping it small for less transaction overhead. On the
 * other hand, should be noted that closing a handle with simultaneously used
 * it in transaction(s) could lead to undefined behavior, including data
 * corruption and SIGSEGV. */
typedef uint_fast32_t MDBX_aah_t;
#define MDBX_INVALID_AAH UINT_FAST32_MAX

/* Predefined handle for carbage collector AA. */
#define MDBX_GACO_AAH 0
/* Predefined handle for main AA. */
#define MDBX_MAIN_AAH 1

/* Generic structure used for passing keys and data in and out
 * of the engine.
 *
 * Values returned from the databook are valid only until a subsequent
 * update operation, or the end of the transaction. Do not modify or
 * free them, they commonly point into the databook itself.
 *
 * Key sizes must be between 1 and bi_maxkeysize inclusive.
 * The same applies to data sizes in associative arrays with the MDBX_DUPSORT
 * flag. Other data items can in theory be from 0 to 0xffffffff bytes long. */
#ifdef HAVE_STRUCT_IOVEC
typedef struct iovec MDBX_iov_t;
#else
typedef struct iovec {
  void *iov_base;
  size_t iov_len;
} MDBX_iov_t;
#define HAVE_STRUCT_IOVEC
#endif /* HAVE_STRUCT_IOVEC */

/* The maximum size of a data item.
 * MDBX only store a 32 bit value for node sizes. */
#define MDBX_MAXDATASIZE INT32_MAX

/* A comparer-alike function used to compare two keys in an associative arrays */
typedef ptrdiff_t MDBX_comparer_t(const MDBX_iov_t left, const MDBX_iov_t right);

typedef enum MDBX_flags {
  MDBX_NOFLAGS = 0u,
  /*--------------------------------------------------- Associative Arrays */

  MDBX_INTEGERKEY /* numeric keys in native byte order, either uint32_t or uint64_t.
                   * The keys must all be of the same size. */ = 1u << 2,
  MDBX_REVERSEKEY /* use reverse string keys */ = 1u << 0,

  MDBX_DUPSORT /* use sorted duplicates */ = 1u << 1,

  MDBX_DUPFIXED /* with MDBX_DUPSORT, sorted dup items have fixed size */ = 1u << 3,

  MDBX_INTEGERDUP /* with MDBX_DUPSORT, dups are MDBX_INTEGERKEY-style integers */ = 1u << 4,

  MDBX_REVERSEDUP /* with MDBX_DUPSORT, use reverse string dups */ = 1u << 5,

  /* Custom comparison flags */
  MDBX_ALIEN_KCMP = 1u << 6 /* custom key comparer */,
  MDBX_ALIEN_DCMP = 1u << 7 /* custom data comparer */,
  MDBX_AA_FLAGS = MDBX_REVERSEKEY | MDBX_DUPSORT | MDBX_INTEGERKEY | MDBX_DUPFIXED | MDBX_INTEGERDUP |
                  MDBX_REVERSEDUP | MDBX_ALIEN_KCMP | MDBX_ALIEN_DCMP,

  MDBX_XYZ = 1u << 8 /* reserved */,
  MDBX_SLD = 1u << 9 /* reserved: Separated Large Data */,
  MDBX_CRC64 = 1u << 10 /* reserved: use CRC64 */,
  MDBX_T1HA = 1u << 11 /* reserved: use t1ha */,
  MDBX_MERKLE = 1u << 12 /* reserved: verification by Merkle-tree */,
  MDBX_DB_FLAGS = MDBX_SLD | MDBX_CRC64 | MDBX_T1HA | MDBX_MERKLE | MDBX_XYZ,

  MDBX_INTERIM = 1u << 13 /* auto closing a handle at end of transaction */,
  MDBX_CREATE = 1u << 14 /* create AA/DB if not already existing */,
  MDBX_NONBLOCK = 1u << 15 /* request lock-free guarantee */,
  MDBX_AA_OPEN_FLAGS = MDBX_AA_FLAGS | MDBX_CREATE | MDBX_NONBLOCK | MDBX_INTERIM,

  /* internal */
  MDBX_NOT_YET_IMPLEMENTED = MDBX_SLD | MDBX_CRC64 | MDBX_T1HA | MDBX_MERKLE | MDBX_XYZ,

  /*---------------------------------------------------- Running regime flags */
  /* read only */
  MDBX_RDONLY = 1u << 16,
  /* read-write */
  MDBX_RDWR = MDBX_NOFLAGS,
  /* use writable mmap */
  MDBX_WRITEMAP = 1u << 17,
  /* tie reader locktable slots to MDBX_txn_t objects instead of to threads */
  MDBX_NOTLS = 1u << 18,
  /* don't do readahead */
  MDBX_NORDAHEAD = 1u << 19,
  /* LIFO policy for reclaiming GACO records */
  MDBX_LIFORECLAIM = 1u << 20,
  /* Changing those flags requires closing the
   * databook and re-opening it with the new flags. */
  MDBX_REGIME_CHANGELESS =
      MDBX_RDONLY | MDBX_RDWR | MDBX_WRITEMAP | MDBX_NOTLS | MDBX_NORDAHEAD | MDBX_LIFORECLAIM,

  /* don't fsync after commit */
  MDBX_NOSYNC = 1u << 21,
  /* don't fsync metapage after commit */
  MDBX_NOMETASYNC = 1u << 22,
  /* use asynchronous msync when MDBX_WRITEMAP is used */
  MDBX_MAPASYNC = 1u << 23,
  /* don't initialize malloc'd memory before writing to datafile */
  MDBX_NOMEMINIT = 1u << 24,
  /* aim to coalesce GACO records */
  MDBX_COALESCE = 1u << 25,
  /* make a steady-sync only on close and explicit env-sync */
  MDBX_UTTERLY_NOSYNC = (MDBX_NOSYNC | MDBX_MAPASYNC),
  /* fill/perturb released pages */
  MDBX_PAGEPERTURB = 1u << 26,
  /* request exclusive mode, e.g. for check Databook */
  MDBX_EXCLUSIVE = 1u << 27,
  /* Those flags can be changed at runtime */
  MDBX_REGIME_CHANGEABLE = MDBX_NOSYNC | MDBX_NOMETASYNC | MDBX_MAPASYNC | MDBX_NOMEMINIT | MDBX_COALESCE |
                           MDBX_UTTERLY_NOSYNC | MDBX_PAGEPERTURB | MDBX_EXCLUSIVE,

  /* Flags valid for mdbx_open() */
  MDBX_DB_OPEN_FLAGS = MDBX_AA_OPEN_FLAGS | MDBX_REGIME_CHANGELESS | MDBX_REGIME_CHANGEABLE,

  /*----------------------------------------------------- Internal state bits */
  /* Environment state flags */
  /* custom locking, caller must manage their own locks */
  MDBX_ALIEN_LOCKING = 1u << 28 /* */,
  MDBX_ENV_TXKEY = 1u << 29 /* me_txkey is set */,
  MDBX_ENV_ACTIVE = 1u << 30 /* env active/busy */,
  MDBX_SHRINK_ALLOWED = MDBX_ENV_ACTIVE /* internal alias, only for mdbx_sync_locked() */,
  MDBX_ENV_TAINTED = 1u << 31 /* got a fatal error */,
  MDBX_ENV_FLAGS = MDBX_ENV_TXKEY | MDBX_ENV_ACTIVE | MDBX_SHRINK_ALLOWED | MDBX_ENV_TAINTED | MDBX_EXCLUSIVE,

  /* Internal transaction flags */
  MDBX_TXN_FINISHED = 1u << 0 /* txn is finished or never began */,
  MDBX_TXN_ERROR = 1u << 2 /* txn is unusable after an error */,
  MDBX_TXN_DIRTY = 1u << 3 /* must write, even if dirty list is empty */,
  MDBX_TXN_SPILLS = 1u << 4 /* txn or a parent has spilled pages */,
  MDBX_TXN_HAS_CHILD = 1u << 5 /* txn has a nested transaction */,
  MDBX_TXN_NONBLOCK = MDBX_NONBLOCK /* request lock-free guarantee */,

  MDBX_TXN_STATE_FLAGS =
      MDBX_TXN_FINISHED | MDBX_TXN_ERROR | MDBX_TXN_DIRTY | MDBX_TXN_SPILLS | MDBX_TXN_HAS_CHILD,

  /* most operations on the txn are currently illegal */
  MDBX_TXN_BLOCKED = (MDBX_TXN_FINISHED | MDBX_TXN_ERROR | MDBX_TXN_HAS_CHILD),

  MDBX_TXN_BEGIN_FLAGS =
      (MDBX_NOMETASYNC | MDBX_NOSYNC | MDBX_TXN_NONBLOCK | MDBX_RDONLY) /* mask for mdbx_begin() flags */,

  /* Principal operation mode */
  MDBX_REGIME_PRINCIPAL_FLAGS =
      MDBX_WRITEMAP | MDBX_NOSYNC | MDBX_NOMETASYNC | MDBX_MAPASYNC | MDBX_ALIEN_LOCKING,

} MDBX_flags_t;
MDBX_ENUM_FLAG_OPERATORS(MDBX_flags)

typedef union MDBX_id128 {
  uint8_t bytes[16];
  uint64_t qwords[2];
} MDBX_id128_t;

/* Information about of associative array in the databook */
typedef struct MDBX_aa_info {
  uint64_t ai_branch_pages;   /* Number of internal (non-leaf) pages */
  uint64_t ai_leaf_pages;     /* Number of leaf pages */
  uint64_t ai_overflow_pages; /* Number of overflow pages */
  uint64_t ai_entries;        /* Number of data items */
  uint64_t ai_sequence;       /* Sequrence counter */
  uint32_t ai_tree_depth;     /* Depth (height) of the B-tree */
  uint32_t ai_flags;
  MDBX_iov_t ai_ident;
} MDBX_aa_info_t;

/* Information about the databook */
typedef struct MDBX_db_info {
  struct {
    uint64_t lower;   /* lower limit for datafile size */
    uint64_t upper;   /* upper limit for datafile size */
    uint64_t current; /* current datafile size */
    uint64_t shrink;  /* shrink theshold for datafile */
    uint64_t grow;    /* growth step for datafile */
  } bi_geo;
  uint64_t bi_mapsize;                  /* size of the memory map section */
  uint64_t bi_last_pgno;                /* ID of the last used page */
  uint64_t bi_recent_txnid;             /* ID of the last committed transaction */
  uint64_t bi_latter_reader_txnid;      /* ID of the last reader transaction */
  uint64_t bi_self_latter_reader_txnid; /* ID of the last reader transaction of caller process */
  uint64_t bi_autosync_threshold;
  uint64_t bi_dirty_volume;
  struct {
    uint64_t txnid0, sign0;
    uint64_t txnid1, sign1;
    uint64_t txnid2, sign2;
  } bi_meta;
  uint32_t bi_maxkeysize;   /* maximum size of keys and MDBX_DUPSORT data */
  uint32_t bi_pagesize;     /* databook pagesize */
  uint32_t bi_sys_pagesize; /* system pagesize */
  uint32_t bi_dxb_ioblk;    /* system ioblocksize for dxb file */
  uint32_t bi_ovf_ioblk;    /* system ioblocksize for ovf file (if used) */

  uint32_t bi_regime;
  uint32_t bi_readers_max; /* max reader slots in the databook */
  uint32_t bi_readers_num; /* max reader slots used in the databook */
} MDBX_db_info_t;

/* A callback function for killing or waiting a laggard readers. Called in case
 * of MDBX_MAP_FULL error as described below for mdbx_set_rbr().
 *
 * [in] env     An databook handle returned by mdbx_init().
 * [in] pid     pid of the reader process.
 * [in] tid     thread_id of the reader thread.
 * [in] txn     Read transaction number on which stalled.
 * [in] gap     A lag from the last commited txn.
 * [in] retry   A retry number, will negative for notification
 *              about of end of RBR-loop.
 *
 * The return result is extremely important and determines further actions:
 *   MDBX_RBR_UNABLE  - To break the retry cycle and immediately raise MDBX_MAP_FULL error.
 *   MDBX_RBR_RETRY   - For retry. Assumes that the callback has already made a reasonable pause
 *                      or waiting, taken in account the passed arguments (mainly the retry).
 *   MDBX_RBR_EVICTED - Remove the reader's lock. Implies that reader's was aborted and will
 *                      not continue execute current transaction.
 *   MDBX_RBR_KILLED  - Drop reader registration. Implies that reader
 *                      process was already killed and reader slot should be cleared. */
typedef enum MDBX_rbr {
  MDBX_RBR_UNABLE = -1,
  MDBX_RBR_RETRY = 0,
  MDBX_RBR_EVICTED = 1,
  MDBX_RBR_KILLED = 2
} MDBX_rbr_t;
typedef MDBX_rbr_t MDBX_rbr_callback_t(MDBX_env_t *env, MDBX_pid_t pid, MDBX_tid_t tid, uint_fast64_t txn,
                                       unsigned gap, int retry);

/* Auxilary runtime/environment information */
typedef struct MDBX_aux_info {
  /* FIXME: Describe */
  uint32_t ei_aah_max;
  uint32_t ei_aah_num;
  MDBX_rbr_callback_t *ei_rbr_callback;
  MDBX_assert_callback_t *ei_assert_callback;

  MDBX_filehandle_t ei_dxb_fd;
  MDBX_filehandle_t ei_clk_fd;
  MDBX_filehandle_t ei_ovf_fd;

  /* Address of a string pointer to contain zero-separated
   * pathnames of databook files: data (dxb), lock (lck) and
   * optional overflow (OVF) files.
   * This is the actual string in the databook, not a copy.
   * It should not be altered in any way */
  const char *ei_pathnames;
} MDBX_aux_info_t;

LIBMDBX_API void *mdbx_malloc(size_t size, MDBX_env_t *env);
LIBMDBX_API void mdbx_free(void *ptr, MDBX_env_t *env);
LIBMDBX_API void *mdbx_calloc(size_t num, size_t size, MDBX_env_t *env);
LIBMDBX_API void *mdbx_realloc(void *ptr, size_t size, MDBX_env_t *env);
LIBMDBX_API void *mdbx_aligned_alloc(size_t alignment, size_t bytes, MDBX_env_t *env);
LIBMDBX_API void mdbx_aligned_free(void *ptr, MDBX_env_t *env);

typedef enum MDBX_seize {
  MDBX_SEIZE_FAILED,
  MDBX_SEIZE_NOLCK,
  MDBX_SEIZE_SHARED,
  MDBX_SEIZE_EXCLUSIVE_CONTINUE,
  MDBX_SEIZE_EXCLUSIVE_FIRST,
} MDBX_seize_t;

typedef struct MDBX_seize_result {
  MDBX_error_t err;
  MDBX_seize_t seize;
} MDBX_seize_result_t;

LIBMDBX_API MDBX_seize_result_t mdbx_lck_seize(MDBX_env_t *env,
                                               MDBX_flags_t flags /* MDBX_EXCLUSIVE, MDBX_NONBLOCK */);
LIBMDBX_API MDBX_error_t mdbx_lck_init(MDBX_env_t *env);
LIBMDBX_API MDBX_error_t mdbx_lck_downgrade(MDBX_env_t *env);
LIBMDBX_API MDBX_error_t mdbx_lck_upgrade(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */);
LIBMDBX_API void mdbx_lck_detach(MDBX_env_t *env);

LIBMDBX_API MDBX_error_t mdbx_lck_reader_registration_lock(MDBX_env_t *env,
                                                           MDBX_flags_t flags /* MDBX_NONBLOCK */);
LIBMDBX_API void mdbx_lck_reader_registration_unlock(MDBX_env_t *env);
LIBMDBX_API MDBX_error_t mdbx_lck_reader_alive_set(MDBX_env_t *env, MDBX_pid_t pid);
LIBMDBX_API MDBX_error_t mdbx_lck_reader_alive_clear(MDBX_env_t *env, MDBX_pid_t pid);
LIBMDBX_API MDBX_error_t mdbx_lck_reader_alive_check(MDBX_env_t *env, MDBX_pid_t pid);
LIBMDBX_API MDBX_error_t mdbx_lck_writer_lock(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */);
LIBMDBX_API void mdbx_lck_writer_unlock(MDBX_env_t *env);

typedef struct MDBX_ops {
  /* locking-ops – это Интерфейс, реализующий блокировки и синхронизацию.
   * В целом это очень специфическое API, которое не предназначено для легкого
   * и простого замещения внешней реализацией. Тем не менее, возможность
   * замещения locking-ops обусловлена двумя целями:
   *   1) поддержка режима «без блокировок» (MDB_NOLCK d в Symas LMDB),
   *      который может быть реализован пустыми заглушками или
   *      специфичной для приложения реализацией.
   *   2) возможность добавление прозрачной отладки/трассировки посредством
   *      пользовательских proxy-функций к предоставляемой реализации. */
  struct {
    /* Метод ops_seize() вызывается при инициализации экземпляра среды, т.е. при
     * открытии/подключении к БД, прежде всего для определения текущего состояния
     * LCK-файла. Иначе говоря ops_seize() всегда вызывается до ops_init().
     *
     * В свою очередь вызов метода ops_init() опционален и будет сделан только
     * для инициализации LCK-файла, если ops_seize() вернул MDBX_SEIZE_EXCLUSIVE_FIRST,
     * т.е. только когда требуется инициализация. Кроме этого, возврат MDBX_SEIZE_EXCLUSIVE_FIRST
     * означает что БД следует откатить к последней надежной точки фиксации.
     *
     * В противоположность этому, возврат MDBX_SEIZE_EXCLUSIVE_CONTINUE означает
     * что откат транзакций и инициализация LCK-файла может не выполнятся, так
     * как достоверно известно, что с момента последнего использования БД
     * операционная система не перезагружалась (продолжала выполняться без сбоев).
     *
     * ВАЖНО: Состояние блокировок после ops_seize() может не соответствовать
     * состоянию после вызова метода ops_upgrade(). Так при успешном выполнении
     * ops_upgrade(), т.е. при возврате им MDBX_SUCCESS, может удерживаться
     * блокировка уровня пишущей транзакции, чего не обязано быть после ops_seize().
     * Тем не менее, конкретное поведение является часть реализации и в свою
     * очередь метод ops_downgrade() должен отрабатывать корректно в обоих случаях,
     * как после эксклюзивного результата ops_seize(), так и после успешного
     * вызова ops_upgrade(). */
    MDBX_seize_result_t (*ops_seize)(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_EXCLUSIVE, MDBX_NONBLOCK */);
    MDBX_error_t (*ops_init)(MDBX_env_t *env);

    /* Метод ops_downgrade() вызывается для перехода из монопольного режима
     * использования БД в кооперативный. В основном ops_downgrade() вызывается
     * при открытии БД, в случае когда метод ops_seize() сообщил что удался захват
     * БД в монопольном режиме, но пользователь при открытии БД не заказывал
     * монопольного режима.
     *
     * Кроме этого, метод ops_downgrade() может быть вызван для возврата в
     * кооперативный режим, по требованию пользователя после предварительного
     * успешного перехода в монопольный режим посредством вызова ops_upgrade(). */
    MDBX_error_t (*ops_downgrade)(MDBX_env_t *env);

    /* Метод ops_upgrade() вызывается для получения эксклюзивного доступа к уже
     * открытой БД. В основном ops_upgrade() вызывается при закрытии БД для выяснения
     * необходимости создания надежной точки фиксации. В частности, если БД
     * закрывается в штатном режиме посредством mdbx_shutdown(), либо посредством
     * mdbx_shutdown_ex(MDBX_shutdown_default), то надежную точку фиксации будет
     * формировать только последний закрывающий БД процесс, открывший её в режиме
     * чтения-записи. Другими словами, при этом не производится формирование
     * контрольной точки, если по результату ops_upgrade() можно сделать вывод
     * что еще есть другие процессы-писатели работающие с данной БД.
     *
     * Кроме этого, метод ops_upgrade() может быть вызван для перехода в монопольный
     * режим по требованию пользователя, опционально с последующим возратом в
     * кооперативный режим, также по требованию пользователя.
     *
     * ops_upgrade() MUST returns:
     *   MDBX_SUCCESS - exclusive lock acquired,
     *                  NO other processes use DB.
     *   MDBX_BUSY    - exclusive lock NOT acquired, other processes use DB,
     *                  but not known ones are reader(s), writer(s) or mixed.
     *   MDBX_SIGN    - exclusive lock NOT acquired and SURE known that
     *                  other writer(s) is present, i.e. db-sync not required on close */
    MDBX_error_t (*ops_upgrade)(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */);

    /* FIXME: describe... */
    void (*ops_detach)(MDBX_env_t *env);
    /* Acquire lock for reader registration.
     * ops_reader_registration_lock() MUST returns:
     *   MDBX_SUCCESS - registration lock successfully acquired.
     *   MDBX_SIGN    - robust mutex recovered (previous owner dead)
     *                  and registration lock successfully acquired.
     *   MDBX_BUSY    - when MDBX_NONBLOCK specified in the flags argument
                        and registration lock NOT acquired because other process hold it.
     *   otherwise the errcode. */
    MDBX_error_t (*ops_reader_registration_lock)(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */);

    /* Release reader registration lock */
    void (*ops_reader_registration_unlock)(MDBX_env_t *env);

    /* FIXME: describe... */
    MDBX_error_t (*ops_reader_alive_set)(MDBX_env_t *env, MDBX_pid_t pid);

    /* FIXME: describe... */
    MDBX_error_t (*ops_reader_alive_clear)(MDBX_env_t *env, MDBX_pid_t pid);

    /* Checks reader by pid (Process ID)s MUST return:
     *   MDBX_SUCCESS, if pid is live (i.e. unable to acquire lock)
     *   MDBX_SIGN, if pid is dead (i.e. lock acquired)
     *   otherwise the errcode. */
    MDBX_error_t (*ops_reader_alive_check)(MDBX_env_t *env, MDBX_pid_t pid);

    /* Acquire lock for write transaction.
     * ops_writer_lock() MUST returns:
     *   MDBX_SUCCESS - write transaction lock successfully acquired, include
     *                  case when robust mutex recovered (previous owner dead).
     *   MDBX_BUSY    - when MDBX_NONBLOCK specified in the flags argument
                        and write transaction lock NOT acquired
                        because other process hold it (i.e. running a transaction).
     *   otherwise the errcode. */
    MDBX_error_t (*ops_writer_lock)(MDBX_env_t *env, MDBX_flags_t flags /* MDBX_NONBLOCK */);

    /* Release write transaction lock */
    void (*ops_writer_unlock)(MDBX_env_t *env);
  } locking;

  struct {
    void *(*ops_malloc)(size_t size, MDBX_env_t *env);
    void (*ops_free)(void *ptr, MDBX_env_t *env);
    void *(*ops_calloc)(size_t num, size_t size, MDBX_env_t *env);
    void *(*ops_realloc)(void *ptr, size_t size, MDBX_env_t *env);
    void *(*ops_aligned_alloc)(size_t alignment, size_t bytes, MDBX_env_t *env);
    void (*ops_aligned_free)(void *ptr, MDBX_env_t *env);
  } memory;
} MDBX_ops_t;

/*------------------------------------------------------------------------------
 * Databook Control functions */

/* Create an initialize an MDBX databook handle.
 *
 * This function allocates memory for a MDBX_env_t structure. To release
 * the allocated memory and discard the handle, call mdbx_shutdown().
 *
 * Before the handle may be used, it must be opened using mdbx_open().
 * Various other options may also need to be set before opening the handle,
 * depending on usage requirements:
 *  - mdbx_set_geometry() or mdbx_set_mapsize();
 *  - mdbx_set_maxreaders();
 *  - mdbx_set_maxhandles().
 *
 * [out] env The address where the new handle will be stored.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API MDBX_error_t mdbx_init(MDBX_env_t **pbk);
LIBMDBX_API MDBX_error_t mdbx_init_ex(MDBX_env_t **pbk, void *user_ctx, MDBX_ops_t *ops);

/* Open an databook.
 *
 * If this function fails, mdbx_shutdown() or mdbx_bk_close_ex() must be
 * called to discard the MDBX_env_t handle.
 *
 * This function creates its databook in a directory whose pathname is
 * given in the `path` argument, and creates its data and lock files
 * under that directory. For other operation modes (e.g. similar to LMDB's
 * MDB_NOSUBDIR option) advanced mdbx_open_ex() API should be used.
 *
 * [in] env    An databook handle returned by mdbx_init()
 * [in] path   The directory in which the databook files (LCK and DXB) reside.
 *             This directory must already exist and be writable.
 * [in] flags  Special options for this databook. This parameter
 *             must be set to 0 or by bitwise OR'ing together one
 *             or more of the values described here.
 *
 *  - MDBX_RDONLY
 *      Open the databook in read-only mode. No write operations will
 *      be allowed. MDBX will still modify the lock file - except on
 *      read-only filesystems, where MDBX does not use locks.
 *
 *  - MDBX_WRITEMAP
 *      Use a writeable memory map unless MDBX_RDONLY is set. This uses fewer
 *      mallocs but loses protection from application bugs like wild pointer
 *      writes and other bad updates into the databook.
 *      This may be slightly faster for DBs that fit entirely in RAM,
 *      but is slower for DBs larger than RAM.
 *      Incompatible with nested transactions.
 *      Do not mix processes with and without MDBX_WRITEMAP on the same
 *      databook.  This can defeat durability (mdbx_sync etc).
 *
 *  - MDBX_NOMETASYNC
 *      Flush system buffers to disk only once per transaction, omit the
 *      metadata flush. Defer that until the system flushes files to disk,
 *      or next non-MDBX_RDONLY commit or mdbx_sync(). This optimization
 *      maintains databook integrity, but a system crash may undo the last
 *      committed transaction. I.e. it preserves the ACI (atomicity,
 *      consistency, isolation) but not D (durability) databook property.
 *
 *  - MDBX_NOSYNC
 *      Don't flush system buffers to disk when committing a transaction.
 *      This optimization means a system crash can corrupt the databook or
 *      lose the last transactions if buffers are not yet flushed to disk.
 *      The risk is governed by how often the system flushes dirty buffers
 *      to disk and how often mdbx_sync() is called.  However, if the
 *      filesystem preserves write order and the MDBX_WRITEMAP and/or
 *      MDBX_LIFORECLAIM flags are not used, transactions exhibit ACI
 *      (atomicity, consistency, isolation) properties and only lose D
 *      (durability).  I.e. databook integrity is maintained, but a system
 *      crash may undo the final transactions.
 *
 *      Note that (MDBX_NOSYNC | MDBX_WRITEMAP) leaves the system with no
 *      hint for when to write transactions to disk.
 *      Therefore the (MDBX_MAPASYNC | MDBX_WRITEMAP) may be preferable.
 *
 *  - MDBX_UTTERLY_NOSYNC (internally MDBX_NOSYNC | MDBX_MAPASYNC)
 *      FIXME: TODO
 *
 *  - MDBX_MAPASYNC
 *      When using MDBX_WRITEMAP, use asynchronous flushes to disk. As with
 *      MDBX_NOSYNC, a system crash can then corrupt the databook or lose
 *      the last transactions. Calling mdbx_sync() ensures on-disk
 *      databook integrity until next commit.
 *
 *  - MDBX_NOTLS
 *      Don't use Thread-Local Storage. Tie reader locktable slots to
 *      MDBX_txn_t objects instead of to threads. I.e. mdbx_txn_reset() keeps
 *      the slot reseved for the MDBX_txn_t object. A thread may use parallel
 *      read-only transactions. A read-only transaction may span threads if
 *      the user synchronizes its use. Applications that multiplex many
 *      user threads over individual OS threads need this option. Such an
 *      application must also serialize the write transactions in an OS
 *      thread, since MDBX's write locking is unaware of the user threads.
 *
 *  - MDBX_ALIEN_LOCKING
 *      FIXME: TODO
 *
 *  - MDBX_NORDAHEAD
 *      Turn off readahead. Most operating systems perform readahead on
 *      read requests by default. This option turns it off if the OS
 *      supports it. Turning it off may help random read performance
 *      when the Databook is larger than RAM and system RAM is full.
 *
 *  - MDBX_NOMEMINIT
 *      Don't initialize malloc'd memory before writing to unused spaces
 *      in the data file. By default, memory for pages written to the data
 *      file is obtained using malloc. While these pages may be reused in
 *      subsequent transactions, freshly malloc'd pages will be initialized
 *      to zeroes before use. This avoids persisting leftover data from other
 *      code (that used the heap and subsequently freed the memory) into the
 *      data file. Note that many other system libraries may allocate and free
 *      memory from the heap for arbitrary uses. E.g., stdio may use the heap
 *      for file I/O buffers. This initialization step has a modest performance
 *      cost so some applications may want to disable it using this flag. This
 *      option can be a problem for applications which handle sensitive data
 *      like passwords, and it makes memory checkers like Valgrind noisy. This
 *      flag is not needed with MDBX_WRITEMAP, which writes directly to the
 *      mmap instead of using malloc for pages. The initialization is also
 *      skipped if MDBX_IUD_RESERVE is used; the caller is expected to overwrite
 *      all of the memory that was reserved in that case.
 *
 *  - MDBX_COALESCE
 *      Aim to coalesce records while reclaiming GACO.
 *      FIXME: TODO
 *
 *  - MDBX_LIFORECLAIM
 *      LIFO policy for reclaiming GACO records. This significantly reduce
 *      write IPOs in case MDBX_NOSYNC with periodically checkpoints.
 *      FIXME: TODO
 *
 * [in] mode4create The UNIX permissions to set on created files.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *   - MDBX_VERSION_MISMATCH - the version of the MDBX library doesn't match the
 *                             version that created the databook.
 *   - MDBX_INCOMPATIBLE     - the databook was already opened with
 *                             incompatible mode/flags, e.g. MDBX_WRITEMAP.
 *   - MDBX_INVALID  - the databook file headers are corrupted.
 *   - MDBX_ENOENT   - the directory specified by the path parameter
 *                     doesn't exist.
 *   - MDBX_EACCES   - the user didn't have permission to access
 *                     the databook files.
 *   - MDBX_EAGAIN   - the databook was locked by another process. */
LIBMDBX_API MDBX_error_t mdbx_open(MDBX_env_t *env, const char *path, MDBX_flags_t flags, mode_t mode4create);

/* Advanced open an databook.
 *
 * In comparison to mdbx_open() this function provides more control
 * and features:
 *  - create mapping with a given (fixed) address;
 *  - use specific LCK (lock) and DXB (data) files with an unrelated pathnames,
 *    e.g use a raw block devices at /dev/block;
 *  - use separate data files for large (aka overflow) pages,
 *    e.g. use a dedicated slow and cheap media for large data;
 *  - compatible opening with checking only the specific flags and accepting
 *    others, e.g. attaching to an databook which was already opened by
 *    another process with unknown mode/flags;
 *  - open databook in exclusive mode, e.g. for checking.
 *
 * In general, MDBX support a set of modes for files placement. But firstly,
 * the OVF-file should be noted:
 *  - It is optionals separate file for large data items (aka overflow pages);
 *  - Reasonably usage this dedicated file only when it placed on separate
 *    media, which implied to be cheap and slowly-seeking;
 *  - Therefore, when required it should be defined by while databook created,
 *    and then the same file must be preserved for every databook opening;
 *  - This file always defined by `ovf_pathname` argument, independently
 *    from other files and pathnames;
 *
 * Secondly, MDBX support three options for placement of data (DXB)
 * and lock (LCK) files:
 * 1. `subdir` mode: Databook files have default (predefined) names
 *    and resides in the directory, whose pathname is given by the argument.
 *    In this case `dxb_pathname` defines the directory pathname,
 *    but `lck_pathname` must be NULL;
 * 2. `no-subdir` mode: Pathname for DBX (data) file is given by the argument,
 *    and LCK (lock) file is same with MDBX_LOCK_SUFFIX appended. This is an
 *    equivalent for the LDMB's MDB_NOSUBDIR flag, which was removed from MDBX.
 *    In this case `dxb_pathname` defines DXB (data) file pathname,
 *    but `lck_pathname` MUST be just the "." string (one dot),
 *    which will be substituted as described above;
 * 3. `separate` mode: Both DXB (data) and LCK (lock) files placed
 *    independently, whose pathnames defines by two arguments.
 *    In this case both `dxb_pathname` and `lck_pathname` must be valid
 *    and `lck_pathname` must NOT be the "." (one dot).
 *
 *    NOTE: Should be avoided simultaneously use one DXB (data) file
 *    !!!!  with differ LCK (lock) files. Otherwise deadlock and/or
 *    !!!!  data corruption should be expected.
 *
 *
 * [in] env                  - An databook handle returned by mdbx_init().
 *
 * [in] target_base_address  - non-NULL value means that mapping for given
 *                             address is required, i.e. corresponding
 *                             to the LMDB's MDB_FIMEXMAP option.
 * [in] dxb_pathname         - Pathname of DXB (data) file or the directory in
 *                             which the DXB (data) and LCK (lock) files reside.
 *                             In the second case `lck_pathname` must be NULL.
 * [in] lck_pathname         - Optional pathname of LCK (lock) file. This param
 *                             should be NULL when `dxb_pathname` defines a
 *                             directory for databook files. Otherwise it
 *                             defines pathname of LCK (lock) file, which could
 *                             be placed separately.
 * [in] ovf_pathname         - The optional separate file for large (overflow)
 *                             pages. Reasonably usage this dedicated file only
 *                             when it placed on separate media. Therefore,
 *                             when required it should be defined while
 *                             databook created, and then the same file must
 *                             be preserved for every databook opening.
 * [in] regime_flags
 * [in] regime_check_mask
 * [out] regime_present
 * [in] mode4create
 */
/* TODO: FIXME (API MDBX_NONBLOCK) */
LIBMDBX_API MDBX_error_t mdbx_open_ex(MDBX_env_t *env, void *required_base_address, const char *dxb_pathname,
                                      const char *lck_pathname, const char *ovf_pathname,
                                      unsigned regime_flags, unsigned regime_check_mask,
                                      unsigned *regime_present, mode_t mode4create);

/* FIXME: Describe */
/* TODO */
LIBMDBX_API MDBX_error_t mdbx_open_fd(MDBX_env_t *env, void *required_base_address,
                                      MDBX_filehandle_t dxb_handle, MDBX_filehandle_t lck_handle,
                                      MDBX_filehandle_t ovf_handle, unsigned regime_flags,
                                      unsigned regime_check_mask, unsigned *regime_present,
                                      mode_t mode4create);

/* Return information about the MDBX databook.
 *
 * [in]  env        An databook handle returned by mdbx_init()
 * [out] info       The optional address of an MDBX_db_info_t structure
 *                  where the information will be copied.
 * [in]  info_size  Size in bytes of MDBX_db_info_t which the caller expects.
 * [out] env       The optional address of an MDBX_aux_info_t structure where
 *                  the runtime/environment will be copied
 * [in]  env_size   Size in bytes of MDBX_aux_info_t which the caller expects.
 */
LIBMDBX_API MDBX_error_t mdbx_info_ex(MDBX_env_t *env, MDBX_db_info_t *info, size_t info_size,
                                      MDBX_aux_info_t *aux, size_t aux_size);
LIBMDBX_API MDBX_error_t mdbx_info(MDBX_env_t *env, MDBX_db_info_t *info, size_t info_size);

/* Flush the data buffers to disk.
 *
 * Data is always written to disk when mdbx_commit() is called,
 * but the operating system may keep it buffered. MDBX always flushes
 * the OS buffers upon commit as well, unless the databook was
 * opened with MDBX_NOSYNC or in part MDBX_NOMETASYNC. This call is
 * not valid if the databook was opened with MDBX_RDONLY.
 *
 * [in] env   An databook handle returned by mdbx_init()
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EACCES   - the databook is read-only.
 *  - MDBX_EINVAL   - an invalid parameter was specified.
 *  - MDBX_EIO      - an error occurred during synchronization. */
LIBMDBX_API MDBX_error_t mdbx_sync(MDBX_env_t *env);

/* FIXME: Describe */
/* TODO */
LIBMDBX_API MDBX_error_t mdbx_sync_ex(MDBX_env_t *env, size_t dirty_volume_threshold, size_t txn_gap_threshold,
                                      size_t time_gap_threshold);

/* Close the databook and release the memory map.
 *
 * Only a single thread may call this function. All transactions, associative
 * arrays handles, and cursors must already be closed before calling this
 * function. Attempts to use any such handles after calling this function will
 * cause a SIGSEGV. The databook handle will be freed and must not be used
 * again after this call.
 *
 * [in] env        An databook handle returned by mdbx_init()
 * [in] dont_sync  A dont'sync flag, if non-zero the last checkpoint (meta-page
 *                 update) will be kept "as is" and may be still "weak" in the
 *                 NOSYNC/MAPASYNC modes. Such "weak" checkpoint will be
 *                 ignored on opening next time, and transactions since the
 *                 last non-weak checkpoint (meta-page update) will rolledback
 *                 for consistency guarantee.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EIO      - an error occurred during synchronization.
 *  - MDBX_EINVAL   - an invalid parameter was specified. */
typedef enum MDBX_shutdown_mode {
  MDBX_shutdown_default = 0,
  MDBX_shutdown_sync = 1,
  MDBX_shutdown_dirty = 2,
} MDBX_shutdown_mode_t;
LIBMDBX_API MDBX_error_t mdbx_shutdown_ex(MDBX_env_t *env, MDBX_shutdown_mode_t mode);
LIBMDBX_API MDBX_error_t mdbx_shutdown(MDBX_env_t *env);

/* Set the size of the memory map to use for this databook.
 *
 * The size should be a multiple of the OS page size. The default is
 * 10485760 bytes. The size of the memory map is also the maximum size
 * of the databook. The value should be chosen as large as possible,
 * to accommodate future growth of the databook.
 * This function should be called after mdbx_init() and before
 * mdbx_open(). It may be called at later times if no transactions
 * are active in this process. Note that the library does not check for
 * this condition, the caller must ensure it explicitly.
 *
 * The new size takes effect immediately for the current process but
 * will not be persisted to any others until a write transaction has been
 * committed by the current process. Also, only mapsize increases are
 * persisted into the databook.
 *
 * If the mapsize is increased by another process, and data has grown
 * beyond the range of the current mapsize, mdbx_begin() will
 * return MDBX_MAP_RESIZED. This function may be called with a size
 * of zero to adopt the new size.
 *
 * Any attempt to set a size smaller than the space already consumed by the
 * databook will be silently changed to the current size of the used space.
 *
 * [in] env  An databook handle returned by mdbx_init()
 * [in] size  The size in bytes
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   - an invalid parameter was specified,
 *                    or the databook has an active write transaction. */
LIBMDBX_API MDBX_error_t mdbx_set_mapsize(MDBX_env_t *env, size_t size);
/* FIXME: Describe */
LIBMDBX_API MDBX_error_t mdbx_set_geometry(MDBX_env_t *env, intptr_t size_lower, intptr_t size_now,
                                           intptr_t size_upper, intptr_t growth_step,
                                           intptr_t shrink_threshold, intptr_t pagesize);

/* Set the maximum number of threads/reader slots for the databook.
 *
 * This defines the number of slots in the lock table that is used to track
 * readers in the the databook. The default is 61.
 * Starting a read-only transaction normally ties a lock table slot to the
 * current thread until the databook closes or the thread exits. If
 * MDBX_NOTLS is in use, mdbx_begin() instead ties the slot to the
 * MDBX_txn_t object until it or the MDBX_env_t object is destroyed.
 * This function may only be called after mdbx_init() and before
 * mdbx_open().
 *
 * [in] env       An databook handle returned by mdbx_init()
 * [in] readers   The maximum number of reader lock table slots
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   - an invalid parameter was specified,
 *                    or the databook is already open. */
LIBMDBX_API MDBX_error_t mdbx_set_maxreaders(MDBX_env_t *env, unsigned readers);

/* Set the maximum number of named associative arrays for the databook.
 *
 * This function is only needed if multiple associative arrays will be used in
 * the databook. Simpler applications that use the databook as a single
 * unnamed associative array can ignore this option. This function may only
 * be called after mdbx_init() and before mdbx_open().
 *
 * Currently a moderate number of slots are cheap but a huge number gets
 * expensive: 7-120 dwords per transaction, and every mdbx_aa_open()
 * does a linear search of the opened slots.
 *
 * [in] env   An databook handle returned by mdbx_init()
 * [in] count The maximum number of associative arrays
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   - an invalid parameter was specified,
 *                    or the databook is already open. */
LIBMDBX_API MDBX_error_t mdbx_set_maxhandles(MDBX_env_t *env, size_t count);

/* Get the maximum size of keys and MDBX_DUPSORT data we can write
 * for given pagesize.
 *
 * Returns The maximum size of a key we can write. */
typedef struct MDBX_numeric_result {
  uintptr_t value;
  MDBX_error_t err;
} MDBX_numeric_result_t;
LIBMDBX_API MDBX_numeric_result_t mdbx_pagesize2maxkeylen(size_t pagesize);

/* Set application information associated with the MDBX_env_t.
 *
 * [in] env  An databook handle returned by mdbx_init()
 * [in] ctx  An arbitrary pointer for whatever the application needs.
 *
 * Returns A non-zero error value on failure and 0 on success. */
typedef struct MDBX_userctx_result {
  void *userctx;
  MDBX_error_t err;
} MDBX_userctx_result_t;
LIBMDBX_API MDBX_userctx_result_t mdbx_set_userctx(MDBX_env_t *env, void *ctx);
LIBMDBX_API_INLINE void *mdbx_set_userctx_unsafe(const MDBX_env_t *env, void *ctx) {
  void *prev_ctx = ((MDXB_env_base_t *)env)->userctx;
  ((MDXB_env_base_t *)env)->userctx = ctx;
  return prev_ctx;
}

/* Get the application information associated with the MDBX_env_t.
 *
 * [in] env An databook handle returned by mdbx_init()
 * Returns The pointer set by mdbx_set_userctx(). */
LIBMDBX_API MDBX_userctx_result_t mdbx_get_userctx(MDBX_env_t *env);
LIBMDBX_API_INLINE void *mdbx_get_userctx_unsafe(const MDBX_env_t *env) {
  return ((MDXB_env_base_t *)env)->userctx;
}

/* Set threshold to force flush the data buffers to disk,
 * even of MDBX_NOSYNC, MDBX_NOMETASYNC and MDBX_MAPASYNC flags
 * in the databook.
 *
 * Data is always written to disk when mdbx_commit() is called,
 * but the operating system may keep it buffered. MDBX always flushes
 * the OS buffers upon commit as well, unless the databook was
 * opened with MDBX_NOSYNC or in part MDBX_NOMETASYNC.
 *
 * The default is 0, than mean no any threshold checked, and no additional
 * flush will be made.
 *
 * [in] env     An databook handle returned by mdbx_init()
 * [in] bytes   The size in bytes of summary changes when a synchronous
 *              flush would be made.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API MDBX_error_t mdbx_set_syncbytes(MDBX_env_t *env, size_t bytes);

/* Set the "Reclaiming Blocked by Reader" callback.
 *
 * Callback will be called on no-free-pages condition when reclaiming of GACO
 * pages is blocked by at least one read transaction for killing or waiting this
 * slow reader.
 *
 * [in] env       An databook handle returned by mdbx_init().
 * [in] cb        A MDBX_rbr_callback function or NULL to disable.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API MDBX_error_t mdbx_set_rbr(MDBX_env_t *env, MDBX_rbr_callback_t *cb);

/* FIXME: Describe */
typedef MDBX_error_t MDBX_walk_func_t(uint_fast64_t pgno, unsigned pgnumber, void *ctx, const MDBX_iov_t ident,
                                      /* FIXME */ const char *type, size_t nentries, size_t payload_bytes,
                                      size_t header_bytes, size_t unused_bytes);
/* FIXME: Describe */
LIBMDBX_API MDBX_error_t mdbx_walk(MDBX_txn_t *txn, MDBX_walk_func_t *visitor, void *ctx);

/* A callback function used to print a message from the library.
 *
 * [in] msg   The string to be printed.
 * [in] ctx   An arbitrary context pointer for the callback.
 *
 * Returns < 0 on failure, >= 0 on success. */
typedef MDBX_error_t MDBX_readers_enum_func_t(void *ctx, unsigned index, MDBX_pid_t pid, MDBX_tid_t tid,
                                              uint_fast64_t txnid);

/* Dump the entries in the reader lock table.
 *
 * [in] env   An databook handle returned by mdbx_init()
 * [in] func  A MDBX_readers_enum_func function
 * [in] ctx   Anything the message function needs
 *
 * Returns < 0 on failure, >= 0 on success. */
LIBMDBX_API MDBX_error_t mdbx_readers_enum(MDBX_env_t *env, MDBX_readers_enum_func_t *func, void *ctx);

/* Check for stale entries in the reader lock table.
 *
 * [in] env     An databook handle returned by mdbx_init()
 * [out] dead   Number of stale slots that were cleared
 *
 * Returns 0 on success, non-zero on failure. */
LIBMDBX_API MDBX_numeric_result_t mdbx_readers_check(MDBX_env_t *env);

/* Copy Flags */
typedef enum MDBX_copy_flags {
  MDBX_CP_COMPACT = 1 /* Compacting copy: Omit free space from copy,
                       * and renumber all pages sequentially. */,
  MDBX_CP_NOSUBDIR = 2 /* no-subdir mode for destination */,
} MDBX_copy_flags_t;
MDBX_ENUM_FLAG_OPERATORS(MDBX_copy_flags)

/* Copy an MDBX databook to the specified path, with options.
 *
 * This function may be used to make a backup of an existing databook.
 * No lockfile is created, since it gets recreated at need.
 * NOTE: This call can trigger significant file size growth if run in
 * parallel with write transactions, because it employs a read-only
 * transaction. See long-lived transactions under "Caveats" section.
 *
 * [in] env    An databook handle returned by mdbx_init(). It must
 *             have already been opened successfully.
 * [in] path   The directory in which the copy will reside. This directory
 *             must already exist and be writable but must otherwise be empty.
 * [in] flags  Special options for this operation. This parameter must be set
 *             to 0 or by bitwise OR'ing together one or more of the values
 *             described here:
 *
 *  - MDBX_CP_COMPACT
 *      Perform compaction while copying: omit free pages and sequentially
 *      renumber all pages in output. This option consumes little bit more
 *      CPU for processing, but may running quickly than the default, on
 *      account skipping free pages.
 *
 *  - MDBX_CP_NOSUBDIR
 *      NOTE: It NOT fails if the databook has suffered a page leak (FIXME?).
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API MDBX_error_t mdbx_bk_copy(MDBX_env_t *env, const char *pathname, MDBX_copy_flags_t flags);

/* Copy an MDBX databook to the specified file descriptor with options.
 *
 * This function may be used to make a backup of an existing databook.
 * No lockfile is created, since it gets recreated at need. See
 * mdbx_bk_copy() for further details.
 *
 * NOTE: This call can trigger significant file size growth if run in
 * parallel with write transactions, because it employs a read-only
 * transaction. See long-lived transactions under "Caveats" section.
 *
 * [in] env     An databook handle returned by mdbx_init(). It must
 *              have already been opened successfully.
 * [in] fd      The filedescriptor to write the copy to. It must have already
 *              been opened for Write access.
 * [in] flags   Special options for this operation. See mdbx_bk_copy() for
 *              options.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API MDBX_error_t mdbx_bk_copy2fd(MDBX_env_t *env, MDBX_filehandle_t fd, MDBX_copy_flags_t flags);

/*------------------------------------------------------------------------------
 * Transaction Control functions */

/* Create a transaction for use with the databook.
 *
 * The transaction handle may be discarded using mdbx_abort()
 * or mdbx_commit().
 * NOTE: A transaction and its cursors must only be used by a single
 * thread, and a thread may only have a single transaction at a time.
 * If MDBX_NOTLS is in use, this does not apply to read-only transactions.
 * NOTE: Cursors may not span transactions.
 *
 * [in] env     An databook handle returned by mdbx_init()
 * [in] parent  If this parameter is non-NULL, the new transaction will be
 *              a nested transaction, with the transaction indicated by parent
 *              as its parent. Transactions may be nested to any level.
 *              A parent transaction and its cursors may not issue any other
 *              operations than mdbx_commit and mdbx_abort while it
 *              has active child transactions.
 * [in] flags   Special options for this transaction. This parameter
 *              must be set to 0 or by bitwise OR'ing together one or more
 *              of the values described here.
 *
 *  - MDBX_RDONLY
 *      This transaction will not perform any write operations.
 *
 * [out] txn Address where the new MDBX_txn_t handle will be stored
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_PANIC         - a fatal error occurred earlier and the databook
  *                        must be shut down.
 *  - MDBX_MAP_RESIZED   - another process wrote data beyond this MDBX_env_t's
 *                         mapsize and this databook's map must be resized
 *                         as well. See mdbx_set_mapsize().
 *  - MDBX_READERS_FULL  - a read-only transaction was requested and the reader
 *                         lock table is full. See mdbx_set_maxreaders().
 *  - MDBX_ENOMEM        - out of memory. */
typedef struct MDBX_txn_result {
  MDBX_txn_t *txn;
  MDBX_error_t err;
} MDBX_txn_result_t;
LIBMDBX_API MDBX_txn_result_t mdbx_begin(MDBX_env_t *env, MDBX_txn_t *parent, MDBX_flags_t flags);

/* Returns the transaction's MDBX_env_t
 *
 * [in] txn  A transaction handle returned by mdbx_begin() */
typedef struct MDBX_env_result {
  MDBX_env_t *env;
  MDBX_error_t err;
} MDBX_env_result_t;
LIBMDBX_API MDBX_env_result_t mdbx_txn_env(MDBX_txn_t *txn);
LIBMDBX_API_INLINE MDBX_env_t *mdbx_txn_env_unsafe(MDBX_txn_t *txn) { return ((MDXB_txn_base_t *)txn)->env; }

/* Return the transaction's flags.
 *
 * This returns the flags associated with this transaction.
 *
 * [in] txn A transaction handle returned by mdbx_begin()
 *
 * Returns A transaction flags, valid if input is an active transaction. */
LIBMDBX_API MDBX_numeric_result_t mdbx_txn_flags(MDBX_txn_t *txn);

/* Return the transaction's ID.
 *
 * This returns the identifier associated with this transaction. For a
 * read-only transaction, this corresponds to the snapshot being read;
 * concurrent readers will frequently have the same transaction ID.
 *
 * [in] txn A transaction handle returned by mdbx_begin()
 *
 * Returns A transaction ID, valid if input is an active transaction. */
typedef struct MDBX_txnid_result {
  uint_fast64_t txnid;
  MDBX_error_t err;
} MDBX_txnid_result_t;
LIBMDBX_API MDBX_txnid_result_t mdbx_txnid(MDBX_txn_t *txn);
LIBMDBX_API_INLINE uint_fast64_t mdbx_txnid_unsafe(MDBX_txn_t *txn) { return ((MDXB_txn_base_t *)txn)->txnid; }

/* Commit all the operations of a transaction into the databook.
 *
 * The transaction handle is freed. It and its cursors must not be used
 * again after this call, except with mdbx_cr_renew().
 *
 * A cursor must be closed explicitly always, before
 * or after its transaction ends. It can be reused with
 * mdbx_cr_renew() before finally closing it.
 *
 * [in] txn  A transaction handle returned by mdbx_begin()
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   - an invalid parameter was specified.
 *  - MDBX_ENOSPC   - no more disk space.
 *  - MDBX_EIO      - a low-level I/O error occurred while writing.
 *  - MDBX_ENOMEM   - out of memory. */
LIBMDBX_API MDBX_error_t mdbx_commit(MDBX_txn_t *txn);

/* Abandon all the operations of the transaction instead of saving them.
 *
 * The transaction handle is freed. It and its cursors must not be used
 * again after this call, except with mdbx_cr_renew().
 *
 * Opened AA handles will be closed. To save ones for
 *
 * A cursor must be closed explicitly always, before or after its transaction
 * ends. It can be reused with mdbx_cr_renew() before finally closing it.
 *
 * [in] txn  A transaction handle returned by mdbx_begin(). */
LIBMDBX_API MDBX_error_t mdbx_abort(MDBX_txn_t *txn);

/* Reset a read-only transaction.
 *
 * Abort the transaction like mdbx_abort(), but keep the transaction
 * handle. Therefore mdbx_txn_renew() may reuse the handle. This saves
 * allocation overhead if the process will start a new read-only transaction
 * soon, and also locking overhead if MDBX_NOTLS is in use. The reader table
 * lock is released, but the table slot stays tied to its thread or
 * MDBX_txn_t. Use mdbx_abort() to discard a reset handle, and to free
 * its lock table slot if MDBX_NOTLS is in use.
 *
 * Cursors opened within the transaction must not be used
 * again after this call, except with mdbx_cr_renew().
 *
 * Reader locks generally don't interfere with writers, but they keep old
 * versions of databook pages allocated. Thus they prevent the old pages
 * from being reused when writers commit new data, and so under heavy load
 * the databook size may grow much more rapidly than otherwise.
 *
 * [in] txn  A transaction handle returned by mdbx_begin() */
LIBMDBX_API MDBX_error_t mdbx_txn_reset(MDBX_txn_t *txn);

/* Renew a read-only transaction.
 *
 * This acquires a new reader lock for a transaction handle that had been
 * released by mdbx_txn_reset(). It must be called before a reset transaction
 * may be used again.
 *
 * [in] txn  A transaction handle returned by mdbx_begin()
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_PANIC     - a fatal error occurred earlier and the databook
 *                     must be shut down.
 *  - MDBX_EINVAL    - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_txn_renew(MDBX_txn_t *txn);

/* Returns a lag of the reading for the given transaction.
 *
 * Returns an information for estimate how much given read-only
 * transaction is lagging relative to the latest version of data.
 *
 * [in] txn          A transaction handle returned by mdbx_begin()
 * [out] alloc_ratio Optional pointer to store ratio of space allocation
 *                   in the databook multipled with 65536,
 *                   i.e. 32768 means 50% of current DB-file size is allocated.
 *
 * Returns Number of transactions committed after the given was started for
 * read, or -1 on failure. */
LIBMDBX_API MDBX_numeric_result_t mdbx_txn_lag(MDBX_txn_t *txn, unsigned *alloc_ratio);

/* FIXME: Describe */
typedef struct MDBX_canary { uint64_t x, y, z, v; } MDBX_canary_t;

/* FIXME: Describe */
LIBMDBX_API MDBX_error_t mdbx_canary_put(MDBX_txn_t *txn, const MDBX_canary_t *canary);
/* FIXME: Describe */
LIBMDBX_API MDBX_error_t mdbx_canary_get(MDBX_txn_t *txn, MDBX_canary_t *canary);

/* FIXME: Describe */
LIBMDBX_API MDBX_error_t mdbx_is_dirty(MDBX_txn_t *txn, const void *ptr);

/* Compare two keys according to a particular associative array.
 *
 * This returns a comparison as if the two data items were keys in the
 * specified associative array.
 *
 * [in] txn   A transaction handle returned by mdbx_begin()
 * [in] aah   A associative array handle returned by mdbx_aa_open()
 * [in] a     The first item to compare
 * [in] b     The second item to compare
 *
 * Returns < 0 if a < b, 0 if a == b, > 0 if a > b */
LIBMDBX_API MDBX_error_t mdbx_cmp(MDBX_txn_t *txn, MDBX_aah_t aah, const MDBX_iov_t *a, const MDBX_iov_t *b);

/* Compare two data items according to a particular associative array.
 *
 * This returns a comparison as if the two items were data items of
 * the specified associative array. The associative array must have the
 * MDBX_DUPSORT flag.
 *
 * [in] txn   A transaction handle returned by mdbx_begin()
 * [in] aah   A associative array handle returned by mdbx_aa_open()
 * [in] a     The first item to compare
 * [in] b     The second item to compare
 *
 * Returns < 0 if a < b, 0 if a == b, > 0 if a > b */
LIBMDBX_API MDBX_error_t mdbx_dcmp(MDBX_txn_t *txn, MDBX_aah_t aah, const MDBX_iov_t *a, const MDBX_iov_t *b);

/*------------------------------------------------------------------------------
 * Associative Array Control functions */

/* Open a table in the databook.
 *
 * A table handle denotes the name and parameters of a table, independently
 * of whether such a table exists. The table handle may be discarded by
 * calling mdbx_aa_close(). The old table handle is returned if the table
 * was already open. The handle may only be closed once.
 *
 * The table handle will be private to the current transaction until
 * the transaction is successfully committed. If the transaction is
 * aborted the handle will be closed automatically.
 * After a successful commit the handle will reside in the shared
 * environment, and may be used by other transactions.
 *
 * FIXME: This function must not be called from multiple concurrent
 * transactions in the same process. A transaction that uses
 * this function must finish (either commit or abort) before
 * any other transaction in the process may use this function.
 *
 * To use named table (with name != NULL), mdbx_set_maxhandles()
 * must be called before opening the databook. Table names are
 * keys in the internal unnamed table, and may be read but not written.
 *
 * [in] txn    transaction handle returned by mdbx_begin()
 * [in] name   The name of the table to open. If only a single
 *             table is needed in the databook, this value may be NULL.
 * [in] flags  Special options for this table. This parameter must be set
 *             to 0 or by bitwise OR'ing together one or more of the values
 *             described here:
 *  - MDBX_REVERSEKEY
 *      Keys are strings to be compared in reverse order, from the end
 *      of the strings to the beginning. By default, Keys are treated as
 *      strings and compared from beginning to end.
 *  - MDBX_DUPSORT
 *      Duplicate keys may be used in the table. Or, from another point of
 *      view, keys may have multiple data items, stored in sorted order. By
 *      default keys must be unique and may have only a single data item.
 *  - MDBX_INTEGERKEY
 *      Keys are binary integers in native byte order, either uin32_t or
 *      uint64_t, and will be sorted as such. The keys must all be of the
 *      same size.
 *  - MDBX_DUPFIXED
 *      This flag may only be used in combination with MDBX_DUPSORT. This
 *      option tells the library that the data items for this associative array
 *      are all the same size, which allows further optimizations in storage and
 *      retrieval. When all data items are the same size, the MDBX_GET_MULTIPLE,
 *      MDBX_NEXT_MULTIPLE and MDBX_PREV_MULTIPLE cursor operations may be used
 *      to retrieve multiple items at once.
 *  - MDBX_INTEGERDUP
 *      This option specifies that duplicate data items are binary integers,
 *      similar to MDBX_INTEGERKEY keys.
 *  - MDBX_REVERSEDUP
 *      This option specifies that duplicate data items should be compared as
 *      strings in reverse order (the comparison is performed in the direction
 *      from the last byte to the first).
 *  - MDBX_CREATE
 *      Create the named associative array if it doesn't exist. This option is
 *      not allowed in a read-only transaction or a read-only databook.
 *
 * [out]  aah Address where the new MDBX_handle handle will be stored
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  - the specified associative array doesn't exist in the
 *                     databook and MDBX_CREATE was not specified.
 *  - MDBX_DBS_FULL  - too many associative arrays have been opened.
 *                     See mdbx_set_maxhandles(). */
typedef struct MDBX_aah_result {
  MDBX_aah_t aah;
  MDBX_error_t err;
} MDBX_aah_result_t;

LIBMDBX_API MDBX_aah_result_t mdbx_aa_preopen(MDBX_env_t *env, const MDBX_iov_t aa_ident, MDBX_flags_t flags,
                                              MDBX_comparer_t *keycmp, MDBX_comparer_t *datacmp);
LIBMDBX_API MDBX_aah_result_t mdbx_aa_open(MDBX_txn_t *txn, const MDBX_iov_t aa_ident, MDBX_flags_t flags,
                                           MDBX_comparer_t *keycmp, MDBX_comparer_t *datacmp);
LIBMDBX_API MDBX_error_t mdbx_aa_addref(MDBX_env_t *env, MDBX_aah_t aah);

/* Close an associative array handle. Normally unnecessary.
 *
 * Use with care:
 * FIXME: This call is not mutex protected. Handles should only be closed by
 * a single thread, and only if no other threads are going to reference
 * the associative array handle or one of its cursors any further. Do not close
 * a handle if an existing transaction has modified its associative array.
 * Doing so can cause misbehavior from associative array corruption to errors
 * like MDBX_BAD_VALSIZE (since the AA name is gone).
 *
 * Closing an associative array handle is not necessary, but lets mdbx_aa_open()
 * reuse the handle value.  Usually it's better to set a bigger
 * mdbx_set_maxhandles(), unless that value would be large.
 *
 * [in] env  An databook handle returned by mdbx_init()
 * [in] aah  A associative array handle returned by mdbx_aa_open()
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API MDBX_error_t mdbx_aa_close(MDBX_env_t *env, MDBX_aah_t aah);

/* Flags for mdbx_aa_drop() */
/* FIXME: Describe */
typedef enum mdbx_drop_flags {
  MDBX_PURGE_AA = 0u,
  MDBX_DELETE_AA = 1u,
  MDBX_CLOSE_HANDLE = 2u
} mdbx_drop_flags_t;
MDBX_ENUM_FLAG_OPERATORS(mdbx_drop_flags)

/* Empty or delete an associative array.
 *
 * See mdbx_aa_close() for restrictions about closing the associative array
 * handle.
 *
 * [in] txn  A transaction handle returned by mdbx_begin()
 * [in] aah  A associative array handle returned by mdbx_aa_open()
 * [in] flags
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API MDBX_error_t mdbx_aa_drop(MDBX_txn_t *txn, MDBX_aah_t aah, mdbx_drop_flags_t flags);

/* Retrieve statistics for an associative array.
 *
 * [in]  txn        A transaction handle returned by mdbx_begin()
 * [in]  aah        A associative arrays handle returned by mdbx_aa_open()
 * [out] stat       The address of an MDBX_aa_info_t structure where the
 *                  statistics and info will be copied.
 * [in]  info_size  Size in bytes of MDBX_aa_info_t which the caller expects.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_aa_info(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_aa_info_t *info, size_t info_size);

/* Associative Array Handle flags returned by mdbx_aa_state(). */
typedef enum MDBX_aah_flags {
  MDBX_AAH_STALE = 0 /* AA-record was not loaded */,
  MDBX_AAH_VALID = 1 << 0 /* AA-record is update */,
  MDBX_AAH_ABSENT = 1 << 1,
  MDBX_AAH_BAD = 1 << 2,
  MDBX_AAH_DIRTY = 1 << 3 /* AA was written in this txn */,
  MDBX_AAH_DROPPED = 1 << 4,
  MDBX_AAH_CREATED = 1 << 5,

  /* Internal transaction AA Flags */
  MDBX_AAH_INTERIM = 1 << 8,
  MDBX_AAH_GACO = 1 << 9,
  MDBX_AAH_MAIN = 1 << 10,
  MDBX_AAH_DUPS = 1 << 11
} MDBX_aah_flags_t;
MDBX_ENUM_FLAG_OPERATORS(MDBX_aah_flags)

/* Retrieve the flags for an associative array handle.
 *
 * [in]  txn    A transaction handle returned by mdbx_begin()
 * [in]  aah    A associative array handle returned by mdbx_aa_open()
 * [out] state  The optional address where the state will be returned.
 *
 * Returns A non-zero error value on failure and 0 on success. */
typedef struct MDBX_state_result {
  MDBX_aah_flags_t state;
  MDBX_error_t err;
} MDBX_state_result_t;
LIBMDBX_API MDBX_state_result_t mdbx_aa_state(MDBX_txn_t *txn, MDBX_aah_t aah);

/* FIXME: Describe
 *   MDBX_SIGN - sequence overflow */
typedef struct MDBX_sequence_result {
  uint64_t value;
  MDBX_error_t err;
} MDBX_sequence_result_t;
LIBMDBX_API MDBX_sequence_result_t mdbx_aa_sequence(MDBX_txn_t *txn, MDBX_aah_t aah, uint64_t increment);

/*------------------------------------------------------------------------------
 * Direct key-value functions */

/* Insert/Update/Delete Flags */
typedef enum mdbx_iud_flags {
  MDBX_IUD_NODEFLAGS = 0xff /* lower 8 bits used for node flags */,
  /* For put: Don't write if the key already exists. */
  MDBX_IUD_NOOVERWRITE = 1u << 8,
  /* Only for associative arrays created winth MDBX_DUPSORT:
   * For put: don't write if the key and data pair already exist.
   * For delete: remove all duplicate data items. */
  MDBX_IUD_NODUP = 1u << 9,
  /* For mdbx_cursor_put: overwrite the current KV-pair.
   * For mdbx_put(): don't insert new KV-pair, but update present only. */
  MDBX_IUD_CURRENT = 1u << 10,
  /* For put: Just reserve space for data, don't copy it.
   * Return a pointer to the reserved space. */
  MDBX_IUD_RESERVE = 1u << 11,
  /* Data is being appended, don't split full pages. */
  MDBX_IUD_APPEND = 1u << 12,
  /* Duplicate data is being appended, don't split full pages. */
  MDBX_IUD_APPENDDUP = 1u << 13,
  /* Store multiple data items in one call. Only for MDBX_DUPFIXED. */
  MDBX_IUD_MULTIPLE = 1u << 14,
  /* Do not spill pages to disk if txn is getting full, may fail instead */
  MDBX_IUD_NOSPILL = 1u << 15,

  MDBX_IUD_FLAGS = MDBX_IUD_NOOVERWRITE | MDBX_IUD_NODUP | MDBX_IUD_CURRENT | MDBX_IUD_RESERVE |
                   MDBX_IUD_APPEND | MDBX_IUD_APPENDDUP | MDBX_IUD_MULTIPLE | MDBX_IUD_NOSPILL,
} mdbx_iud_flags_t;
MDBX_ENUM_FLAG_OPERATORS(mdbx_iud_flags)

/* Get items from an associative array.
 *
 * This function retrieves key/data pairs from the associative array. The
 * address and length of the data associated with the specified key are returned
 * in the structure to which data refers. If the associative array supports
 * duplicate keys (MDBX_DUPSORT) then the first data item for the key will be
 * returned. Retrieval of other items requires the use of mdbx_cursor_get().
 *
 * NOTE: The memory pointed to by the returned values is owned by the
 * associative array. The caller need not dispose of the memory, and may not
 * modify it in any way. For values returned in a read-only transaction
 * any modification attempts will cause a SIGSEGV.
 *
 * NOTE: Values returned from the associative array are valid only until a
 * subsequent update operation, or the end of the transaction.
 *
 * [in] txn       A transaction handle returned by mdbx_begin()
 * [in] aah       A associative array handle returned by mdbx_aa_open()
 * [in] key       The key to search for in the associative array
 * [in,out] data  The data corresponding to the key
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  - the key was not in the associative array.
 *  - MDBX_EINVAL    - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_get(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data);

/* Store items into an associative array.
 *
 * This function stores key/data pairs in the associative array. The default
 * behavior is to enter the new key/data pair, replacing any previously existing
 * key if duplicates are disallowed, or adding a duplicate data item if
 * duplicates are allowed (MDBX_DUPSORT).
 *
 * [in] txn    A transaction handle returned by mdbx_begin()
 * [in] aah    A associative array handle returned by mdbx_aa_open()
 * [in] key    The key to store in the associative array
 * [in,out]    data The data to store
 * [in] flags  Special options for this operation. This parameter must be
 *             set to 0 or by bitwise OR'ing together one or more of the
 *             values described here.
 *
 *  - MDBX_IUD_NODUP
 *      Enter the new key/data pair only if it does not already appear
 *      in the associative array. This flag may only be specified if the
 *      associative array was opened with MDBX_DUPSORT. The function will return
 *      MDBX_KEYEXIST if the key/data pair already appears in the associative
 *      array.
 *
 *  - MDBX_IUD_NOOVERWRITE
 *      Enter the new key/data pair only if the key does not already appear
 *      in the associative array. The function will return MDBX_KEYEXIST if the
 *      key already appears in the associative array, even if the associative
 *      array supports duplicates (MDBX_DUPSORT). The data parameter will be set
 *      to point to the existing item.
 *
 *  - MDBX_IUD_CURRENT
 *      Update an single existing entry, but not add new ones. The function
 *      will return MDBX_NOTFOUND if the given key not exist in the associative
 *      array. Or the MDBX_EMULTIVAL in case duplicates for the given key.
 *
 *  - MDBX_IUD_RESERVE
 *      Reserve space for data of the given size, but don't copy the given
 *      data. Instead, return a pointer to the reserved space, which the
 *      caller can fill in later - before the next update operation or the
 *      transaction ends. This saves an extra memcpy if the data is being
 *      generated later. MDBX does nothing else with this memory, the caller
 *      is expected to modify all of the space requested. This flag must not
 *      be specified if the associative array was opened with MDBX_DUPSORT.
 *
 *  - MDBX_IUD_APPEND
 *      Append the given key/data pair to the end of the associative array. This
 *      option allows fast bulk loading when keys are already known to be in the
 *      correct order. Loading unsorted keys with this flag will cause
 *      a MDBX_EKEYMISMATCH error.
 *
 *  - MDBX_IUD_APPENDDUP
 *      As above, but for sorted dup data.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_KEYEXIST
 *  - MDBX_MAP_FULL  - the databook is full, see mdbx_set_mapsize().
 *  - MDBX_TXN_FULL  - the transaction has too many dirty pages.
 *  - MDBX_EACCES    - an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL    - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_put(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                                  mdbx_iud_flags_t flags);

/* Delete items from an associative array.
 *
 * This function removes key/data pairs from the associative array.
 *
 * The data parameter is NOT ignored regardless the associative array does
 * support sorted duplicate data items or not. If the data parameter
 * is non-NULL only the matching data item will be deleted.
 *
 * This function will return MDBX_NOTFOUND if the specified key/data
 * pair is not in the associative array.
 *
 * [in] txn   A transaction handle returned by mdbx_begin()
 * [in] aah   A associative array handle returned by mdbx_aa_open()
 * [in] key   The key to delete from the associative array
 * [in] data  The data to delete
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EACCES   - an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL   - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_del_ex(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                                     mdbx_iud_flags_t flags);
LIBMDBX_API_INLINE MDBX_error_t mdbx_del(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data) {
  return mdbx_del_ex(txn, aah, key, data, (mdbx_iud_flags_t)0);
}

/* FIXME: Describe */
LIBMDBX_API MDBX_error_t mdbx_replace(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *new_data,
                                      MDBX_iov_t *old_data, mdbx_iud_flags_t flags);
/* FIXME: Describe */
/* Same as mdbx_get(), but:
 * 1) if values_count is not NULL, then returns the count
 *    of multi-values/duplicates for a given key.
 * 2) updates the key for pointing to the actual key's data inside Databook. */
LIBMDBX_API MDBX_error_t mdbx_get_ex(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                                     size_t *values_count);

/*------------------------------------------------------------------------------
 * Cursor functions */

/* Create a cursor handle.
 *
 * A cursor is associated with a specific transaction and associative array.
 * A cursor cannot be used when its associative array handle is closed.  Nor
 * when its transaction has ended, except with mdbx_cr_renew().
 * It can be discarded with mdbx_cursor_close().
 *
 * A cursor must be closed explicitly always, before
 * or after its transaction ends. It can be reused with
 * mdbx_cr_renew() before finally closing it.
 *
 * [in] txn      A transaction handle returned by mdbx_begin()
 * [in] aah      A associative array handle returned by mdbx_aa_open()
 * [out] cursor  Address where the new MDBX_cursor_t handle will be stored
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   - an invalid parameter was specified. */
typedef struct MDBX_cursor_result {
  MDBX_cursor_t *cursor;
  MDBX_error_t err;
} MDBX_cursor_result_t;
LIBMDBX_API MDBX_cursor_result_t mdbx_cursor_open(MDBX_txn_t *txn, MDBX_aah_t aah);

/* Close a cursor handle.
 *
 * The cursor handle will be freed and must not be used again after this call.
 * Its transaction must still be live if it is a write-transaction.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open() */
LIBMDBX_API MDBX_error_t mdbx_cursor_close(MDBX_cursor_t *cursor);

/* Renew a cursor handle.
 *
 * A cursor is associated with a specific transaction and associative array.
 * Cursors that are only used in read-only transactions may be re-used,
 * to avoid unnecessary malloc/free overhead. The cursor may be associated
 * with a new read-only transaction, and referencing the same associative array
 * handle as it was created with.
 *
 * This may be done whether the previous transaction is live or dead.
 * [in] txn     A transaction handle returned by mdbx_begin()
 * [in] cursor  A cursor handle returned by mdbx_cursor_open()
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_cursor_renew(MDBX_txn_t *txn, MDBX_cursor_t *cursor);

/* Return the cursor's transaction handle.
 *
 * [in] cursor A cursor handle returned by mdbx_cursor_open() */
LIBMDBX_API MDBX_txn_result_t mdbx_cursor_txn(const MDBX_cursor_t *cursor);
LIBMDBX_API_INLINE MDBX_txn_t *mdbx_cursor_txn_unsafe(const MDBX_cursor_t *cursor) {
  return ((const MDBX_cursor_base_t *)cursor)->txn;
}

/* Return the cursor's associative array handle.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open() */
LIBMDBX_API MDBX_aah_result_t mdbx_cursor_aah(const MDBX_cursor_t *cursor);

/* Cursor Get operations.
 *
 * This is the set of all operations for retrieving data
 * using a cursor. */
typedef enum MDBX_cursor_op {
  MDBX_FIRST /* Position at first key/data item */,
  MDBX_FIRST_DUP /* MDBX_DUPSORT-only: Position at first data item
                       * of current key. */,
  MDBX_GET_BOTH /* MDBX_DUPSORT-only: Position at key/data pair. */,
  MDBX_GET_BOTH_RANGE /* MDBX_DUPSORT-only: position at key, nearest data. */,
  MDBX_GET_CURRENT /* Return key/data at current cursor position */,
  MDBX_GET_MULTIPLE /* MDBX_DUPFIXED-only: Return key and up to a page of
                       * duplicate data items from current cursor position.
                       * Move cursor to prepare for MDBX_NEXT_MULTIPLE.*/,
  MDBX_LAST /* Position at last key/data item */,
  MDBX_LAST_DUP /* MDBX_DUPSORT-only: Position at last data item
                       * of current key. */,
  MDBX_NEXT /* Position at next data item */,
  MDBX_NEXT_DUP /* MDBX_DUPSORT-only: Position at next data item
                       * of current key. */,
  MDBX_NEXT_MULTIPLE /* MDBX_DUPFIXED-only: Return key and up to a page of
                       * duplicate data items from next cursor position.
                       * Move cursor to prepare for MDBX_NEXT_MULTIPLE. */,
  MDBX_NEXT_NODUP /* Position at first data item of next key */,
  MDBX_PREV /* Position at previous data item */,
  MDBX_PREV_DUP /* MDBX_DUPSORT-only: Position at previous data item
                       * of current key. */,
  MDBX_PREV_NODUP /* Position at last data item of previous key */,
  MDBX_SET /* Position at specified key */,
  MDBX_SET_KEY /* Position at specified key, return both key and data */,
  MDBX_SET_RANGE /* Position at first key greater than or equal to
                       * specified key. */,
  MDBX_PREV_MULTIPLE /* MDBX_DUPFIXED-only: Position at previous page and
                     * return key and up to a page of duplicate data items. */
} MDBX_cursor_op_t;

/* Retrieve by cursor.
 *
 * This function retrieves key/data pairs from the associative array. The
 * address and length of the key are returned in the object to which key refers
 * (except for the case of the MDBX_SET option, in which the key object is
 * unchanged), and the address and length of the data are returned in the object
 * to which data refers. See mdbx_get() for restrictions on using the output
 * values.
 *
 * [in] cursor    A cursor handle returned by mdbx_cursor_open()
 * [in,out] key   The key for a retrieved item
 * [in,out] data  The data of a retrieved item
 * [in] op        A cursor operation MDBX_cursor_op
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  - no matching key found.
 *  - MDBX_EINVAL    - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_cursor_get(MDBX_cursor_t *cursor, MDBX_iov_t *key, MDBX_iov_t *data,
                                         MDBX_cursor_op_t op);

/* Store by cursor.
 *
 * This function stores key/data pairs into the associative array. The cursor is
 * positioned at the new item, or on failure usually near it.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open()
 * [in] key     The key operated on.
 * [in] data    The data operated on.
 * [in] flags   Options for this operation. This parameter
 *              must be set to 0 or one of the values described here:
 *
 *  - MDBX_IUD_CURRENT
 *      Replace the item at the current cursor position. The key parameter
 *      must still be provided, and must match it, otherwise the function
 *      return MDBX_EKEYMISMATCH.
 *
 *      NOTE: MDBX unlike LMDB allows you to change the size of the data and
 *      automatically handles reordering for sorted duplicates (MDBX_DUPSORT).
 *
 *  - MDBX_IUD_NODUP
 *      Enter the new key/data pair only if it does not already appear in the
 *      associative array. This flag may only be specified if the associative
 *      array was opened with MDBX_DUPSORT. The function will return
 *      MDBX_KEYEXIST if the key/data pair already appears in the associative
 *      array.
 *
 *  - MDBX_IUD_NOOVERWRITE
 *      Enter the new key/data pair only if the key does not already appear
 *      in the associative array. The function will return MDBX_KEYEXIST if the
 *      key already appears in the associative array, even if the associative
 *      array supports duplicates (MDBX_DUPSORT).
 *
 *  - MDBX_IUD_RESERVE
 *      Reserve space for data of the given size, but don't copy the given
 *      data. Instead, return a pointer to the reserved space, which the
 *      caller can fill in later - before the next update operation or the
 *      transaction ends. This saves an extra memcpy if the data is being
 *      generated later. This flag must not be specified if the associative
 *      array was opened with MDBX_DUPSORT.
 *
 *  - MDBX_IUD_APPEND
 *      Append the given key/data pair to the end of the associative array.
 *      No key comparisons are performed. This option allows fast bulk loading
 *      when keys are already known to be in the correct order. Loading unsorted
 *      keys with this flag will cause a MDBX_KEYEXIST error.
 *
 *  - MDBX_IUD_APPENDDUP
 *      As above, but for sorted dup data.
 *
 *  - MDBX_IUD_MULTIPLE
 *      Store multiple contiguous data elements in a single request. This flag
 *      may only be specified if the associative array was opened with
 *      MDBX_DUPFIXED. The data argument must be an array of two MDBX_vals. The
 *      iov_len of the first MDBX_iov_t must be the size of a single data
 * element.
 *      The iov_base of the first MDBX_iov_t must point to the beginning of the
 *      array of contiguous data elements. The iov_len of the second MDBX_iov_t
 *      must be the count of the number of data elements to store. On return
 *      this field will be set to the count of the number of elements actually
 *      written. The iov_base of the second MDBX_iov_t is unused.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EKEYMISMATCH
 *  - MDBX_MAP_FULL  - the databook is full, see mdbx_set_mapsize().
 *  - MDBX_TXN_FULL  - the transaction has too many dirty pages.
 *  - MDBX_EACCES    - an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL    - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_cursor_put(MDBX_cursor_t *cursor, MDBX_iov_t *key, MDBX_iov_t *data,
                                         mdbx_iud_flags_t flags);

/* Delete current key/data pair
 *
 * This function deletes the key/data pair to which the cursor refers.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open()
 * [in] flags   Options for this operation. This parameter must be set to 0
 *              or one of the values described here.
 *
 *  - MDBX_IUD_NODUP
 *      Delete all of the data items for the current key. This flag may only
 *      be specified if the associative array was opened with MDBX_DUPSORT.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EACCES  - an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL  - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_cursor_delete(MDBX_cursor_t *cursor, mdbx_iud_flags_t flags);

/* Return count of duplicates for current key.
 *
 * This call is only valid on associative arrays that support sorted duplicate
 * data items MDBX_DUPSORT.
 *
 * [in] cursor    A cursor handle returned by mdbx_cursor_open()
 * [out] countp   Address where the count will be stored
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   - cursor is not initialized, or an invalid parameter
 *                    was specified. */
LIBMDBX_API MDBX_numeric_result_t mdbx_cursor_count(MDBX_cursor_t *cursor);

/* FIXME: Describe */
/* Returns:
 *  - MDBX_EOF
 *      when no more data available or cursor not positioned;
 *  - MDBX_SUCCESS
 *      when data available;
 *  - Otherwise the error code. */
LIBMDBX_API MDBX_error_t mdbx_cursor_eof(MDBX_cursor_t *cursor);

/* FIXME: Describe */
/* Returns: MDBX_SIGN, MDBX_SUCESS or Error code. */
LIBMDBX_API MDBX_error_t mdbx_cursor_at_first(MDBX_cursor_t *cursor);

/* FIXME: Describe */
/* Returns: MDBX_SIGN, MDBX_SUCCESS or Error code. */
LIBMDBX_API MDBX_error_t mdbx_cursor_at_last(MDBX_cursor_t *cursor);

/*----------------------------------------------------------------------------*/
/* Attribute support functions for Nexenta */
typedef uint_fast64_t MDBX_attr_t;

/* Store by cursor with attribute.
 *
 * This function stores key/data pairs into the associative array. The cursor is
 * positioned at the new item, or on failure usually near it.
 *
 * NOTE: Internally based on MDBX_IUD_RESERVE feature,
 *       therefore doesn't support MDBX_DUPSORT.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open()
 * [in] key     The key operated on.
 * [in] data    The data operated on.
 * [in] attr    The attribute.
 * [in] flags   Options for this operation. This parameter must be set to 0
 *              or one of the values described here:
 *
 *  - MDBX_IUD_CURRENT
 *      Replace the item at the current cursor position. The key parameter
 *      must still be provided, and must match it, otherwise the function
 *      return MDBX_EKEYMISMATCH.
 *
 *  - MDBX_IUD_APPEND
 *      Append the given key/data pair to the end of the associative array.
 *      No key comparisons are performed. This option allows fast bulk loading
 *      when keys are already known to be in the correct order. Loading unsorted
 *      keys with this flag will cause a MDBX_KEYEXIST error.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EKEYMISMATCH
 *  - MDBX_MAP_FULL  - the database is full, see mdbx_set_mapsize().
 *  - MDBX_TXN_FULL  - the transaction has too many dirty pages.
 *  - MDBX_EACCES    - an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL    - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_cursor_put_attr(MDBX_cursor_t *cursor, MDBX_iov_t *key, MDBX_iov_t *data,
                                              MDBX_attr_t attr, mdbx_iud_flags_t flags);

/* Store items and attributes into an associative array.
 *
 * This function stores key/data pairs in the associative array. The default
 * behavior is to enter the new key/data pair, replacing any previously existing
 * key if duplicates are disallowed.
 *
 * NOTE: Internally based on MDBX_IUD_RESERVE feature,
 *       therefore doesn't support MDBX_DUPSORT.
 *
 * [in] txn       A transaction handle returned by mdbx_begin().
 * [in] aah       A associative array handle returned by mdbx_aa_open().
 * [in] key       The key to store in the associative array.
 * [in] attr      The attribute to store in the associative array.
 * [in,out] data  The data to store.
 * [in] flags     Special options for this operation. This parameter must be
 *                set to 0 or by bitwise OR'ing together one or more of the
 *                values described here:
 *
 *  - MDBX_IUD_NOOVERWRITE
 *      Enter the new key/data pair only if the key does not already appear
 *      in the associative array. The function will return MDBX_KEYEXIST
 *      if the key already appears in the associative array. The data parameter
 *      will be set to point to the existing item.
 *
 *  - MDBX_IUD_CURRENT
 *      Update an single existing entry, but not add new ones. The function
 *      will return MDBX_NOTFOUND if the given key not exist in the associative
 *      array. Or the MDBX_EMULTIVAL in case duplicates for the given key.
 *
 *  - MDBX_IUD_APPEND
 *      Append the given key/data pair to the end of the associative array.
 *      This option allows fast bulk loading when keys are already known to be
 *      in the correct order. Loading unsorted keys with this flag will cause
 *      a MDBX_EKEYMISMATCH error.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_KEYEXIST
 *  - MDBX_MAP_FULL  - the database is full, see mdbx_set_mapsize().
 *  - MDBX_TXN_FULL  - the transaction has too many dirty pages.
 *  - MDBX_EACCES    - an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL    - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_put_attr(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                                       MDBX_attr_t attr, mdbx_iud_flags_t flags);

/* Set items attribute inside an associative array.
 *
 * This function stores key/data pairs attribute to the associative array.
 *
 * NOTE: Internally based on MDBX_IUD_RESERVE feature,
 *       therefore doesn't support MDBX_DUPSORT.
 *
 * [in] txn   A transaction handle returned by mdbx_begin().
 * [in] aah   A associative array handle returned by mdbx_aa_open().
 * [in] key   The key to search for in the associative array.
 * [in] data  The data to be stored or NULL to save previous value.
 * [in] attr  The attribute to be stored.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *	 - MDBX_NOTFOUND   - the key-value pair was not in the associative
 *array.
 *	 - MDBX_EINVAL     - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_set_attr(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                                       MDBX_attr_t attr);

/* Get items attribute from an associative array cursor.
 *
 * This function retrieves key/data pairs from the associative array. The
 * address and  length of the key are returned in the object to which key
 * refers (except for the case of the MDBX_SET option, in which the key object
 * is unchanged), and the address and length of the data are returned in the
 * object to which data refers. See mdbx_get() for restrictions on using the
 * output values.
 *
 * [in] cursor    A cursor handle returned by mdbx_cursor_open()
 * [in,out] key   The key for a retrieved item
 * [in,out] data  The data of a retrieved item
 * [in] op        A cursor operation MDBX_cursor_op
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  - no matching key found.
 *  - MDBX_EINVAL    - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_cursor_get_attr(MDBX_cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data,
                                              MDBX_attr_t *attrptr, MDBX_cursor_op_t op);

/* Get items attribute from an associative array.
 *
 * This function retrieves key/data pairs from the associative array. The
 * address and length of the data associated with the specified key are returned
 * in the structure to which data refers.
 * If the associative array supports duplicate keys (MDBX_DUPSORT) then the
 * first data item for the key will be returned. Retrieval of other
 * items requires the use of mdbx_cursor_get().
 *
 * NOTE: The memory pointed to by the returned values is owned by the
 * engine. The caller need not dispose of the memory, and may not
 * modify it in any way. For values returned in a read-only transaction
 * any modification attempts will cause a SIGSEGV.
 *
 * NOTE: Values returned from the engine are valid only until a
 * subsequent update operation, or the end of the transaction.
 *
 * [in] txn       A transaction handle returned by mdbx_begin()
 * [in] aah       A associative array handle returned by mdbx_aa_open()
 * [in] key       The key to search for in the associative array
 * [in,out] data  The data corresponding to the key
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  - the key was not in the associative array.
 *  - MDBX_EINVAL    - an invalid parameter was specified. */
LIBMDBX_API MDBX_error_t mdbx_get_attr(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data,
                                       MDBX_attr_t *attrptr);

/*------------------------------------------------------------------------------
 * Misc functions */

/* FIXME: Describe */
LIBMDBX_API char *mdbx_dump_iov(const MDBX_iov_t *iov, char *const buf, const size_t bufsize);

/* FIXME: Describe */
LIBMDBX_API MDBX_error_t mdbx_is_directory(const char *pathname);

/* FIXME: Describe */
LIBMDBX_API void mdbx_jitter(char /* bool */ tiny);

/* FIXME: Describe */
LIBMDBX_API MDBX_iov_t mdbx_str2iov(const char *str);

/* FIXME: Describe */
LIBMDBX_API_INLINE bool mdbx_iov_eq(const MDBX_iov_t *a, const MDBX_iov_t *b) {
  return a->iov_len == b->iov_len && memcmp(a->iov_base, b->iov_base, a->iov_len) == 0;
}

/* FIXME: Describe */
LIBMDBX_API_INLINE bool mdbx_iov_eq_str(const MDBX_iov_t *a, const char *b) {
  return a->iov_len == (b ? strlen(b) : 0) && memcmp(a->iov_base, b, a->iov_len) == 0;
}

/* FIXME: Describe */
LIBMDBX_API MDBX_error_t mdbx_iov_dup(MDBX_iov_t *iov);

/* FIXME: Describe */
LIBMDBX_API MDBX_id128_t mdbx_bootid(void);

/*----------------------------------------------------------------------------*/
/* Version & Build info */

#define MDBX_VERSION_MAJOR 1
#define MDBX_VERSION_MINOR 0

typedef struct mdbx_version_info {
  uint8_t major;
  uint8_t minor;
  uint16_t release;
  uint32_t revision;
  struct {
    const char *datetime;
    const char *tree;
    const char *commit;
    const char *describe;
  } git;
} mdbx_version_info_t;

typedef struct mdbx_build_info {
  const char *datetime;
  const char *target;
  const char *options;
  const char *compiler;
  const char *flags;
  MDBX_debugbits_t debug_abilities;
} mdbx_build_info_t;

extern LIBMDBX_API const mdbx_version_info_t mdbx_version;
extern LIBMDBX_API const mdbx_build_info_t mdbx_build;

/*----------------------------------------------------------------------------*/
/* LY: temporary workaround for Elbrus's memcmp() bug. */
#ifndef __GLIBC_PREREQ
#if defined(__GLIBC__) && defined(__GLIBC_MINOR__)
#define __GLIBC_PREREQ(maj, min) ((__GLIBC__ << 16) + __GLIBC_MINOR__ >= ((maj) << 16) + (min))
#else
#define __GLIBC_PREREQ(maj, min) (0)
#endif
#endif /* __GLIBC_PREREQ */
#if defined(__e2k__) && !__GLIBC_PREREQ(2, 24)
LIBMDBX_API int mdbx_e2k_memcmp_bug_workaround(const void *s1, const void *s2, size_t n);
LIBMDBX_API int mdbx_e2k_strcmp_bug_workaround(const char *s1, const char *s2);
LIBMDBX_API int mdbx_e2k_strncmp_bug_workaround(const char *s1, const char *s2, size_t n);
LIBMDBX_API size_t mdbx_e2k_strlen_bug_workaround(const char *s);
LIBMDBX_API size_t mdbx_e2k_strnlen_bug_workaround(const char *s, size_t maxlen);
#include <string.h>
#include <strings.h>
#undef memcmp
#define memcmp mdbx_e2k_memcmp_bug_workaround
#undef bcmp
#define bcmp mdbx_e2k_memcmp_bug_workaround
#undef strcmp
#define strcmp mdbx_e2k_strcmp_bug_workaround
#undef strncmp
#define strncmp mdbx_e2k_strncmp_bug_workaround
#undef strlen
#define strlen mdbx_e2k_strlen_bug_workaround
#undef strnlen
#define strnlen mdbx_e2k_strnlen_bug_workaround

#endif /* Elbrus's memcmp() bug. */

#ifdef __cplusplus
}
#endif

#endif /* LIBMDBX_H */
