/* https://en.wikipedia.org/wiki/Operating_system_abstraction_layer */

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

/*----------------------------------------------------------------------------*/
/* Microsoft compiler generates a lot of warning for self includes... */

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;                                     \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind                                     \
                                 * semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling                                 \
                                 * mode specified; termination on exception is                                \
                                 * not guaranteed. Specify /EHsc */
#if !defined(_CRT_SECURE_NO_WARNINGS)
#define _CRT_SECURE_NO_WARNINGS
#endif
#endif /* _MSC_VER (warnings) */

/*----------------------------------------------------------------------------*/
/* C99 includes */

#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <assert.h>
#include <fcntl.h>
#include <limits.h>
#include <malloc.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#ifndef _POSIX_C_SOURCE
#ifdef _POSIX_SOURCE
#define _POSIX_C_SOURCE 1
#else
#define _POSIX_C_SOURCE 0
#endif
#endif

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 0
#endif

/*----------------------------------------------------------------------------*/
/* Systems includes */

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#include <winnt.h>
#include <winternl.h>
#define HAVE_SYS_STAT_H
#define HAVE_SYS_TYPES_H
typedef HANDLE mdbx_thread_t;
typedef unsigned mdbx_thread_key_t;
#define MDBX_INVALID_FD INVALID_HANDLE_VALUE
#define MAP_FAILED NULL
#define HIGH_DWORD(v) ((DWORD)((sizeof(v) > 4) ? ((uint64_t)(v) >> 32) : 0))
#define THREAD_CALL WINAPI
#define THREAD_RESULT DWORD
typedef struct {
  HANDLE mutex;
  HANDLE event;
} mdbx_condmutex_t;
typedef CRITICAL_SECTION mdbx_fastmutex_t;
#define MDBX_PATH_SEPARATOR "\\"
#else
#include <pthread.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
typedef pthread_t mdbx_thread_t;
typedef pthread_key_t mdbx_thread_key_t;
#define MDBX_INVALID_FD (-1)
#define THREAD_CALL
#define THREAD_RESULT void *
typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
} mdbx_condmutex_t;
typedef pthread_mutex_t mdbx_fastmutex_t;
#define MDBX_PATH_SEPARATOR "/"
#endif /* Platform */

/* *INDENT-OFF* */
/* clang-format off */
#if defined(HAVE_SYS_STAT_H) || __has_include(<sys/stat.h>)
#include <sys/stat.h>
#endif
#if defined(HAVE_SYS_TYPES_H) || __has_include(<sys/types.h>)
#include <sys/types.h>
#endif
#if defined(HAVE_SYS_FILE_H) || __has_include(<sys/file.h>)
#include <sys/file.h>
#endif
/* *INDENT-ON* */
/* clang-format on */

#ifndef SSIZE_MAX
#define SSIZE_MAX INTPTR_MAX
#endif

#if defined(i386) || defined(__386) || defined(__i386) || defined(__i386__) || defined(i486) ||               \
    defined(__i486) || defined(__i486__) || defined(i586) | defined(__i586) || defined(__i586__) ||           \
    defined(i686) || defined(__i686) || defined(__i686__) || defined(_M_IX86) || defined(_X86_) ||            \
    defined(__THW_INTEL__) || defined(__I86__) || defined(__INTEL__) || defined(__x86_64) ||                  \
    defined(__x86_64__) || defined(__amd64__) || defined(__amd64) || defined(_M_X64) || defined(_M_AMD64) ||  \
    defined(__IA32__) || defined(__INTEL__)
#ifndef __ia32__
/* LY: define neutral __ia32__ for x86 and x86-64 archs */
#define __ia32__ 1
#endif /* __ia32__ */
#if !defined(__amd64__) && (defined(__x86_64) || defined(__x86_64__) || defined(__amd64) || defined(_M_X64))
/* LY: define trusty __amd64__ for all AMD64/x86-64 arch */
#define __amd64__ 1
#endif /* __amd64__ */
#endif /* all x86 */

#if !defined(UNALIGNED_OK)
#if (defined(__ia32__) || defined(__e2k__) || defined(__ARM_FEATURE_UNALIGNED)) && !defined(__ALIGNED__)
#define UNALIGNED_OK 1
#else
#define UNALIGNED_OK 0
#endif
#endif /* UNALIGNED_OK */

#if (-6 & 5) || CHAR_BIT != 8 || UINT_MAX < 0xffffffff || ULONG_MAX % 0xFFFF
#error "Sanity checking failed: Two's complement, reasonably sized integer types"
#endif

/*----------------------------------------------------------------------------*/
/* Compiler's includes for builtins/intrinsics */

#if defined(_MSC_VER) || defined(__INTEL_COMPILER)
#include <intrin.h>
#elif __GNUC_PREREQ(4, 4) || defined(__clang__)
#if defined(__ia32__) || defined(__e2k__)
#include <x86intrin.h>
#endif /* __ia32__ */
#if defined(__ia32__) && !defined(__cpuid_count)
#include <cpuid.h>
#endif /* __ia32__ */
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
#include <mbarrier.h>
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) && (defined(HP_IA64) || defined(__ia64))
#include <machine/sys/inline.h>
#elif defined(__IBMC__) && defined(__powerpc)
#include <atomic.h>
#elif defined(_AIX)
#include <builtins.h>
#include <sys/atomic_op.h>
#elif (defined(__osf__) && defined(__DECC)) || defined(__alpha)
#include <c_asm.h>
#include <machine/builtins.h>
#elif defined(__MWERKS__)
/* CodeWarrior - troubles ? */
#pragma gcc_extensions
#elif defined(__SNC__)
/* Sony PS3 - troubles ? */
#elif defined(__hppa__) || defined(__hppa)
#include <machine/inline.h>
#else
#error Unsupported C compiler, please use GNU C 4.4 or newer
#endif /* Compiler */

/*----------------------------------------------------------------------------*/
/* Byteorder */

#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__) || !defined(__ORDER_BIG_ENDIAN__)

/* *INDENT-OFF* */
/* clang-format off */
#if defined(__GLIBC__) || defined(__GNU_LIBRARY__) || defined(__ANDROID__) ||  \
    defined(HAVE_ENDIAN_H) || __has_include(<endian.h>)
#include <endian.h>
#elif defined(__APPLE__) || defined(__MACH__) || defined(__OpenBSD__) ||       \
    defined(HAVE_MACHINE_ENDIAN_H) || __has_include(<machine/endian.h>)
#include <machine/endian.h>
#elif defined(HAVE_SYS_ISA_DEFS_H) || __has_include(<sys/isa_defs.h>)
#include <sys/isa_defs.h>
#elif (defined(HAVE_SYS_TYPES_H) && defined(HAVE_SYS_ENDIAN_H)) ||             \
    (__has_include(<sys/types.h>) && __has_include(<sys/endian.h>))
#include <sys/endian.h>
#include <sys/types.h>
#elif defined(__bsdi__) || defined(__DragonFly__) || defined(__FreeBSD__) ||   \
    defined(__NETBSD__) || defined(__NetBSD__) ||                              \
    defined(HAVE_SYS_PARAM_H) || __has_include(<sys/param.h>)
#include <sys/param.h>
#endif /* OS */
/* *INDENT-ON* */
/* clang-format on */

#if defined(__BYTE_ORDER) && defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ __LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ __BIG_ENDIAN
#define __BYTE_ORDER__ __BYTE_ORDER
#elif defined(_BYTE_ORDER) && defined(_LITTLE_ENDIAN) && defined(_BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ _LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ _BIG_ENDIAN
#define __BYTE_ORDER__ _BYTE_ORDER
#else
#define __ORDER_LITTLE_ENDIAN__ 1234
#define __ORDER_BIG_ENDIAN__ 4321

#if defined(__LITTLE_ENDIAN__) || (defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN)) || defined(__ARMEL__) || \
    defined(__THUMBEL__) || defined(__AARCH64EL__) || defined(__MIPSEL__) || defined(_MIPSEL) ||              \
    defined(__MIPSEL) || defined(_M_ARM) || defined(_M_ARM64) || defined(__e2k__) ||                          \
    defined(__elbrus_4c__) || defined(__elbrus_8c__) || defined(__bfin__) || defined(__BFIN__) ||             \
    defined(__ia64__) || defined(_IA64) || defined(__IA64__) || defined(__ia64) || defined(_M_IA64) ||        \
    defined(__itanium__) || defined(__ia32__) || defined(__CYGWIN__) || defined(_WIN64) || defined(_WIN32) || \
    defined(__TOS_WIN__) || defined(__WINDOWS__)
#define __BYTE_ORDER__ __ORDER_LITTLE_ENDIAN__

#elif defined(__BIG_ENDIAN__) || (defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN)) || defined(__ARMEB__) ||  \
    defined(__THUMBEB__) || defined(__AARCH64EB__) || defined(__MIPSEB__) || defined(_MIPSEB) ||              \
    defined(__MIPSEB) || defined(__m68k__) || defined(M68000) || defined(__hppa__) || defined(__hppa) ||      \
    defined(__HPPA__) || defined(__sparc__) || defined(__sparc) || defined(__370__) ||                        \
    defined(__THW_370__) || defined(__s390__) || defined(__s390x__) || defined(__SYSC_ZARCH__)
#define __BYTE_ORDER__ __ORDER_BIG_ENDIAN__

#else
#error __BYTE_ORDER__ should be defined.
#endif /* Arch */

#endif
#endif /* __BYTE_ORDER__ || __ORDER_LITTLE_ENDIAN__ || __ORDER_BIG_ENDIAN__ */

/*----------------------------------------------------------------------------*/
/* Memory/Compiler barriers, cache coherence */

static inline void mdbx_compiler_barrier(void) {
#if defined(__clang__) || defined(__GNUC__)
  __asm__ __volatile__("" ::: "memory");
#elif defined(_MSC_VER)
  _ReadWriteBarrier();
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
  __memory_barrier();
  if (type > MDBX_BARRIER_COMPILER)
#if defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
    __mf();
#elif defined(__i386__) || defined(__x86_64__)
    _mm_mfence();
#else
#error "Unknown target for Intel Compiler, please report to us."
#endif
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
  __compiler_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) && (defined(HP_IA64) || defined(__ia64))
  _Asm_sched_fence(/* LY: no-arg meaning 'all expect ALU', e.g. 0x3D3D */);
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) || defined(__ppc64__) || defined(__powerpc64__)
  __fence();
#else
#error "Could not guess the kind of compiler, please report to us."
#endif
}

static inline void mdbx_memory_barrier(void) {
#if __has_extension(c_atomic) || __has_extension(cxx_atomic)
  __c11_atomic_thread_fence(__ATOMIC_SEQ_CST);
#elif defined(__ATOMIC_SEQ_CST)
  __atomic_thread_fence(__ATOMIC_SEQ_CST);
#elif defined(__clang__) || defined(__GNUC__)
  __sync_synchronize();
#elif defined(_MSC_VER)
  MemoryBarrier();
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
#if defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
  __mf();
#elif defined(__i386__) || defined(__x86_64__)
  _mm_mfence();
#else
#error "Unknown target for Intel Compiler, please report to us."
#endif
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
  __machine_rw_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) && (defined(HP_IA64) || defined(__ia64))
  _Asm_mf();
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) || defined(__ppc64__) || defined(__powerpc64__)
  __lwsync();
#else
#error "Could not guess the kind of compiler, please report to us."
#endif
}

/*----------------------------------------------------------------------------*/
/* Cache coherence and invalidation */

#ifndef MDBX_CACHE_IS_COHERENT
#if defined(__ia32__) || defined(__e2k__) || defined(__hppa) || defined(__hppa__)
#define MDBX_CACHE_IS_COHERENT 1
#else
#define MDBX_CACHE_IS_COHERENT 0
#endif
#endif /* MDBX_CACHE_IS_COHERENT */

#ifndef MDBX_CACHELINE_SIZE
#if defined(SYSTEM_CACHE_ALIGNMENT_SIZE)
#define MDBX_CACHELINE_SIZE SYSTEM_CACHE_ALIGNMENT_SIZE
#elif defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#define MDBX_CACHELINE_SIZE 128
#else
#define MDBX_CACHELINE_SIZE 64
#endif
#endif /* MDBX_CACHELINE_SIZE */

#if MDBX_CACHE_IS_COHERENT
#define mdbx_coherent_barrier() mdbx_compiler_barrier()
#else
#define mdbx_coherent_barrier() mdbx_memory_barrier()
#endif

#if defined(__mips) || defined(__mips__) || defined(__mips64) || defined(__mips64) || defined(_M_MRX000) ||   \
    defined(_MIPS_)
/* Only MIPS has explicit cache control */
#include <sys/cachectl.h>
#endif

static inline void mdbx_invalidate_cache(void *addr, size_t nbytes) {
  mdbx_coherent_barrier();
#if defined(__mips) || defined(__mips__) || defined(__mips64) || defined(__mips64) || defined(_M_MRX000) ||   \
    defined(_MIPS_)
#if defined(DCACHE)
  /* MIPS has cache coherency issues.
   * Note: for any nbytes >= on-chip cache size, entire is flushed. */
  cacheflush(addr, nbytes, DCACHE);
#else
#error "Sorry, cacheflush() for MIPS not implemented"
#endif /* __mips__ */
#else
  /* LY: assume no relevant mmap/dcache issues. */
  (void)addr;
  (void)nbytes;
#endif
}

/*----------------------------------------------------------------------------*/
/* libc compatibility stuff */

#if __GLIBC_PREREQ(2, 1)
#define mdbx_asprintf asprintf
#define mdbx_vasprintf vasprintf
#else
int mdbx_asprintf(char **strp, const char *fmt, ...);
int mdbx_vasprintf(char **strp, const char *fmt, va_list ap);
#endif

#ifdef _MSC_VER

#ifndef snprintf
#define snprintf(buffer, buffer_size, format, ...)                                                            \
  _snprintf_s(buffer, buffer_size, _TRUNCATE, format, __VA_ARGS__)
#endif /* snprintf */

#ifndef vsnprintf
#define vsnprintf(buffer, buffer_size, format, args) _vsnprintf_s(buffer, buffer_size, _TRUNCATE, format, args)
#endif /* vsnprintf */

#ifdef _ASSERTE
#undef assert
#define assert _ASSERTE
#endif

#endif /* _MSC_VER */

/*----------------------------------------------------------------------------*/
/* OS abstraction layer stuff */

/* max bytes to write in one call */
#define MAX_WRITE UINT32_C(0x3fff0000)

static inline char *mdbx_stpcpy(char *dest, const char *src) {
#if (defined(_XOPEN_SOURCE) && _XOPEN_SOURCE >= 700) ||                                                       \
    (defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200809L) ||                                               \
    (defined(_GNU_SOURCE) && __GLIBC_PREREQ(2, 10))
  return stpcpy(dest, src) + 1;
#else
  size_t bytes = strlen(src) + 1;
  memcpy(dest, src, bytes);
  return dest + bytes;
#endif
}

static inline int mdbx_get_errno(void) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD rc = GetLastError();
#else
  int rc = errno;
#endif
  return rc;
}

typedef struct mdbx_mmap_param {
  union {
    void *address;
    uint8_t *dxb;
    struct MDBX_lockinfo *lck;
  };
  MDBX_filehandle_t fd;
  size_t length; /* mapping length, but NOT a size of file or databook */
#if defined(_WIN32) || defined(_WIN64)
  size_t current; /* mapped region size, e.g. file and databook */
  uint64_t filesize;
  HANDLE section;
#endif
} mdbx_mmap_t;

#if defined(_WIN32) || defined(_WIN64)
typedef struct {
  unsigned limit, count;
  HANDLE handles[42];
} mdbx_handle_array_t;
static int mdbx_suspend_threads_before_remap(struct MDBX_env *env, mdbx_handle_array_t **array);
static int mdbx_resume_threads_after_remap(mdbx_handle_array_t *array);
#endif /* Windows */

/*----------------------------------------------------------------------------*/
/* Atomics */

#if !defined(__cplusplus) && (__STDC_VERSION__ >= 201112L) && !defined(__STDC_NO_ATOMICS__) &&                \
    (__GNUC_PREREQ(4, 9) || __CLANG_PREREQ(3, 8) || !(defined(__GNUC__) || defined(__clang__)))
#include <stdatomic.h>
#elif defined(__GNUC__) || defined(__clang__)
/* LY: nothing required */
#elif defined(_MSC_VER)
#pragma warning(disable : 4163) /* 'xyz': not available as an intrinsic */
#pragma warning(disable : 4133) /* 'function': incompatible types - from                                      \
                                   'size_t' to 'LONGLONG' */
#pragma warning(disable : 4244) /* 'return': conversion from 'LONGLONG' to                                    \
                                   'std::size_t', possible loss of data */
#pragma warning(disable : 4267) /* 'function': conversion from 'size_t' to                                    \
                                   'long', possible loss of data */
#pragma intrinsic(_InterlockedExchangeAdd, _InterlockedCompareExchange)
#pragma intrinsic(_InterlockedExchangeAdd64, _InterlockedCompareExchange64)
#elif defined(__APPLE__)
#include <libkern/OSAtomic.h>
#else
#error FIXME atomic-ops
#endif

static inline uint32_t mdbx_atomic_add32(volatile uint32_t *p, uint32_t v) {
#if !defined(__cplusplus) && defined(ATOMIC_VAR_INIT)
  assert(atomic_is_lock_free(p));
  STATIC_ASSERT(sizeof(atomic_uint) == 4);
  return atomic_fetch_add((atomic_uint *)p, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_fetch_and_add(p, v);
#else
#ifdef _MSC_VER
  return _InterlockedExchangeAdd(p, v);
#endif
#ifdef __APPLE__
  return OSAtomicAdd32(v, (volatile int32_t *)p);
#endif
#endif
}

static inline uint64_t mdbx_atomic_add64(volatile uint64_t *p, uint64_t v) {
#if !defined(__cplusplus) && defined(ATOMIC_VAR_INIT)
  assert(atomic_is_lock_free(p));
  STATIC_ASSERT(sizeof(atomic_ullong) == 8);
  return atomic_fetch_add((atomic_ullong *)p, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_fetch_and_add(p, v);
#else
#ifdef _MSC_VER
#ifdef _WIN64
  return _InterlockedExchangeAdd64((volatile int64_t *)p, v);
#else
  return InterlockedExchangeAdd64((volatile int64_t *)p, v);
#endif
#endif /* _MSC_VER */
#ifdef __APPLE__
  return OSAtomicAdd64(v, (volatile int64_t *)p);
#endif
#endif
}

#define mdbx_atomic_sub32(p, v) mdbx_atomic_add32(p, 0 - (v))
#define mdbx_atomic_sub64(p, v) mdbx_atomic_add64(p, 0 - (v))

static inline bool mdbx_atomic_compare_and_swap32(volatile uint32_t *p, uint32_t c, uint32_t v) {
#if !defined(__cplusplus) && defined(ATOMIC_VAR_INIT)
  assert(atomic_is_lock_free(p));
  STATIC_ASSERT(sizeof(atomic_uint) == 4);
  STATIC_ASSERT(sizeof(unsigned) == 4);
  return atomic_compare_exchange_strong((atomic_uint *)p, (unsigned *)&c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(p, c, v);
#else
#ifdef _MSC_VER
  return c == _InterlockedCompareExchange(p, v, c);
#endif
#ifdef __APPLE__
  return c == OSAtomicCompareAndSwap32Barrier(c, v, (volatile int32_t *)p);
#endif
#endif
}

static inline bool mdbx_atomic_compare_and_swap64(volatile uint64_t *p, uint64_t c, uint64_t v) {
#if !defined(__cplusplus) && defined(ATOMIC_VAR_INIT)
  assert(atomic_is_lock_free(p));
  STATIC_ASSERT(sizeof(atomic_ullong) == 8);
  STATIC_ASSERT(sizeof(unsigned long long) == 8);
  return atomic_compare_exchange_strong((atomic_ullong *)p, (unsigned long long *)&c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(p, c, v);
#else
#ifdef _MSC_VER
  return c == _InterlockedCompareExchange64((volatile int64_t *)p, v, c);
#endif
#ifdef __APPLE__
  return c == OSAtomicCompareAndSwap64Barrier(c, v, (volatile uint64_t *)p);
#endif
#endif
}

/*----------------------------------------------------------------------------*/

#if defined(_MSC_VER) && _MSC_VER >= 1900 && _MSC_VER < 1920
/* LY: MSVC 2015/2017 has buggy/inconsistent PRIuPTR/PRIxPTR macros
 * for internal format-args checker. */
#undef PRIuPTR
#undef PRIiPTR
#undef PRIdPTR
#undef PRIxPTR
#define PRIuPTR "Iu"
#define PRIiPTR "Ii"
#define PRIdPTR "Id"
#define PRIxPTR "Ix"
#define PRIuSIZE "zu"
#define PRIiSIZE "zi"
#define PRIdSIZE "zd"
#define PRIxSIZE "zx"
#endif /* fix PRI*PTR for _MSC_VER */

#ifndef PRIuSIZE
#define PRIuSIZE PRIuPTR
#define PRIiSIZE PRIiPTR
#define PRIdSIZE PRIdPTR
#define PRIxSIZE PRIxPTR
#endif /* PRI*SIZE macros for MSVC */

#ifdef _MSC_VER
#pragma warning(pop)
#endif
