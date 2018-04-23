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

#define mem_extra(fmt, ...) log_extra(MDBX_LOG_MEM, fmt, ##__VA_ARGS__)
#define mem_trace(fmt, ...) log_trace(MDBX_LOG_MEM, fmt, ##__VA_ARGS__)
#define mem_verbose(fmt, ...) log_verbose(MDBX_LOG_MEM, fmt, ##__VA_ARGS__)
#define mem_info(fmt, ...) log_info(MDBX_LOG_MEM, fmt, ##__VA_ARGS__)
#define mem_notice(fmt, ...) log_notice(MDBX_LOG_MEM, fmt, ##__VA_ARGS__)
#define mem_warning(fmt, ...) log_warning(MDBX_LOG_MEM, fmt, ##__VA_ARGS__)
#define mem_error(fmt, ...) log_error(MDBX_LOG_MEM, fmt, ##__VA_ARGS__)
#define mem_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_MEM, __func__, __LINE__, "%s, error %d", msg, err)

/*----------------------------------------------------------------------------*/

void __weak *mdbx_malloc(size_t size, MDBX_env_t *env) {
  (void)env;
  return malloc(size);
}

void __weak mdbx_free(void *ptr, MDBX_env_t *env) {
  (void)env;
  free(ptr);
}

void __weak *mdbx_calloc(size_t num, size_t size, MDBX_env_t *env) {
  (void)env;
  return calloc(num, size);
}

void __weak *mdbx_realloc(void *ptr, size_t size, MDBX_env_t *env) {
  (void)env;
  return realloc(ptr, size);
}

void *__weak mdbx_aligned_alloc(size_t alignment, size_t bytes, MDBX_env_t *env) {
  (void)env;
#if _MSC_VER
  return _aligned_malloc(bytes, alignment);
#elif __GLIBC_PREREQ(2, 16) || __STDC_VERSION__ >= 201112L
  return memalign(alignment, bytes);
#elif _POSIX_VERSION >= 200112L
  void *ptr = nullptr;
  return (posix_memalign(&ptr, alignment, bytes) == 0) ? ptr : nullptr;
#else
#error FIXME
#endif
}

void __weak mdbx_aligned_free(void *ptr, MDBX_env_t *env) {
  (void)env;
#if _MSC_VER
  _aligned_free(ptr);
#else
  free(ptr);
#endif
}

/*----------------------------------------------------------------------------*/

/* LY: temporary workaround for Elbrus's memcmp() bug. */
#if defined(__e2k__) && !__GLIBC_PREREQ(2, 24)
int __hot mdbx_e2k_memcmp_bug_workaround(const void *s1, const void *s2, size_t n) {
  if (unlikely(n > 42
               /* LY: align followed access if reasonable possible */ &&
               (((uintptr_t)s1) & 7) != 0 && (((uintptr_t)s1) & 7) == (((uintptr_t)s2) & 7))) {
    if (((uintptr_t)s1) & 1) {
      const int diff = *(uint8_t *)s1 - *(uint8_t *)s2;
      if (diff)
        return diff;
      s1 = (char *)s1 + 1;
      s2 = (char *)s2 + 1;
      n -= 1;
    }

    if (((uintptr_t)s1) & 2) {
      const uint16_t a = *(uint16_t *)s1;
      const uint16_t b = *(uint16_t *)s2;
      if (likely(a != b))
        return (__builtin_bswap16(a) > __builtin_bswap16(b)) ? 1 : -1;
      s1 = (char *)s1 + 2;
      s2 = (char *)s2 + 2;
      n -= 2;
    }

    if (((uintptr_t)s1) & 4) {
      const uint32_t a = *(uint32_t *)s1;
      const uint32_t b = *(uint32_t *)s2;
      if (likely(a != b))
        return (__builtin_bswap32(a) > __builtin_bswap32(b)) ? 1 : -1;
      s1 = (char *)s1 + 4;
      s2 = (char *)s2 + 4;
      n -= 4;
    }
  }

  while (n >= 8) {
    const uint64_t a = *(uint64_t *)s1;
    const uint64_t b = *(uint64_t *)s2;
    if (likely(a != b))
      return (__builtin_bswap64(a) > __builtin_bswap64(b)) ? 1 : -1;
    s1 = (char *)s1 + 8;
    s2 = (char *)s2 + 8;
    n -= 8;
  }

  if (n & 4) {
    const uint32_t a = *(uint32_t *)s1;
    const uint32_t b = *(uint32_t *)s2;
    if (likely(a != b))
      return (__builtin_bswap32(a) > __builtin_bswap32(b)) ? 1 : -1;
    s1 = (char *)s1 + 4;
    s2 = (char *)s2 + 4;
  }

  if (n & 2) {
    const uint16_t a = *(uint16_t *)s1;
    const uint16_t b = *(uint16_t *)s2;
    if (likely(a != b))
      return (__builtin_bswap16(a) > __builtin_bswap16(b)) ? 1 : -1;
    s1 = (char *)s1 + 2;
    s2 = (char *)s2 + 2;
  }

  return (n & 1) ? *(uint8_t *)s1 - *(uint8_t *)s2 : 0;
}

int __hot mdbx_e2k_strcmp_bug_workaround(const char *s1, const char *s2) {
  while (true) {
    int diff = *(uint8_t *)s1 - *(uint8_t *)s2;
    if (likely(diff != 0) || *s1 == '\0')
      return diff;
    s1 += 1;
    s2 += 1;
  }
}

int __hot mdbx_e2k_strncmp_bug_workaround(const char *s1, const char *s2, size_t n) {
  while (n > 0) {
    int diff = *(uint8_t *)s1 - *(uint8_t *)s2;
    if (likely(diff != 0) || *s1 == '\0')
      return diff;
    s1 += 1;
    s2 += 1;
    n -= 1;
  }
  return 0;
}

size_t __hot mdbx_e2k_strlen_bug_workaround(const char *s) {
  size_t n = 0;
  while (*s) {
    s += 1;
    n += 1;
  }
  return n;
}

size_t __hot mdbx_e2k_strnlen_bug_workaround(const char *s, size_t maxlen) {
  size_t n = 0;
  while (maxlen > n && *s) {
    s += 1;
    n += 1;
  }
  return n;
}
#endif /* Elbrus's memcmp() bug. */
