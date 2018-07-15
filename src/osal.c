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

#include "./bits.h"
#include "./debug.h"
#include "./proto.h"
#include "./ualb.h"

static MDBX_pid_t mdbx_getpid(void) {
#if defined(_WIN32) || defined(_WIN64)
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

static MDBX_tid_t mdbx_thread_self(void) {
#if defined(_WIN32) || defined(_WIN64)
  return GetCurrentThreadId();
#else
  return pthread_self();
#endif
}

#if defined(_WIN32) || defined(_WIN64)
static int waitstatus2errcode(DWORD result) {
  switch (result) {
  case WAIT_OBJECT_0:
    return MDBX_SUCCESS;
  case WAIT_FAILED:
    return GetLastError();
  case WAIT_ABANDONED:
    return ERROR_ABANDONED_WAIT_0;
  case WAIT_IO_COMPLETION:
    return ERROR_USER_APC;
  case WAIT_TIMEOUT:
    return ERROR_TIMEOUT;
  default:
    return ERROR_UNHANDLED_ERROR;
  }
}

/* Map a result from an NTAPI call to WIN32 error code. */
MDBX_INTERNAL int ntstatus2errcode(NTSTATUS status) {
  DWORD dummy;
  OVERLAPPED ov;
  memset(&ov, 0, sizeof(ov));
  ov.Internal = status;
  return GetOverlappedResult(NULL, &ov, &dummy, FALSE) ? MDBX_SUCCESS : GetLastError();
}

/* We use native NT APIs to setup the memory map, so that we can
 * let the databook file grow incrementally instead of always preallocating
 * the full size. These APIs are defined in <wdm.h> and <ntifs.h>
 * but those headers are meant for driver-level development and
 * conflict with the regular user-level headers, so we explicitly
 * declare them here. Using these APIs also means we must link to
 * ntdll.dll, which is not linked by default in user code. */
#pragma comment(lib, "ntdll.lib")

extern NTSTATUS NTAPI NtCreateSection(OUT PHANDLE SectionHandle, IN ACCESS_MASK DesiredAccess,
                                      IN OPTIONAL POBJECT_ATTRIBUTES ObjectAttributes,
                                      IN OPTIONAL PLARGE_INTEGER MaximumSize, IN ULONG SectionPageProtection,
                                      IN ULONG AllocationAttributes, IN OPTIONAL HANDLE FileHandle);

typedef struct _SECTION_BASIC_INFORMATION {
  ULONG Unknown;
  ULONG SectionAttributes;
  LARGE_INTEGER SectionSize;
} SECTION_BASIC_INFORMATION, *PSECTION_BASIC_INFORMATION;

typedef enum _SECTION_INFORMATION_CLASS {
  SectionBasicInformation,
  SectionImageInformation,
  SectionRelocationInformation, // name:wow64:whNtQuerySection_SectionRelocationInformation
  MaxSectionInfoClass
} SECTION_INFORMATION_CLASS;

extern NTSTATUS NTAPI NtQuerySection(IN HANDLE SectionHandle, IN SECTION_INFORMATION_CLASS InformationClass,
                                     OUT PVOID InformationBuffer, IN ULONG InformationBufferSize,
                                     OUT PULONG ResultLength OPTIONAL);

extern NTSTATUS NTAPI NtExtendSection(IN HANDLE SectionHandle, IN PLARGE_INTEGER NewSectionSize);

typedef enum _SECTION_INHERIT { ViewShare = 1, ViewUnmap = 2 } SECTION_INHERIT;

extern NTSTATUS NTAPI NtMapViewOfSection(IN HANDLE SectionHandle, IN HANDLE ProcessHandle,
                                         IN OUT PVOID *BaseAddress, IN ULONG_PTR ZeroBits,
                                         IN SIZE_T CommitSize, IN OUT OPTIONAL PLARGE_INTEGER SectionOffset,
                                         IN OUT PSIZE_T ViewSize, IN SECTION_INHERIT InheritDisposition,
                                         IN ULONG AllocationType, IN ULONG Win32Protect);

extern NTSTATUS NTAPI NtUnmapViewOfSection(IN HANDLE ProcessHandle, IN OPTIONAL PVOID BaseAddress);

extern NTSTATUS NTAPI NtClose(HANDLE Handle);

extern NTSTATUS NTAPI NtAllocateVirtualMemory(IN HANDLE ProcessHandle, IN OUT PVOID *BaseAddress,
                                              IN ULONG_PTR ZeroBits, IN OUT PSIZE_T RegionSize,
                                              IN ULONG AllocationType, IN ULONG Protect);

extern NTSTATUS NTAPI NtFreeVirtualMemory(IN HANDLE ProcessHandle, IN PVOID *BaseAddress,
                                          IN OUT PSIZE_T RegionSize, IN ULONG FreeType);

#ifndef FILE_PROVIDER_CURRENT_VERSION
typedef struct _FILE_PROVIDER_EXTERNAL_INFO_V1 {
  ULONG Version;
  ULONG Algorithm;
  ULONG Flags;
} FILE_PROVIDER_EXTERNAL_INFO_V1, *PFILE_PROVIDER_EXTERNAL_INFO_V1;
#endif

#ifndef STATUS_OBJECT_NOT_EXTERNALLY_BACKED
#define STATUS_OBJECT_NOT_EXTERNALLY_BACKED ((NTSTATUS)0xC000046DL)
#endif
#ifndef STATUS_INVALID_DEVICE_REQUEST
#define STATUS_INVALID_DEVICE_REQUEST ((NTSTATUS)0xC0000010L)
#endif

extern NTSTATUS NTAPI NtFsControlFile(IN HANDLE FileHandle, IN OUT HANDLE Event,
                                      IN OUT PVOID /* PIO_APC_ROUTINE */ ApcRoutine, IN OUT PVOID ApcContext,
                                      OUT PIO_STATUS_BLOCK IoStatusBlock, IN ULONG FsControlCode,
                                      IN OUT PVOID InputBuffer, IN ULONG InputBufferLength,
                                      OUT OPTIONAL PVOID OutputBuffer, IN ULONG OutputBufferLength);

typedef struct _SYSTEM_BOOT_ENVIRONMENT_INFORMATION {
  GUID BootIdentifier;
  FIRMWARE_TYPE FirmwareType;
  ULONGLONG BootFlags;
} SYSTEM_BOOT_ENVIRONMENT_INFORMATION;

#endif /* _WIN32 || _WIN64 */

/*----------------------------------------------------------------------------*/

#ifndef mdbx_vasprintf
int mdbx_vasprintf(char **strp, const char *fmt, va_list ap) {
  va_list ones;
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#elif defined(vsnprintf) || defined(_BSD_SOURCE) || _XOPEN_SOURCE >= 500 || defined(_ISOC99_SOURCE) ||        \
    _POSIX_C_SOURCE >= 200112L
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#else
#error FIXME
#endif

  if (unlikely(needed < 0 || needed >= INT_MAX)) {
    *strp = nullptr;
    va_end(ones);
    return needed;
  }

  *strp = malloc(needed + 1);
  if (unlikely(*strp == nullptr)) {
    va_end(ones);
    SetLastError(MDBX_ENOMEM);
    return -1;
  }

#if defined(vsnprintf) || defined(_BSD_SOURCE) || _XOPEN_SOURCE >= 500 || defined(_ISOC99_SOURCE) ||          \
    _POSIX_C_SOURCE >= 200112L
  int actual = vsnprintf(*strp, needed + 1, fmt, ones);
#else
#error FIXME
#endif
  va_end(ones);

  assert(actual == needed);
  if (unlikely(actual < 0)) {
    free(*strp);
    *strp = nullptr;
  }
  return actual;
}
#endif /* mdbx_vasprintf */

#ifndef mdbx_asprintf
int mdbx_asprintf(char **strp, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  int rc = mdbx_vasprintf(strp, fmt, ap);
  va_end(ap);
  return rc;
}
#endif /* mdbx_asprintf */

/*----------------------------------------------------------------------------*/

static int mdbx_condmutex_init(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  int rc = MDBX_SUCCESS;
  condmutex->event = NULL;
  condmutex->mutex = CreateMutex(NULL, FALSE, NULL);
  if (!condmutex->mutex)
    return GetLastError();

  condmutex->event = CreateEvent(NULL, FALSE, FALSE, NULL);
  if (!condmutex->event) {
    rc = GetLastError();
    (void)CloseHandle(condmutex->mutex);
    condmutex->mutex = NULL;
  }
  return rc;
#else
  memset(condmutex, 0, sizeof(mdbx_condmutex_t));
  int rc = pthread_mutex_init(&condmutex->mutex, NULL);
  if (rc == 0) {
    rc = pthread_cond_init(&condmutex->cond, NULL);
    if (rc != 0)
      (void)pthread_mutex_destroy(&condmutex->mutex);
  }
  return rc;
#endif
}

static bool is_allzeros(const void *ptr, size_t bytes) {
  const uint8_t *u8 = ptr;
  for (size_t i = 0; i < bytes; ++i)
    if (u8[i] != 0)
      return false;
  return true;
}

static int mdbx_condmutex_destroy(mdbx_condmutex_t *condmutex) {
  int rc = MDBX_EINVAL;
#if defined(_WIN32) || defined(_WIN64)
  if (condmutex->event) {
    rc = CloseHandle(condmutex->event) ? MDBX_SUCCESS : GetLastError();
    if (rc == MDBX_SUCCESS)
      condmutex->event = NULL;
  }
  if (condmutex->mutex) {
    rc = CloseHandle(condmutex->mutex) ? MDBX_SUCCESS : GetLastError();
    if (rc == MDBX_SUCCESS)
      condmutex->mutex = NULL;
  }
#else
  if (!is_allzeros(&condmutex->cond, sizeof(condmutex->cond))) {
    rc = pthread_cond_destroy(&condmutex->cond);
    if (rc == 0)
      memset(&condmutex->cond, 0, sizeof(condmutex->cond));
  }
  if (!is_allzeros(&condmutex->mutex, sizeof(condmutex->mutex))) {
    rc = pthread_mutex_destroy(&condmutex->mutex);
    if (rc == 0)
      memset(&condmutex->mutex, 0, sizeof(condmutex->mutex));
  }
#endif
  return rc;
}

static int mdbx_condmutex_lock(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD code = WaitForSingleObject(condmutex->mutex, INFINITE);
  return waitstatus2errcode(code);
#else
  return pthread_mutex_lock(&condmutex->mutex);
#endif
}

static int mdbx_condmutex_unlock(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  return ReleaseMutex(condmutex->mutex) ? MDBX_SUCCESS : GetLastError();
#else
  return pthread_mutex_unlock(&condmutex->mutex);
#endif
}

static int mdbx_condmutex_signal(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  return SetEvent(condmutex->event) ? MDBX_SUCCESS : GetLastError();
#else
  return pthread_cond_signal(&condmutex->cond);
#endif
}

static int mdbx_condmutex_wait(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD code = SignalObjectAndWait(condmutex->mutex, condmutex->event, INFINITE, FALSE);
  if (code == WAIT_OBJECT_0)
    code = WaitForSingleObject(condmutex->mutex, INFINITE);
  return waitstatus2errcode(code);
#else
  return pthread_cond_wait(&condmutex->cond, &condmutex->mutex);
#endif
}

/*----------------------------------------------------------------------------*/

static int mdbx_fastmutex_init(mdbx_fastmutex_t *fastmutex) {
#if defined(_WIN32) || defined(_WIN64)
  InitializeCriticalSection(fastmutex);
  return MDBX_SUCCESS;
#else
  return pthread_mutex_init(fastmutex, NULL);
#endif
}

static int mdbx_fastmutex_destroy(mdbx_fastmutex_t *fastmutex) {
#if defined(_WIN32) || defined(_WIN64)
  DeleteCriticalSection(fastmutex);
  return MDBX_SUCCESS;
#else
  return pthread_mutex_destroy(fastmutex);
#endif
}

static int mdbx_fastmutex_acquire(mdbx_fastmutex_t *fastmutex, unsigned flags) {
#if defined(_WIN32) || defined(_WIN64)
  if (flags & MDBX_NONBLOCK)
    return TryEnterCriticalSection(fastmutex) ? MDBX_SUCCESS : MDBX_EBUSY;
  EnterCriticalSection(fastmutex);
  return MDBX_SUCCESS;
#else
  if (flags & MDBX_NONBLOCK)
    return pthread_mutex_trylock(fastmutex);
  return pthread_mutex_lock(fastmutex);
#endif
}

static int mdbx_fastmutex_release(mdbx_fastmutex_t *fastmutex) {
#if defined(_WIN32) || defined(_WIN64)
  LeaveCriticalSection(fastmutex);
  return MDBX_SUCCESS;
#else
  return pthread_mutex_unlock(fastmutex);
#endif
}

/*----------------------------------------------------------------------------*/

static int mdbx_openfile(const char *pathname, int flags, mode_t mode, MDBX_filehandle_t *fd) {
  *fd = MDBX_INVALID_FD;
#if defined(_WIN32) || defined(_WIN64)
  (void)mode;

  DWORD DesiredAccess;
  DWORD ShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  DWORD FlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
  switch (flags & (O_RDONLY | O_WRONLY | O_RDWR)) {
  default:
    return ERROR_INVALID_PARAMETER;
  case O_RDONLY:
    DesiredAccess = GENERIC_READ;
    break;
  case O_WRONLY: /* assume for MDBX_env_copy() and friends output */
    DesiredAccess = GENERIC_WRITE;
    ShareMode = 0;
    FlagsAndAttributes |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
    break;
  case O_RDWR:
    DesiredAccess = GENERIC_READ | GENERIC_WRITE;
    break;
  }

  DWORD CreationDisposition;
  switch (flags & (O_EXCL | O_CREAT)) {
  default:
    return ERROR_INVALID_PARAMETER;
  case 0:
    CreationDisposition = OPEN_EXISTING;
    break;
  case O_EXCL | O_CREAT:
    CreationDisposition = CREATE_NEW;
    FlagsAndAttributes |= FILE_ATTRIBUTE_NOT_CONTENT_INDEXED;
    break;
  case O_CREAT:
    CreationDisposition = OPEN_ALWAYS;
    FlagsAndAttributes |= FILE_ATTRIBUTE_NOT_CONTENT_INDEXED;
    break;
  }

  *fd = CreateFileA(pathname, DesiredAccess, ShareMode, NULL, CreationDisposition, FlagsAndAttributes, NULL);

  if (*fd == MDBX_INVALID_FD)
    return GetLastError();
  if ((flags & O_CREAT) && GetLastError() != ERROR_ALREADY_EXISTS) {
    /* set FILE_ATTRIBUTE_NOT_CONTENT_INDEXED for new file */
    DWORD FileAttributes = GetFileAttributesA(pathname);
    if (FileAttributes == INVALID_FILE_ATTRIBUTES ||
        !SetFileAttributesA(pathname, FileAttributes | FILE_ATTRIBUTE_NOT_CONTENT_INDEXED)) {
      int rc = GetLastError();
      CloseHandle(*fd);
      *fd = MDBX_INVALID_FD;
      return rc;
    }
  }
#else

#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOATIME
  flags |= O_NOATIME;
#endif
  *fd = open(pathname, flags, mode);
  if (*fd < 0)
    return errno;
#if defined(FD_CLOEXEC) && defined(F_GETFD)
  flags = fcntl(*fd, F_GETFD);
  if (flags >= 0)
    (void)fcntl(*fd, F_SETFD, flags | FD_CLOEXEC);
#endif
#endif
  return MDBX_SUCCESS;
}

static int mdbx_closefile(MDBX_filehandle_t fd) {
#if defined(_WIN32) || defined(_WIN64)
  return CloseHandle(fd) ? MDBX_SUCCESS : GetLastError();
#else
  return (close(fd) == 0) ? MDBX_SUCCESS : errno;
#endif
}

static int mdbx_pread(MDBX_filehandle_t fd, void *buf, size_t bytes, uint64_t offset) {
  if (bytes > MAX_WRITE)
    return MDBX_EINVAL;
#if defined(_WIN32) || defined(_WIN64)

  OVERLAPPED ov;
  ov.hEvent = 0;
  ov.Offset = (DWORD)offset;
  ov.OffsetHigh = HIGH_DWORD(offset);

  DWORD read = 0;
  if (unlikely(!ReadFile(fd, buf, (DWORD)bytes, &read, &ov))) {
    int rc = GetLastError();
    return (rc == MDBX_SUCCESS) ? /* paranoia */ ERROR_READ_FAULT : rc;
  }
#else
  STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t), "libmdbx requires 64-bit file I/O on 64-bit systems");
  intptr_t read = pread(fd, buf, bytes, offset);
  if (read < 0) {
    int rc = errno;
    return (rc == MDBX_SUCCESS) ? /* paranoia */ MDBX_EIO : rc;
  }
#endif
  return (bytes == (size_t)read) ? MDBX_SUCCESS : MDBX_ENODATA;
}

static int mdbx_pwrite(MDBX_filehandle_t fd, const void *buf, size_t bytes, uint64_t offset) {
#if defined(_WIN32) || defined(_WIN64)
  if (bytes > MAX_WRITE)
    return ERROR_INVALID_PARAMETER;

  OVERLAPPED ov;
  ov.hEvent = 0;
  ov.Offset = (DWORD)offset;
  ov.OffsetHigh = HIGH_DWORD(offset);

  DWORD written;
  if (likely(WriteFile(fd, buf, (DWORD)bytes, &written, &ov)))
    return (bytes == written) ? MDBX_SUCCESS : MDBX_EIO /* ERROR_WRITE_FAULT */;
  return GetLastError();
#else
  int rc;
  intptr_t written;
  do {
    STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t), "libmdbx requires 64-bit file I/O on 64-bit systems");
    written = pwrite(fd, buf, bytes, offset);
    if (likely(bytes == (size_t)written))
      return MDBX_SUCCESS;
    rc = errno;
  } while (rc == EINTR);
  return (written < 0) ? rc : MDBX_EIO /* Use which error code (ENOSPC)? */;
#endif
}

static int mdbx_pwritev(MDBX_filehandle_t fd, struct iovec *iov, int iovcnt, uint64_t offset,
                        size_t expected_written) {
#if defined(_WIN32) || defined(_WIN64)
  size_t written = 0;
  for (int i = 0; i < iovcnt; ++i) {
    int rc = mdbx_pwrite(fd, iov[i].iov_base, iov[i].iov_len, offset);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    written += iov[i].iov_len;
    offset += iov[i].iov_len;
  }
  return (expected_written == written) ? MDBX_SUCCESS : MDBX_EIO /* ERROR_WRITE_FAULT */;
#else
  int rc;
  intptr_t written;
  do {
    STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t), "libmdbx requires 64-bit file I/O on 64-bit systems");
    written = pwritev(fd, iov, iovcnt, offset);
    if (likely(expected_written == (size_t)written))
      return MDBX_SUCCESS;
    rc = errno;
  } while (rc == EINTR);
  return (written < 0) ? rc : MDBX_EIO /* Use which error code? */;
#endif
}

static int mdbx_write(MDBX_filehandle_t fd, const void *buf, size_t bytes) {
#ifdef SIGPIPE
  sigset_t set, old;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  int rc = pthread_sigmask(SIG_BLOCK, &set, &old);
  if (rc != 0)
    return rc;
#endif

  const char *ptr = buf;
  for (;;) {
    size_t chunk = (MAX_WRITE < bytes) ? MAX_WRITE : bytes;
#if defined(_WIN32) || defined(_WIN64)
    DWORD written;
    if (unlikely(!WriteFile(fd, ptr, (DWORD)chunk, &written, NULL)))
      return GetLastError();
#else
    intptr_t written = write(fd, ptr, chunk);
    if (written < 0) {
      int rc = errno;
#ifdef SIGPIPE
      if (rc == EPIPE) {
        /* Collect the pending SIGPIPE, otherwise at least OS X
         * gives it to the process on thread-exit (ITS#8504). */
        int tmp;
        sigwait(&set, &tmp);
        written = 0;
        continue;
      }
      pthread_sigmask(SIG_SETMASK, &old, NULL);
#endif
      return rc;
    }
#endif
    if (likely(bytes == (size_t)written)) {
#ifdef SIGPIPE
      pthread_sigmask(SIG_SETMASK, &old, NULL);
#endif
      return MDBX_SUCCESS;
    }
    ptr += written;
    bytes -= written;
  }
}

static int mdbx_filesync(MDBX_filehandle_t fd, bool filesize_changed) {
#if defined(_WIN32) || defined(_WIN64)
  (void)filesize_changed;
  return FlushFileBuffers(fd) ? MDBX_SUCCESS : GetLastError();
#elif __GLIBC_PREREQ(2, 16) || _BSD_SOURCE || _XOPEN_SOURCE ||                                                \
    (__GLIBC_PREREQ(2, 8) && _POSIX_C_SOURCE >= 200112L)
  for (;;) {
/* LY: It is no reason to use fdatasync() here, even in case
 * no such bug in a kernel. Because "no-bug" mean that a kernel
 * internally do nearly the same, e.g. fdatasync() == fsync()
 * when no-kernel-bug and file size was changed.
 *
 * So, this code is always safe and without appreciable
 * performance degradation.
 *
 * For more info about of a corresponding fdatasync() bug
 * see http://www.spinics.net/lists/linux-ext4/msg33714.html */
#if _POSIX_C_SOURCE >= 199309L || _XOPEN_SOURCE >= 500 || defined(_POSIX_SYNCHRONIZED_IO)
    if (!filesize_changed && fdatasync(fd) == 0)
      return MDBX_SUCCESS;
#else
    (void)filesize_changed;
#endif
    if (fsync(fd) == 0)
      return MDBX_SUCCESS;
    int rc = errno;
    if (rc != EINTR)
      return rc;
  }
#else
#error FIXME
#endif
}

static int mdbx_filesize_sync(MDBX_filehandle_t fd) {
#if defined(_WIN32) || defined(_WIN64)
  (void)fd;
  /* Nothing on Windows (i.e. newer 100% steady) */
  return MDBX_SUCCESS;
#else
  for (;;) {
    if (fsync(fd) == 0)
      return MDBX_SUCCESS;
    int rc = errno;
    if (rc != EINTR)
      return rc;
  }
#endif
}

static int mdbx_filesize(MDBX_filehandle_t fd, uint64_t *length) {
#if defined(_WIN32) || defined(_WIN64)
  BY_HANDLE_FILE_INFORMATION info;
  if (!GetFileInformationByHandle(fd, &info))
    return GetLastError();
  *length = info.nFileSizeLow | (uint64_t)info.nFileSizeHigh << 32;
#else
  struct stat st;

  STATIC_ASSERT_MSG(sizeof(off_t) <= sizeof(uint64_t), "libmdbx requires 64-bit file I/O on 64-bit systems");
  if (fstat(fd, &st))
    return errno;

  *length = st.st_size;
#endif
  return MDBX_SUCCESS;
}

static int mdbx_ftruncate(MDBX_filehandle_t fd, uint64_t length) {
#if defined(_WIN32) || defined(_WIN64)
  LARGE_INTEGER li;
  li.QuadPart = length;
  return (SetFilePointerEx(fd, li, NULL, FILE_BEGIN) && SetEndOfFile(fd)) ? MDBX_SUCCESS : GetLastError();
#else
  STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t), "libmdbx requires 64-bit file I/O on 64-bit systems");
  return ftruncate(fd, length) == 0 ? MDBX_SUCCESS : errno;
#endif
}

MDBX_error_t mdbx_is_directory(const char *pathname) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD attr = GetFileAttributesA(pathname);
  if (attr != INVALID_FILE_ATTRIBUTES)
    return (attr & FILE_ATTRIBUTE_DIRECTORY) ? MDBX_SUCCESS : MDBX_SIGN;
#else
  struct stat info;
  if (stat(pathname, &info) == 0)
    return S_ISDIR(info.st_mode) ? MDBX_SUCCESS : MDBX_SIGN;
#endif
  return mdbx_get_errno();
}

MDBX_INTERNAL void osal_ctor(void) {

// osal_syspagesize
/* Get the size of a memory page for the system.
 * This is the basic size that the platform's memory manager uses, and is
 * fundamental to the use of memory-mapped files. */
#if defined(_WIN32) || defined(_WIN64)
  SYSTEM_INFO si;
  GetSystemInfo(&si);
  osal_syspagesize = si.dwPageSize;
#else
  osal_syspagesize = (size_t)sysconf(_SC_PAGE_SIZE);
#endif

#if defined(_WIN32) || defined(_WIN64)
  DWORD SharedDataAligment, len;
  NTSTATUS status = NtQuerySystemInformation(0x3A /* SystemRecommendedSharedDataAlignment */,
                                             &SharedDataAligment, sizeof(SharedDataAligment), &len);
  if (NT_SUCCESS(status) && len == sizeof(SharedDataAligment))
    osal_cacheline_size = SharedDataAligment;
#else
  long cls_value = sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
  if (cls_value > 0)
    osal_cacheline_size = cls_value;
  cls_value = sysconf(_SC_LEVEL2_CACHE_LINESIZE);
  if (cls_value > 0 && osal_cacheline_size < (unsigned)cls_value)
    osal_cacheline_size = cls_value;
  cls_value = sysconf(_SC_LEVEL3_CACHE_LINESIZE);
  if (cls_value > 0 && osal_cacheline_size < (unsigned)cls_value)
    osal_cacheline_size = cls_value;
  cls_value = sysconf(_SC_LEVEL4_CACHE_LINESIZE);
  if (cls_value > 0 && osal_cacheline_size < (unsigned)cls_value)
    osal_cacheline_size = cls_value;
#endif

// osal_bootid_value
#if defined(_WIN32) || defined(_WIN64)
  t1ha_context_t hash;
  t1ha2_init(&hash, 20180502, 75941);

  union Buffer {
    DWORD BootId;
    DWORD BaseTime;
    SYSTEM_BOOT_ENVIRONMENT_INFORMATION SysBootEnvInfo;
    SYSTEM_TIMEOFDAY_INFORMATION SysTimeOfDayInfo;
    struct {
      LARGE_INTEGER BootTime;
      LARGE_INTEGER CurrentTime;
      LARGE_INTEGER TimeZoneBias;
      ULONG TimeZoneId;
      ULONG Reserved;
      ULONGLONG BootTimeBias;
      ULONGLONG SleepTimeBias;
    } SysTimeOfDayInfoHacked;
  } buffer;
  bool got_bootid = false, got_timesalt = false;

  static const wchar_t HKLM_PrefetcherParams[] =
      L"SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Memory Management\\PrefetchParameters";
  len = sizeof(buffer);
  if (RegGetValueW(HKEY_LOCAL_MACHINE, HKLM_PrefetcherParams, L"BootId", RRF_RT_DWORD, NULL, &buffer.BootId,
                   &len) == ERROR_SUCCESS &&
      len > 0) {
    t1ha2_update(&hash, &buffer.BootId, len);
    got_bootid = true;
  }

  len = sizeof(buffer);
  if (RegGetValueW(HKEY_LOCAL_MACHINE, HKLM_PrefetcherParams, L"BaseTime", RRF_RT_DWORD, NULL,
                   &buffer.BaseTime, &len) == ERROR_SUCCESS &&
      len > 0) {
    t1ha2_update(&hash, &buffer.BaseTime, len);
    got_timesalt = true;
  }

  // BootIdentifier from SYSTEM_BOOT_ENVIRONMENT_INFORMATION
  status = NtQuerySystemInformation(0x5A /* SystemBootEnvironmentInformation */, &buffer.SysBootEnvInfo,
                                    sizeof(buffer.SysBootEnvInfo), &len);
  if (NT_SUCCESS(status) && len >= offsetof(union Buffer, SysBootEnvInfo.BootIdentifier) +
                                       sizeof(buffer.SysBootEnvInfo.BootIdentifier)) {
    t1ha2_update(&hash, &buffer.SysBootEnvInfo.BootIdentifier, sizeof(buffer.SysBootEnvInfo.BootIdentifier));
    got_bootid = true;
  }

  // BootTime from SYSTEM_TIMEOFDAY_INFORMATION
  status = NtQuerySystemInformation(0x03 /* SystemTmeOfDayInformation */, &buffer.SysTimeOfDayInfo,
                                    sizeof(buffer.SysTimeOfDayInfo), &len);
  if (NT_SUCCESS(status) && len >= offsetof(union Buffer, SysTimeOfDayInfoHacked.BootTime) +
                                       sizeof(buffer.SysTimeOfDayInfoHacked.BootTime)) {
    t1ha2_update(&hash, &buffer.SysTimeOfDayInfoHacked.BootTime,
                 sizeof(buffer.SysTimeOfDayInfoHacked.BootTime));
    got_timesalt = true;
  }

  if (hash.total > 7 && got_bootid && got_timesalt)
    osal_bootid_value.qwords[0] = t1ha2_final(&hash, &osal_bootid_value.qwords[1]);

#elif defined(__linux__)

  // use low-level API here for simplicity
  int fd = open("/proc/sys/kernel/random/boot_id", O_RDONLY);
  if (fd != -1) {
    char uuid_string[64];
    ssize_t len = read(fd, uuid_string, sizeof(uuid_string));
    close(fd);
    if (len > 7)
      osal_bootid_value.qwords[0] =
          t1ha2_atonce128(&osal_bootid_value.qwords[1], uuid_string, len, 201805020557);
  }

#elif defined(__APPLE__) || defined(__MACH__)

  // "kern.bootsessionuuid" is only available by name
  char uuid_string[64];
  size_t len = sizeof(uuid_string);
  if (sysctlbyname("kern.bootsessionuuid", uuid_string, &len, nullptr, 0) == 0 && len > 7)
    osal_bootid_value.qwords[0] =
        t1ha2_atonce128(&osal_bootid_value.qwords[1], uuid_string, len, 201805020557);

#else
#warning FIXME
#endif
}

/*----------------------------------------------------------------------------*/

static int mdbx_thread_create(mdbx_thread_t *thread, THREAD_RESULT(THREAD_CALL *start_routine)(void *),
                              void *arg) {
#if defined(_WIN32) || defined(_WIN64)
  *thread = CreateThread(NULL, 0, start_routine, arg, 0, NULL);
  return *thread ? MDBX_SUCCESS : GetLastError();
#else
  return pthread_create(thread, NULL, start_routine, arg);
#endif
}

static int mdbx_thread_join(mdbx_thread_t thread) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD code = WaitForSingleObject(thread, INFINITE);
  return waitstatus2errcode(code);
#else
  void *unused_retval = &unused_retval;
  return pthread_join(thread, &unused_retval);
#endif
}

/*----------------------------------------------------------------------------*/

static int mdbx_msync(mdbx_mmap_t *map, size_t offset, size_t length, int async) {
  uint8_t *ptr = (uint8_t *)map->address + offset;
#if defined(_WIN32) || defined(_WIN64)
  if (FlushViewOfFile(ptr, length) && (async || FlushFileBuffers(map->fd)))
    return MDBX_SUCCESS;
  return GetLastError();
#else
  const int mode = async ? MS_ASYNC : MS_SYNC;
  return (msync(ptr, length, mode) == 0) ? MDBX_SUCCESS : errno;
#endif
}

int mdbx_mmap(int flags, mdbx_mmap_t *map, size_t size, size_t limit) {
  assert(size <= limit);
#if defined(_WIN32) || defined(_WIN64)
  NTSTATUS rc;
  map->length = 0;
  map->current = 0;
  map->section = NULL;
  map->address = nullptr;

  if (GetFileType(map->fd) != FILE_TYPE_DISK)
    return ERROR_FILE_OFFLINE;

  FILE_REMOTE_PROTOCOL_INFO RemoteProtocolInfo;
  if (GetFileInformationByHandleEx(map->fd, FileRemoteProtocolInfo, &RemoteProtocolInfo,
                                   sizeof(RemoteProtocolInfo))) {
    if ((RemoteProtocolInfo.Flags & (REMOTE_PROTOCOL_INFO_FLAG_LOOPBACK |
                                     REMOTE_PROTOCOL_INFO_FLAG_OFFLINE)) != REMOTE_PROTOCOL_INFO_FLAG_LOOPBACK)
      return ERROR_FILE_OFFLINE;
  }

#if defined(_WIN64) && defined(WOF_CURRENT_VERSION)
  struct {
    WOF_EXTERNAL_INFO wof_info;
    union {
      WIM_PROVIDER_EXTERNAL_INFO wim_info;
      FILE_PROVIDER_EXTERNAL_INFO_V1 file_info;
    };
    size_t reserved_for_microsoft_madness[42];
  } GetExternalBacking_OutputBuffer;
  IO_STATUS_BLOCK StatusBlock;
  rc = NtFsControlFile(map->fd, NULL, NULL, NULL, &StatusBlock, FSCTL_GET_EXTERNAL_BACKING, NULL, 0,
                       &GetExternalBacking_OutputBuffer, sizeof(GetExternalBacking_OutputBuffer));
  if (rc != STATUS_OBJECT_NOT_EXTERNALLY_BACKED && rc != STATUS_INVALID_DEVICE_REQUEST)
    return NT_SUCCESS(rc) ? ERROR_FILE_OFFLINE : ntstatus2errcode(rc);
#endif

  WCHAR PathBuffer[INT16_MAX];
  DWORD VolumeSerialNumber, FileSystemFlags;
  if (!GetVolumeInformationByHandleW(map->fd, PathBuffer, INT16_MAX, &VolumeSerialNumber, NULL,
                                     &FileSystemFlags, NULL, 0))
    return GetLastError();

  if ((flags & MDBX_RDONLY) == 0) {
    if (FileSystemFlags & (FILE_SEQUENTIAL_WRITE_ONCE | FILE_READ_ONLY_VOLUME | FILE_VOLUME_IS_COMPRESSED))
      return ERROR_FILE_OFFLINE;
  }

  if (!GetFinalPathNameByHandleW(map->fd, PathBuffer, INT16_MAX, FILE_NAME_NORMALIZED | VOLUME_NAME_NT))
    return GetLastError();

  if (_wcsnicmp(PathBuffer, L"\\Device\\Mup\\", 12) == 0)
    return ERROR_FILE_OFFLINE;

  if (GetFinalPathNameByHandleW(map->fd, PathBuffer, INT16_MAX, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS)) {
    UINT DriveType = GetDriveTypeW(PathBuffer);
    if (DriveType == DRIVE_NO_ROOT_DIR && wcsncmp(PathBuffer, L"\\\\?\\", 4) == 0 &&
        wcsncmp(PathBuffer + 5, L":\\", 2) == 0) {
      PathBuffer[7] = 0;
      DriveType = GetDriveTypeW(PathBuffer + 4);
    }
    switch (DriveType) {
    case DRIVE_CDROM:
      if (flags & MDBX_RDONLY)
        break;
    // fall through
    case DRIVE_UNKNOWN:
    case DRIVE_NO_ROOT_DIR:
    case DRIVE_REMOTE:
    default:
      return ERROR_FILE_OFFLINE;
    case DRIVE_REMOVABLE:
    case DRIVE_FIXED:
    case DRIVE_RAMDISK:
      break;
    }
  }

  rc = mdbx_filesize(map->fd, &map->filesize);
  if (rc != MDBX_SUCCESS)
    return rc;
  if ((flags & MDBX_RDONLY) == 0 && map->filesize != size) {
    rc = mdbx_ftruncate(map->fd, size);
    if (rc == MDBX_SUCCESS)
      map->filesize = size;
    /* ignore error, because Windows unable shrink file
     * that already mapped (by another process) */
  }

  LARGE_INTEGER SectionSize;
  SectionSize.QuadPart = size;
  rc = NtCreateSection(&map->section,
                       /* DesiredAccess */
                       (flags & MDBX_WRITEMAP)
                           ? SECTION_QUERY | SECTION_MAP_READ | SECTION_EXTEND_SIZE | SECTION_MAP_WRITE
                           : SECTION_QUERY | SECTION_MAP_READ | SECTION_EXTEND_SIZE,
                       /* ObjectAttributes */ NULL, /* MaximumSize (InitialSize) */ &SectionSize,
                       /* SectionPageProtection */ (flags & MDBX_RDONLY) ? PAGE_READONLY : PAGE_READWRITE,
                       /* AllocationAttributes */ SEC_RESERVE, map->fd);
  if (!NT_SUCCESS(rc))
    return ntstatus2errcode(rc);

  SIZE_T ViewSize = (flags & MDBX_RDONLY) ? 0 : limit;
  rc = NtMapViewOfSection(map->section, GetCurrentProcess(), &map->address,
                          /* ZeroBits */ 0,
                          /* CommitSize */ 0,
                          /* SectionOffset */ NULL, &ViewSize,
                          /* InheritDisposition */ ViewUnmap,
                          /* AllocationType */ (flags & MDBX_RDONLY) ? 0 : MEM_RESERVE,
                          /* Win32Protect */ (flags & MDBX_WRITEMAP) ? PAGE_READWRITE : PAGE_READONLY);
  if (!NT_SUCCESS(rc)) {
    NtClose(map->section);
    map->section = 0;
    map->address = nullptr;
    return ntstatus2errcode(rc);
  }
  assert(map->address != MAP_FAILED);

  map->current = (size_t)SectionSize.QuadPart;
  map->length = ViewSize;
  return MDBX_SUCCESS;
#else
  (void)size;
  map->address =
      mmap(NULL, limit, (flags & MDBX_WRITEMAP) ? PROT_READ | PROT_WRITE : PROT_READ, MAP_SHARED, map->fd, 0);
  if (likely(map->address != MAP_FAILED)) {
    map->length = limit;
    return MDBX_SUCCESS;
  }
  map->length = 0;
  map->address = nullptr;
  return errno;
#endif
}

int mdbx_munmap(mdbx_mmap_t *map) {
#if defined(_WIN32) || defined(_WIN64)
  if (map->section)
    NtClose(map->section);
  NTSTATUS rc = NtUnmapViewOfSection(GetCurrentProcess(), map->address);
  if (!NT_SUCCESS(rc))
    ntstatus2errcode(rc);

  if (map->filesize != map->current && mdbx_filesize(map->fd, &map->filesize) == MDBX_SUCCESS &&
      map->filesize != map->current)
    (void)mdbx_ftruncate(map->fd, map->current);

  map->length = 0;
  map->current = 0;
  map->address = nullptr;
#else
  if (unlikely(munmap(map->address, map->length)))
    return errno;
  map->length = 0;
  map->address = nullptr;
#endif
  return MDBX_SUCCESS;
}

int mdbx_mresize(int flags, mdbx_mmap_t *map, size_t size, size_t limit) {
  assert(size <= limit);
#if defined(_WIN32) || defined(_WIN64)
  assert(size != map->current || limit != map->length || size < map->filesize);

  NTSTATUS status;
  LARGE_INTEGER SectionSize;
  int err, rc = MDBX_SUCCESS;

  if (!(flags & MDBX_RDONLY) && limit == map->length && size > map->current) {
    /* growth rw-section */
    SectionSize.QuadPart = size;
    status = NtExtendSection(map->section, &SectionSize);
    if (NT_SUCCESS(status))
      map->filesize = map->current = size;
    return ntstatus2errcode(status);
  }

  if (limit > map->length) {
    /* check ability of address space for growth before umnap */
    PVOID BaseAddress = (PBYTE)map->address + map->length;
    SIZE_T RegionSize = limit - map->length;
    status =
        NtAllocateVirtualMemory(GetCurrentProcess(), &BaseAddress, 0, &RegionSize, MEM_RESERVE, PAGE_NOACCESS);
    if (!NT_SUCCESS(status))
      return ntstatus2errcode(status);

    status = NtFreeVirtualMemory(GetCurrentProcess(), &BaseAddress, &RegionSize, MEM_RELEASE);
    if (!NT_SUCCESS(status))
      return ntstatus2errcode(status);
  }

  /* Windows unable:
   *  - shrink a mapped file;
   *  - change size of mapped view;
   *  - extend read-only mapping;
   * Therefore we should unmap/map entire section. */
  status = NtUnmapViewOfSection(GetCurrentProcess(), map->address);
  if (!NT_SUCCESS(status))
    return ntstatus2errcode(status);
  status = NtClose(map->section);
  map->section = NULL;
  PVOID ReservedAddress = NULL;
  SIZE_T ReservedSize = limit;

  if (!NT_SUCCESS(status)) {
  bailout_ntstatus:
    err = ntstatus2errcode(status);
  bailout:
    map->address = NULL;
    map->current = map->length = 0;
    if (ReservedAddress)
      (void)NtFreeVirtualMemory(GetCurrentProcess(), &ReservedAddress, &ReservedSize, MEM_RELEASE);
    return err;
  }

  /* resizing of the file may take a while,
   * therefore we reserve address space to avoid occupy it by other threads */
  ReservedAddress = map->address;
  status = NtAllocateVirtualMemory(GetCurrentProcess(), &ReservedAddress, 0, &ReservedSize, MEM_RESERVE,
                                   PAGE_NOACCESS);
  if (!NT_SUCCESS(status)) {
    ReservedAddress = NULL;
    if (status != /* STATUS_CONFLICTING_ADDRESSES */ 0xC0000018 || limit == map->length)
      goto bailout_ntstatus /* no way to recovery */;

    /* assume we can change base address if mapping size changed */
    map->address = NULL;
  }

retry_file_and_section:
  err = mdbx_filesize(map->fd, &map->filesize);
  if (err != MDBX_SUCCESS)
    goto bailout;

  if ((flags & MDBX_RDONLY) == 0 && map->filesize != size) {
    err = mdbx_ftruncate(map->fd, size);
    if (err == MDBX_SUCCESS)
      map->filesize = size;
    /* ignore error, because Windows unable shrink file
     * that already mapped (by another process) */
  }

  SectionSize.QuadPart = size;
  status = NtCreateSection(&map->section,
                           /* DesiredAccess */
                           (flags & MDBX_WRITEMAP)
                               ? SECTION_QUERY | SECTION_MAP_READ | SECTION_EXTEND_SIZE | SECTION_MAP_WRITE
                               : SECTION_QUERY | SECTION_MAP_READ | SECTION_EXTEND_SIZE,
                           /* ObjectAttributes */ NULL,
                           /* MaximumSize (InitialSize) */ &SectionSize,
                           /* SectionPageProtection */ (flags & MDBX_RDONLY) ? PAGE_READONLY : PAGE_READWRITE,
                           /* AllocationAttributes */ SEC_RESERVE, map->fd);

  if (!NT_SUCCESS(status))
    goto bailout_ntstatus;

  if (ReservedAddress) {
    /* release reserved address space */
    status = NtFreeVirtualMemory(GetCurrentProcess(), &ReservedAddress, &ReservedSize, MEM_RELEASE);
    ReservedAddress = NULL;
    if (!NT_SUCCESS(status))
      goto bailout_ntstatus;
  }

retry_mapview:;
  SIZE_T ViewSize = (flags & MDBX_RDONLY) ? size : limit;
  status = NtMapViewOfSection(map->section, GetCurrentProcess(), &map->address,
                              /* ZeroBits */ 0,
                              /* CommitSize */ 0,
                              /* SectionOffset */ NULL, &ViewSize,
                              /* InheritDisposition */ ViewUnmap,
                              /* AllocationType */ (flags & MDBX_RDONLY) ? 0 : MEM_RESERVE,
                              /* Win32Protect */ (flags & MDBX_WRITEMAP) ? PAGE_READWRITE : PAGE_READONLY);

  if (!NT_SUCCESS(status)) {
    if (status == /* STATUS_CONFLICTING_ADDRESSES */ 0xC0000018 && map->address && limit != map->length) {
      /* try remap at another base address, but only if the limit is changing */
      map->address = NULL;
      goto retry_mapview;
    }
    NtClose(map->section);
    map->section = NULL;

    if (map->address && (size != map->current || limit != map->length)) {
      /* try remap with previously size and limit,
       * but will return MDBX_SIGN on success */
      rc = MDBX_SIGN;
      size = map->current;
      limit = map->length;
      goto retry_file_and_section;
    }

    /* no way to recovery */
    goto bailout_ntstatus;
  }
  assert(map->address != MAP_FAILED);

  map->current = (size_t)SectionSize.QuadPart;
  map->length = ViewSize;
  return rc;
#else
  if (limit != map->length) {
    void *ptr = mremap(map->address, map->length, limit, MREMAP_MAYMOVE);
    if (ptr == MAP_FAILED)
      return errno;
    map->address = ptr;
    map->length = limit;
  }
  return (flags & MDBX_RDONLY) ? MDBX_SUCCESS : mdbx_ftruncate(map->fd, size);
#endif
}

/*----------------------------------------------------------------------------*/

__cold void mdbx_jitter(char /* bool */ tiny) {
  for (;;) {
#if defined(_M_IX86) || defined(_M_X64) || defined(__i386__) || defined(__x86_64__)
    const unsigned salt = 277u * (unsigned)__rdtsc();
#else
    const unsigned salt = rand();
#endif

    const unsigned coin = salt % (tiny ? 29u : 43u);
    if (coin < 43 / 3)
      break;
#if defined(_WIN32) || defined(_WIN64)
    SwitchToThread();
    if (coin > 43 * 2 / 3)
      Sleep(1);
#else
    sched_yield();
    if (coin > 43 * 2 / 3)
      usleep(coin);
#endif
  }
}
