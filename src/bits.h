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
 * <http://www.OpenLDAP.org/license.html>. */

#pragma once
#include "config.h"

/* *INDENT-OFF* */
/* clang-format off */

/*----------------------------------------------------------------------------*/
/* lck stuff */

#if defined(_WIN32) || defined(_WIN64)
#   undef MDBX_OSAL_LOCK
#   define MDBX_OSAL_LOCK_SIGN UINT32_C(0xF10C)
#else
#   define MDBX_OSAL_LOCK pthread_mutex_t
#   define MDBX_OSAL_LOCK_SIGN UINT32_C(0x8017)
#endif /* MDBX_OSAL_LOCK */

/*----------------------------------------------------------------------------*/

/* Should be defined before any includes */
#ifndef _GNU_SOURCE
#   define _GNU_SOURCE 1
#endif
#ifndef _FILE_OFFSET_BITS
#   define _FILE_OFFSET_BITS 64
#endif

#ifdef _MSC_VER
#   ifndef _CRT_SECURE_NO_WARNINGS
#       define _CRT_SECURE_NO_WARNINGS
#   endif
#if _MSC_VER > 1800
#   pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#pragma warning(disable : 4710) /* 'foo': function not inlined */
#pragma warning(disable : 4711) /* function 'bar' selected for automatic inline expansion */
#if _MSC_VER > 1913
#   pragma warning(disable : 5045) /* Compiler will insert Spectre mitigation... */
#endif
#pragma warning(disable : 4702) /* unreachable code */
#pragma warning(disable : 4127) /* conditional expression is constant */
#pragma warning(disable : 4310) /* cast truncates constant value */
#pragma warning(disable : 4820) /* bytes padding added after data member for aligment */
#pragma warning(disable : 4201) /* nonstandard extension used : nameless struct / union */
#pragma warning(disable : 4204) /* nonstandard extension used: non-constant aggregate initializer */
#pragma warning(disable : 4221) /* nonstandard extension used: 'foo': cannot be initialized using address of automatic variable 'bar' */
#pragma warning(disable : 4214) /* nonstandard extension used: bit field types other than int */
#endif                          /* _MSC_VER (warnings) */

#include "../mdbx.h"
#include "defs.h"

#if defined(__GNUC__) && !__GNUC_PREREQ(4,2)
    /* Actualy libmdbx was not tested with compilers older than GCC from RHEL6.
     * But you could remove this #warning and try to continue at your own risk.
     * In such case please don't rise up an issues related ONLY to old compilers. */
#   warning "libmdbx required GCC >= 4.2"
#endif

#if defined(__clang__) && !__CLANG_PREREQ(3,8)
    /* Actualy libmdbx was not tested with CLANG older than 3.8.
     * But you could remove this #warning and try to continue at your own risk.
     * In such case please don't rise up an issues related ONLY to old compilers. */
#   warning "libmdbx required CLANG >= 3.8"
#endif

#if defined(__GLIBC__) && !__GLIBC_PREREQ(2,12)
    /* Actualy libmdbx was not tested with something older than glibc 2.12 (from RHEL6).
     * But you could remove this #warning and try to continue at your own risk.
     * In such case please don't rise up an issues related ONLY to old systems. */
#   warning "libmdbx required at least GLIBC 2.12."
#endif

#ifdef __SANITIZE_THREAD__
#   warning "libmdbx don't compatible with ThreadSanitizer, you will get a lot of false-positive issues."
#endif /* __SANITIZE_THREAD__ */

#if __has_warning("-Wconstant-logical-operand")
#   if defined(__clang__)
#       pragma clang diagnostic ignored "-Wconstant-logical-operand"
#   elif defined(__GNUC__)
#       pragma GCC diagnostic ignored "-Wconstant-logical-operand"
#   else
#      pragma warning disable "constant-logical-operand"
#   endif
#endif /* -Wconstant-logical-operand */

#if defined(__LCC__) && (__LCC__ <= 121)
    /* bug #2798 */
#   pragma diag_suppress alignment_reduction_ignored
#elif defined(__ICC)
#   pragma warning(disable: 3453 1366)
#elif __has_warning("-Walignment-reduction-ignored")
#   if defined(__clang__)
#       pragma clang diagnostic ignored "-Walignment-reduction-ignored"
#   elif defined(__GNUC__)
#       pragma GCC diagnostic ignored "-Walignment-reduction-ignored"
#   else
#       pragma warning disable "alignment-reduction-ignored"
#   endif
#endif /* -Walignment-reduction-ignored */

#include "osal.h"
#include "ualb.h"

/* *INDENT-ON* */
/* clang-format on */

/*----------------------------------------------------------------------------*/
/* Basic constants and types */

/* The minimum number of keys required in a databook page.
 * Setting this to a larger value will place a smaller bound on the
 * maximum size of a data item. Data items larger than this size will
 * be pushed into overflow pages instead of being stored directly in
 * the B-tree node. This value used to default to 4. With a page size
 * of 4096 bytes that meant that any item larger than 1024 bytes would
 * go into an overflow page. That also meant that on average 2-3KB of
 * each overflow page was wasted space. The value cannot be lower than
 * 2 because then there would no longer be a tree structure. With this
 * value, items larger than 2KB will go into overflow pages, and on
 * average only 1KB will be wasted. */
#define MDBX_MINKEYS 2

/* A stamp that identifies a file as an MDBX file.
 * There's nothing special about this value other than that it is easily
 * recognizable, and it will reflect any byte order mismatches. */
#define MDBX_MAGIC UINT64_C(/* 56-bit prime */ 0x59659DBDEF4C11)

/* The version number for a databook's datafile format. */
#define MDBX_DATA_VERSION 3
/* The version number for a databook's lockfile format. */
#define MDBX_LOCK_VERSION 3

/* Number of AAs in metapage (gaco and main) - also hardcoded elsewhere */
#define CORE_AAH 2
#define MAX_AAH (INT16_MAX - CORE_AAH)

/* A page number in the databook.
 *
 * MDBX uses 32 bit for page numbers. This limits databook
 * size up to 2^44 bytes, in case of 4K pages. */
typedef uint32_t pgno_t;
#define PRIaPGNO PRIu32
#define MAX_PAGENO ((pgno_t)UINT64_C(0xffffFFFFffff))
#define MIN_PAGENO MDBX_NUM_METAS

/* A transaction ID. */
typedef uint64_t txnid_t;
#define PRIaTXN PRIi64
#if MDBX_DEVEL
#define MIN_TXNID (UINT64_MAX >> 1)
#elif MDBX_DEBUG
#define MIN_TXNID UINT64_C(0x100000000)
#else
#define MIN_TXNID UINT64_C(1)
#endif /* MIN_TXNID */
#define MAX_TXNID UINT64_MAX

/* Used for offsets within a single page.
 * Since memory pages are typically 4 or 8KB in size, 12-13 bits,
 * this is plenty. */
typedef uint16_t indx_t;
#define INDX_MAX UINT16_MAX

typedef uint64_t checksum_t;

#define MEGABYTE ((size_t)1 << 20)

/* The maximum size of a associative array page.
 *
 * It is 64K, but value - PAGEHDRSZ must fit in page_t.mp_upper.
 *
 * MDBX will use associative array pages < OS pages if needed.
 * That causes more I/O in write transactions: The OS must
 * know (read) the whole page before writing a partial page.
 *
 * Note that we don't currently support Huge pages. On Linux,
 * regular data files cannot use Huge pages, and in general
 * Huge pages aren't actually pageable. We rely on the OS
 * demand-pager to read our data and page it out when memory
 * pressure from other processes is high. So until OSs have
 * actual paging support for Huge pages, they're not viable. */
#define MAX_PAGESIZE 0x10000u
#define MIN_PAGESIZE 512u

#define MIN_MAPSIZE (MIN_PAGESIZE * MIN_PAGENO)
#if defined(_WIN32) || defined(_WIN64)
#define MAX_MAPSIZE32 UINT32_C(0x38000000)
#else
#define MAX_MAPSIZE32 UINT32_C(0x7ff80000)
#endif
#define MAX_MAPSIZE64                                                                                         \
  ((sizeof(pgno_t) > 4) ? UINT64_C(0x7fffFFFFfff80000) : MAX_PAGENO * (uint64_t)MAX_PAGESIZE)

#define MAX_MAPSIZE ((sizeof(size_t) < 8) ? MAX_MAPSIZE32 : MAX_MAPSIZE64)

/*----------------------------------------------------------------------------*/
/* Core structures for databook and shared memory (i.e. format definition) */
#pragma pack(push, 1)

#define MDBX_CACHELINE_ALIGN_PAD(name, bytes_since)                                                           \
  uint8_t name[MDBX_CACHELINE_SIZE - (bytes_since) % MDBX_CACHELINE_SIZE]

/* Information about a single associative array in the databook. */
typedef struct aatree {
  uint16_t aa_flags16; /* see AA flags */
  uint16_t aa_depth16; /* stat: depth of this tree */
  uint32_t aa_xsize32; /* pagesize or keysize for DFL pages */
  /* uint64_t boundary -----------------------------------------------------*/
  pgno_t aa_root;         /* the root page of this tree */
  pgno_t aa_branch_pages; /* stat: number of internal pages */
  /* uint64_t boundary -----------------------------------------------------*/
  pgno_t aa_leaf_pages;     /* stat: number of leaf pages */
  pgno_t aa_overflow_pages; /* stat: number of overflow pages */
  /* uint64_t boundary -----------------------------------------------------*/
  uint64_t aa_entries; /* stat: number of data items */
  /* uint64_t boundary -----------------------------------------------------*/
  uint64_t aa_genseq; /* AA sequence counter */
  /* uint64_t boundary -----------------------------------------------------*/
  txnid_t aa_modification_txnid;
  /* uint64_t boundary -----------------------------------------------------*/
  MDBX_time_t aa_modification_time;
  /* uint64_t boundary -----------------------------------------------------*/
  union {
    struct {
      txnid_t aa_creation_txnid;
      /* uint64_t boundary -------------------------------------------------*/
      MDBX_time_t aa_creation_time;
    };
    uint64_t meta_gaco_reserved[2];
  };
  /* uint64_t boundary -----------------------------------------------------*/
  checksum_t aa_merkle; /* Merkle tree checksum */
} aatree_t;

/* Meta page content.
 * A meta page is the start point for accessing a databook snapshot.
 * Pages 0,1,2 are meta pages. */
typedef struct meta {
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with MDBX_DATA_VERSION. */
  uint64_t mm_magic_and_version;

  /* uint64_t boundary -----------------------------------------------------*/
  /* txnid that committed this page, the first of a two-phase-update pair */
  volatile txnid_t mm_txnid_a;

  /* uint64_t boundary -----------------------------------------------------*/
  struct {
    uint16_t grow16;   /* dxb-file growth step in pages */
    uint16_t shrink16; /* dxb-file shrink threshold in pages */
    pgno_t lower;      /* minimal size of dxb-file in pages */
                       /* ------------------------------- uint64_t boundary */
    pgno_t upper;      /* maximal size of dxb-file in pages */
    pgno_t now;        /* current size of dxb-file in pages */
                       /* ------------------------------- uint64_t boundary */
    pgno_t next;       /* first unused page in the dxb-file (begin-to-end allocation),
                        * but actually the file may be shorter. */
  } mm_dxb_geo;

  struct {
    uint16_t grow16;   /* sld-file growth step in pages */
    uint16_t shrink16; /* sld-file shrink threshold in pages */
                       /* ------------------------------- uint64_t boundary */
    pgno_t lower;      /* minimal size of sld-file in pages */
    pgno_t upper;      /* maximal size of sld-file in pages */
                       /* ------------------------------- uint64_t boundary */
    pgno_t now;        /* current size of sld-file in pages */
    pgno_t next;       /* first unused page in the sld-file (top-down allocation) */
  } mm_sld_geo;

  /* uint64_t boundary -----------------------------------------------------*/
  aatree_t mm_aas[CORE_AAH]; /* first is GACO space, 2nd is main AA */
                             /* The size of pages used in this AA */
#define mm_psize32 mm_aas[MDBX_GACO_AAH].aa_xsize32
/* Databook features, see MDBX_db_features_t (zero for now) */
#define mm_features16 mm_aas[MDBX_GACO_AAH].aa_flags16
/* Any persistent databook flags, see MDBX_aa_flags_t */
#define mm_db_flags16 mm_aas[MDBX_MAIN_AAH].aa_flags16
  MDBX_canary_t mm_canary;
  /* uint64_t boundary -------------------------------------------------------*/

#define MDBX_DATASIGN_NONE 0u
#define MDBX_DATASIGN_WEAK 1u
#define SIGN_IS_WEAK(sign) ((sign) == MDBX_DATASIGN_WEAK)
#define SIGN_IS_STEADY(sign) ((sign) > MDBX_DATASIGN_WEAK)
#define META_IS_WEAK(meta) SIGN_IS_WEAK((meta)->mm_sign_checksum)
#define META_IS_STEADY(meta) SIGN_IS_STEADY((meta)->mm_sign_checksum)
  volatile checksum_t mm_sign_checksum;

  /* uint64_t boundary -----------------------------------------------------*/
  /* txnid that committed this page, the second of a two-phase-update pair */
  volatile txnid_t mm_txnid_b;
} meta_t;

/* Page flags, e.g. page_t.mp_flags */
typedef enum mp_flags {
  /* page type flags */
  P_BRANCH = 1 << 0 /* branch page */,
  P_LEAF = 1 << 1 /* leaf page */,
  P_OVERFLOW = 1 << 2 /* overflow page */,
  P_META = 1 << 3 /* meta page */,
  P_DFL = 1 << 4 /* for MDBX_DUPFIXED records */,
  P_SUBP = 1 << 5 /* for MDBX_DUPSORT sub-pages */,
  /* page state flags */
  P_DIRTY = 1 << 8 /* dirty page, also set for P_SUBP pages */,
  P_LOOSE = 1 << 9 /* page was dirtied then freed, can be reused */,
  P_KEEP = 1 << 10 /* leave this page alone during spill */,
} mp_flags_t;

/* Common header for all page types.
 *
 * P_BRANCH and P_LEAF pages have unsorted 'node_t's at the end, with
 * sorted mp_ptrs[] entries referring to them. Exception: P_DFL pages
 * omit mp_ptrs and pack sorted MDBX_DUPFIXED values after the page header.
 *
 * P_OVERFLOW records occupy one or more contiguous pages where only the
 * first has a page header. They hold the real data of NODE_BIGDATA nodes.
 *
 * P_SUBP sub-pages are small leaf "pages" with duplicate data.
 * A node with flag NODE_DUP but not NODE_SUBTREE contains a
 * sub-page. (Duplicate data can also go in sub-AAs, which use normal pages.)
 *
 * P_META pages contain meta_t, the start point of an MDBX snapshot.
 *
 * Each non-metapage up to meta_t.mm_last_pg is reachable exactly once
 * in the snapshot: Either used by a associative array or listed in a GACO
 * record. */
typedef struct page {
  union {
    struct page *mp_next;     /* for in-memory list of freed pages,
                               * must be first field, see NEXT_LOOSE_PAGE */
    checksum_t page_checksum; /* checksum of page content or a txnid during
                               * which the page has been updated */
  };
  /* uint64_t boundary -----------------------------------------------------*/
  uint16_t mp_leaf2_ksize16; /* key size if this is a DFL page */
  uint16_t mp_flags16;
  union {
    struct {
      indx_t mp_lower; /* lower bound of free space */
      indx_t mp_upper; /* upper bound of free space */
    };
    uint32_t mp_pages; /* number of overflow pages */
  };

  /* uint64_t boundary -----------------------------------------------------*/
  pgno_t mp_pgno; /* page number */

  /* dynamic size */
  union {
    indx_t mp_ptrs[1];
    struct {
      uint32_t mp_meta_alignment_pad;
      /* uint64_t boundary- ------------------------------------------------*/
      meta_t mp_meta;
    };
    uint8_t mp_data[1];
  };
} page_t;

/* Size of the page header, excluding dynamic data at the end */
#define PAGEHDRSZ ((unsigned)offsetof(page_t, mp_data))

/* Reader Lock Table
 *
 * Readers don't acquire any locks for their data access. Instead, they
 * simply record their transaction ID in the reader table. The reader
 * mutex is needed just to find an empty slot in the reader table. The
 * slot's address is saved in thread-specific data so that subsequent
 * read transactions started by the same thread need no further locking to
 * proceed.
 *
 * If MDBX_NOTLS is set, the slot address is not saved in thread-specific data.
 * No reader table is used if the databook is on a read-only filesystem.
 *
 * Since the databook uses multi-version concurrency control, readers don't
 * actually need any locking. This table is used to keep track of which
 * readers are using data from which old transactions, so that we'll know
 * when a particular old transaction is no longer in use. Old transactions
 * that have discarded any data pages can then have those pages reclaimed
 * for use by a later write transaction.
 *
 * The lock table is constructed such that reader slots are aligned with the
 * processor's cache line size. Any slot is only ever used by one thread.
 * This alignment guarantees that there will be no contention or cache
 * thrashing as threads update their own slot info, and also eliminates
 * any need for locking when accessing a slot.
 *
 * A writer thread will scan every slot in the table to determine the oldest
 * outstanding reader transaction. Any freed pages older than this will be
 * reclaimed by the writer. The writer doesn't use any locks when scanning
 * this table. This means that there's no guarantee that the writer will
 * see the most up-to-date reader info, but that's not required for correct
 * operation - all we need is to know the upper bound on the oldest reader,
 * we don't care at all about the newest reader. So the only consequence of
 * reading stale information here is that old pages might hang around a
 * while longer before being reclaimed. That's actually good anyway, because
 * the longer we delay reclaiming old pages, the more likely it is that a
 * string of contiguous pages can be found after coalescing old pages from
 * many old transactions together. */

/* The actual reader record, with cacheline padding. */
typedef struct MDBX_reader {
  /* Current Transaction ID when this transaction began, or (txnid_t)-1.
   * Multiple readers that start at the same time will probably have the
   * same ID here. Again, it's not important to exclude them from
   * anything; all we need to know is which version of the databook they
   * started from so we can avoid overwriting any data used in that
   * particular version. */
  volatile txnid_t mr_txnid;
  /* TODO: MDBX_time_t mr_timestamp; */

  /* The information we store in a single slot of the reader table.
   * In addition to a transaction ID, we also record the process and
   * thread ID that owns a slot, so that we can detect stale information,
   * e.g. threads or processes that went away without cleaning up.
   *
   * NOTE: We currently don't check for stale records.
   * We simply re-init the table when we know that we're the only process
   * opening the lock file. */

  /* The process ID of the process owning this reader txn. */
  volatile MDBX_pid_t mr_pid;
  /* The thread ID of the thread owning this txn. */
  volatile MDBX_tid_t mr_tid;

  /* padding for cache line alignment */
  MDBX_CACHELINE_ALIGN_PAD(alignment_pad, sizeof(txnid_t) + sizeof(MDBX_pid_t) + sizeof(MDBX_tid_t));
} MDBX_reader_t;

/* The header for the reader table (a memory-mapped lock file). */
typedef struct MDBX_lockinfo {
  /*------------------------------------------------------------- cacheline */
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with with MDBX_LOCK_VERSION. */
  uint64_t li_magic_and_version;
  /* uint64_t boundary -----------------------------------------------------*/
  /* Format of this lock file. Must be set to MDBX_LOCK_FORMAT. */
  uint32_t li_os_and_format;
  /* Flags which databook was opened. */
  volatile uint32_t li_regime;
  /* uint64_t boundary -----------------------------------------------------*/
  MDBX_id128_t li_bootid /* host's bootid when DB was changed last time */;
  /* uint64_t boundary -----------------------------------------------------*/
  /* Treshold to force synchronous flush */
  volatile uint32_t li_autosync_threshold;
  volatile uint32_t li_reserved32 /* reserved */;
  /* uint64_t boundary -----------------------------------------------------*/

  /* TODO: use CMake for calculation on pre-build stage */
  MDBX_CACHELINE_ALIGN_PAD(li_alignment_pad1, sizeof(uint64_t /* li_magic_and_version */) +
                                                  sizeof(uint32_t /* li_os_and_format */) +
                                                  sizeof(uint32_t /* li_regime */) +
                                                  sizeof(MDBX_id128_t /* li_bootid */) +
                                                  sizeof(uint64_t /* li_autosync_threshold */));
  /*------------------------------------------------------------- cacheline */

#ifdef MDBX_OSAL_LOCK
  union {
    /* Mutex protecting write access to the database. */
    MDBX_OSAL_LOCK li_wmutex;
    uint64_t li_wmutex_pad[(sizeof(MDBX_OSAL_LOCK) + 7) / 8] /* 24 .. 40 bytes for pthread */;
  };
#define MDBX_OSAL_LOCK_ALIGNEDSIZE ((sizeof(MDBX_OSAL_LOCK) + 7) & ~7u)
#else
#define MDBX_OSAL_LOCK_ALIGNEDSIZE 0
#endif /* MDBX_OSAL_LOCK */

  /* uint64_t boundary -----------------------------------------------------*/
  volatile txnid_t li_oldest;
  volatile uint64_t li_dirty_volume; /* Total dirty/non-sync'ed bytes since the last
                                        mdbx_sync() */
  /* uint64_t boundary -----------------------------------------------------*/
  volatile MDBX_tid_t li_wowner_tid;
  volatile MDBX_pid_t li_wowner_pid;

  /* TODO: use CMake for calculation on pre-build stage */
  MDBX_CACHELINE_ALIGN_PAD(li_alignment_pad2, MDBX_OSAL_LOCK_ALIGNEDSIZE /* li_wmutex */
                                                  + sizeof(txnid_t /* li_oldest */) +
                                                  sizeof(uint64_t /* li_dirty_volume */) +
                                                  sizeof(MDBX_tid_t /* li_wowner_tid */) +
                                                  sizeof(MDBX_pid_t /* li_wowner_pid */));

/*------------------------------------------------------------- cacheline */
#ifdef MDBX_OSAL_LOCK
  union {
    /* Mutex protecting readers registration access to this table. */
    MDBX_OSAL_LOCK li_rmutex;
    uint64_t li_rmutex_pad[(sizeof(MDBX_OSAL_LOCK) + 7) / 8];
  };
#endif /* MDBX_OSAL_LOCK */
  volatile MDBX_tid_t li_rowner_tid;
  volatile MDBX_pid_t li_rowner_pid;

  /* The number of slots that have been used in the reader table.
   * This always records the maximum count, it is not decremented
   * when readers release their slots. */
  volatile uint32_t li_numreaders;
  volatile uint32_t li_readers_refresh_flag;

  /* TODO: use CMake for calculation on pre-build stage */
  /* TODO: use CMake for calculation on pre-build stage */
  MDBX_CACHELINE_ALIGN_PAD(li_alignment_pad3, MDBX_OSAL_LOCK_ALIGNEDSIZE /* li_rmutex */
                                                  + sizeof(uint32_t /* li_numreaders */) +
                                                  sizeof(uint32_t /* li_readers_refresh_flag */) +
                                                  sizeof(MDBX_tid_t /* li_rowner_tid */) +
                                                  sizeof(MDBX_pid_t /* li_rowner_pid */));

  /*------------------------------------------------------------- cacheline */
  MDBX_reader_t li_readers[1];
} MDBX_lockinfo_t;

#pragma pack(pop)

#define MDBX_LOCKINFO_WHOLE_SIZE                                                                              \
  ((sizeof(MDBX_lockinfo_t) + MDBX_CACHELINE_SIZE - 1) & ~((size_t)MDBX_CACHELINE_SIZE - 1))

/* Lockfile format signature: version, features and field layout */
#define MDBX_LOCK_FORMAT                                                                                      \
  ((MDBX_OSAL_LOCK_SIGN << 16) + (uint16_t)(MDBX_LOCKINFO_WHOLE_SIZE + MDBX_CACHELINE_SIZE - 1))

#define MDBX_DATA_MAGIC ((MDBX_MAGIC << 8) + MDBX_DATA_VERSION)

#define MDBX_LOCK_MAGIC ((MDBX_MAGIC << 8) + MDBX_LOCK_VERSION)

/*----------------------------------------------------------------------------*/
/* Two kind lists of pages (aka PNL) */

/* An PNL is an Page Number List, a sorted array of IDs. The first element of
 * the array is a counter for how many actual page-numbers are in the list.
 * PNLs are sorted in descending order, this allow cut off a page with lowest
 * pgno (at the tail) just truncating the list */
#define MDBX_PNL_ASCENDING 0
typedef pgno_t *MDBX_PNL;

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_ORDERED(first, last) ((first) < (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) >= (last))
#else
#define MDBX_PNL_ORDERED(first, last) ((first) > (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) <= (last))
#endif

/* List of txnid, only for MDBX_env_t.mt_lifo_reclaimed */
typedef txnid_t *MDBX_TXL;

/* An ID2 is an ID/pointer pair. */
typedef struct MDBX_ID2 {
  pgno_t mid;   /* The ID */
  page_t *mptr; /* The pointer */
} MDBX_ID2;

/* An ID2L is an ID2 List, a sorted array of ID2s.
 * The first element's mid member is a count of how many actual
 * elements are in the array. The mptr member of the first element is
 * unused. The array is sorted in ascending order by mid. */
typedef MDBX_ID2 *MDBX_ID2L;

/* PNL sizes - likely should be even bigger
 * limiting factors: sizeof(pgno_t), thread stack size */
#define MDBX_PNL_LOGN                                                                                         \
  16 /* MDBX_PNL_DB_SIZE = 2^16, MDBX_PNL_UM_SIZE = 2^17                                                      \
      */
#define MDBX_PNL_DB_SIZE (1 << MDBX_PNL_LOGN)
#define MDBX_PNL_UM_SIZE (1 << (MDBX_PNL_LOGN + 1))

#define MDBX_PNL_DB_MAX (MDBX_PNL_DB_SIZE - 1)
#define MDBX_PNL_UM_MAX (MDBX_PNL_UM_SIZE - 1)

#define MDBX_PNL_SIZEOF(pl) (((pl)[0] + 1) * sizeof(pgno_t))
#define MDBX_PNL_IS_ZERO(pl) ((pl)[0] == 0)
#define MDBX_PNL_CPY(dst, src) (memcpy(dst, src, MDBX_PNL_SIZEOF(src)))
#define MDBX_PNL_FIRST(pl) ((pl)[1])
#define MDBX_PNL_LAST(pl) ((pl)[(pl)[0]])

/* Current max length of an mdbx_pnl_alloc()ed PNL */
#define MDBX_PNL_ALLOCLEN(pl) ((pl)[-1])

/*----------------------------------------------------------------------------*/
/* Internal structures */

/* Environmant AA-handle context.
 * The information here is mostly static/read-only. There is
 * only a single copy of this record in the open environment. */
typedef struct env_aah {
  uint16_t ax_refcounter16;
  uint16_t ax_flags16;
  union {
    struct {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
      uint16_t ax_ord16, ax_seqaah16;
#else
      uint16_t ax_sequence16, ax_ord16;
#endif
    };
    uint32_t ax_aah;
  };
  MDBX_comparer_t *ax_kcmp; /* function for comparing keys */
  MDBX_comparer_t *ax_dcmp; /* function for comparing data items */
  txnid_t ax_since;
  txnid_t ax_until;
  MDBX_iov_t ax_ident; /* name of the associative array */
} ahe_t;

/* Transaction AA-handle context. */
typedef struct txn_aah {
  struct {
    uint16_t depth16; /* stat: depth of this tree */
    uint16_t flags16; /* see AA flags */
    uint32_t xsize32; /* pagesize or keysize for DFL pages */
    /* uint64_t boundary ---------------------------------------------------*/
    pgno_t root;         /* the root page of this tree */
    pgno_t branch_pages; /* stat: number of internal pages */
    /* uint64_t boundary ---------------------------------------------------*/
    pgno_t leaf_pages;     /* stat: number of leaf pages */
    pgno_t overflow_pages; /* stat: number of overflow pages */
    /* uint64_t boundary ---------------------------------------------------*/
    uint64_t entries; /* stat: number of data items */
    /* uint64_t boundary ---------------------------------------------------*/
    uint64_t genseq; /* AA sequence counter */
    /* uint64_t boundary ---------------------------------------------------*/
    uint64_t modification_txnid;
    /* uint64_t boundary ---------------------------------------------------*/
    MDBX_time_t modification_time;
    /* uint64_t boundary ---------------------------------------------------*/
    uint64_t creation_txnid;
    /* uint64_t boundary ---------------------------------------------------*/
    MDBX_time_t creation_time;
  } aa;

  /* uint64_t boundary -----------------------------------------------------*/
  ahe_t *ahe;

  struct {
    uint16_t seq16;
    union {
      uint8_t state8;
      uint16_t kind_and_state16;
    };
  } ah;
} aht_t;

typedef struct ahe_rc {
  ahe_t *ahe;
  int err;
} ahe_rc_t;

typedef struct aht_rc {
  aht_t *aht;
  int err;
} aht_rc_t;

typedef struct node node_t;
typedef struct cursor cursor_t;
typedef struct subcursor subcur_t;

typedef struct node_rc {
  node_t *node;
  bool exact;
} node_rc_t;

/* A databook transaction.
 * Every operation requires a transaction handle. */
struct MDBX_txn {
#define MDBX_MT_SIGNATURE UINT32_C(0x93D53A31)
  MDBX_txn_base_t base;
#define mt_signature base.signature
#define mt_env base.env
#define mt_txnid base.txnid

  MDBX_txn_t *mt_parent; /* parent of a nested txn */
  /* Nested txn under this txn, set together with flag MDBX_TXN_HAS_CHILD */
  MDBX_txn_t *mt_child;
  pgno_t mt_next_pgno; /* next unallocated page */
  pgno_t mt_end_pgno;  /* corresponding to the current size of datafile */
                       /* The ID of this transaction. IDs are integers incrementing from 1.
                        * Only committed write transactions increment the ID. If a transaction
                        * aborts, the ID may be re-used by the next writer. */
  /* The list of reclaimed txns from GACO */
  MDBX_TXL mt_lifo_reclaimed;
  /* The list of pages that became unused during this transaction. */
  MDBX_PNL mt_befree_pages;
  /* The list of loose pages that became unused and may be reused
   * in this transaction, linked through NEXT_LOOSE_PAGE(page). */
  page_t *mt_loose_pages;
  /* Number of loose pages (mt_loose_pages) */
  unsigned mt_loose_count;
  /* The sorted list of dirty pages we temporarily wrote to disk
   * because the dirty list was full. page numbers in here are
   * shifted left by 1, deleted slots have the LSB set. */
  MDBX_PNL mt_spill_pages;
  union {
    /* For write txns: Modified pages. Sorted when not MDBX_WRITEMAP. */
    MDBX_ID2L mt_rw_dirtylist;
    /* For read txns: This thread/txn's reader table slot, or NULL. */
    MDBX_reader_t *mt_ro_reader;
  };
  /* Array of txn_aah_t records for each known AA */
  aht_t *txn_aht_array;
  /* In write txns, array of cursors for each AA */
  MDBX_cursor_t **mt_cursors;

  /* Number of AAH records in use, or 0 when the txn is finished.
   * This number only ever increments until the txn finishes; we
   * don't decrement it when individual AA handles are closed. */
  unsigned txn_ah_num;

  /* Transaction Flags */
  MDBX_flags_t mt_flags;

  /* dirtylist room: Array size - dirty pages visible to this txn.
   * Includes ancestor txns' dirty pages not hidden by other txns'
   * dirty/spilled pages. Thus commit(nested txn) has room to merge
   * dirtylist into mt_parent after freeing hidden mt_parent pages. */
  unsigned mt_dirtyroom;

  MDBX_tid_t mt_owner; /* thread ID that owns this transaction */
  MDBX_canary_t mt_canary;
};

/* State of GACO old pages, stored in the MDBX_env_t */
typedef struct pgstate {
  MDBX_PNL mf_reclaimed_pglist; /* Reclaimed GACO pages, or NULL before use */
  txnid_t mf_last_reclaimed;    /* ID of last used record, or 0 if
                                   !mf_reclaimed_pglist */
} pgstate_t;

/* Nested transaction */
typedef struct nested_txn {
  MDBX_txn_t mnt_txn;    /* the transaction */
  pgstate_t mnt_pgstate; /* parent transaction's saved freestate */
} nested_txn_t;

/* Enough space for 2^32 nodes with minimum of 2 keys per node. I.e., plenty.
 * At 4 keys per node, enough for 2^64 nodes, so there's probably no need to
 * raise this on a 64 bit machine. */
#define CURSOR_STACK 32

/* Cursors are used for all AA operations.
 * A cursor holds a path of (page pointer, key index) from the AA
 * root to a position in the AA, plus other state. MDBX_DUPSORT
 * cursors include an xcursor to the current data item. Write txns
 * track their cursors and keep them up to date when data moves.
 * Exception: An xcursor's pointer to a P_SUBP page can be stale.
 * (A node with NODE_DUP but no NODE_SUBTREE contains a subpage). */
struct cursor {
  aht_t *mc_aht /* The AA-handle for this cursor */;
  /* The transaction that owns this cursor */
  MDBX_txn_t *mc_txn /* TODO: use txn field from MDBX_txn_base_t */;
  unsigned mc_snum /* number of pushed pages */;
  unsigned mc_top /* index of top page, normally mc_snum-1 */;

  union {
    struct {
      uint8_t mc_state8;
      uint8_t mc_kind8;
    };
    unsigned mc_kind_and_state;
  };

  page_t *mc_pg[CURSOR_STACK] /* stack of pushed pages */;
  indx_t mc_ki[CURSOR_STACK] /* stack of page indices */;
};

enum /* Cursor flags. */ {
  C_INITIALIZED = 1 << 0 /* cursor has been initialized and is valid */,
  C_EOF = 1 << 1 /* No more data */,
  C_AFTERDELETE = 1 << 2 /* last op was a cursor_del */,
  C_RECLAIMING = 1 << 3 /* GACO lookup is prohibited */,
  C_UNTRACK = 1 << 4 /* Un-track cursor when closing */,

  S_SUBCURSOR = 1 << 5 /* Cursor is a sub-cursor */,
  S_HAVESUB = 1 << 6 /* Cursor have a sub-cursor */,
  S_DUPFIXED = 1 << 7,
};

/* Context for sorted-dup records.
 * We could have gone to a fully recursive design, with arbitrarily
 * deep nesting of sub-AAs. But for now we only handle these
 * levels - main AA, optional sub-AA, sorted-duplicate AA. */
struct subcursor {
  /* A sub-cursor for traversing the dup-aa */
  cursor_t mx_cursor;
  /* The pseudo AA-handle for this dup-aa cursor */
  aht_t mx_aht_body;
  ahe_t mx_ahe_body;
};

struct MDBX_cursor {
#define MDBX_MC_SIGNATURE UINT32_C(0xFE05D5B1)
#define MDBX_MC_READY4CLOSE UINT32_C(0x2817A047)
#define MDBX_MC_WAIT4EOT UINT32_C(0x90E297A7)
#define MDBX_MC_BACKUP UINT32_C(0x82FF6E47)
  MDBX_cursor_base_t mc_base;
#define mc_signature mc_base.signature

  /* Next cursor on this AA in this txn */
  MDBX_cursor_t *mc_next;
  /* Backup of the original cursor if this cursor is a shadow */
  MDBX_cursor_t *mc_backup;
  cursor_t primal;
  subcur_t subcursor;
};

/* The databook. */
struct MDBX_env {
#define MDBX_ME_SIGNATURE UINT32_C(0x9A899641)
  MDBX_env_base_t me_base;
#define me_signature me_base.signature
#define me_userctx me_base.userctx

  mdbx_mmap_t me_dxb_mmap; /* The main data file */
#define me_map me_dxb_mmap.dxb
#define me_dxb_fd me_dxb_mmap.fd
#define me_mapsize me_dxb_mmap.length
  mdbx_mmap_t me_lck_mmap; /* The lock file */
#define me_lck_fd me_lck_mmap.fd
#define me_lck me_lck_mmap.lck

  MDBX_flags_t me_flags32; /* see mdbx_book */
  unsigned me_psize;       /* databook page size, inited from me_os_psize */
  unsigned me_psize2log;   /* log2 of databook page size */
  unsigned me_maxreaders;  /* size of the reader table */
  /* Max MDBX_lockinfo.li_numreaders of interest to mdbx_shutdown() */
  unsigned me_close_readers;
  mdbx_fastmutex_t me_aah_lock;
  unsigned env_ah_num;         /* number of AAs opened */
  unsigned env_ah_max;         /* size of the AA table */
  MDBX_pid_t me_pid;           /* process ID of this env */
  mdbx_thread_key_t me_txkey;  /* thread-key for readers */
  void *me_pagebuf;            /* scratch area for DUPSORT put() */
  MDBX_txn_t *me_current_txn;  /* current write transaction */
  MDBX_txn_t *me_wpa_txn;      /* prealloc'd write transaction */
  ahe_t *env_ahe_array;        /* array of AA-handles info */
  volatile txnid_t *me_oldest; /* ID of oldest reader last time we looked */
  pgstate_t me_pgstate;        /* state of old pages from GACO */
#define me_last_reclaimed me_pgstate.mf_last_reclaimed
#define me_reclaimed_pglist me_pgstate.mf_reclaimed_pglist
  page_t *me_dpages; /* list of malloc'd blocks for re-use */
                     /* PNL of pages that became unused in a write txn */
  MDBX_PNL me_free_pgs;
  /* ID2L of pages written during a write txn. Length MDBX_PNL_UM_SIZE. */
  MDBX_ID2L me_dirtylist;
  /* Max number of freelist items that can fit in a single page */
  unsigned me_maxfree_1pg;
  /* Max size of a node on a page */
  unsigned me_nodemax;
  unsigned me_keymax;        /* max size of a key */
  MDBX_pid_t me_live_reader; /* have liveness lock in reader table */
  txnid_t me_oldest_stub;
  MDBX_rbr_callback_t *me_callback_rbr; /* Callback for kicking laggard readers */

  MDBX_ops_t ops;

  struct {
    size_t lower;  /* minimal size of datafile */
    size_t upper;  /* maximal size of datafile */
    size_t now;    /* current size of datafile */
    size_t grow;   /* step to grow datafile */
    size_t shrink; /* threshold to shrink datafile */
  } me_dxb_geo, me_sld_geo;

#if defined(_WIN32) || defined(_WIN64)
  SRWLOCK me_remap_guard;
  /* Workaround for LockFileEx and WriteFile multithread bug */
  CRITICAL_SECTION me_windowsbug_lock;
#else
  mdbx_fastmutex_t me_remap_guard;
#endif

  char *me_pathname_lck; /* pathname of the LCK file */
  char *me_pathname_dxb; /* pathname of the DXB file */
  char *me_pathname_sld; /* optional pathname of the SLD file (Separate Large Data) */
  char *me_pathname_buf; /* buffer for all pathnames */

#if MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_DBG_ASSERTIONS
  MDBX_assert_callback_t *me_assert_func; /*  Callback for assertion failures */
#endif
#ifdef USE_VALGRIND
  int me_valgrind_handle;
#endif
};

/*----------------------------------------------------------------------------*/

/* Internal error codes, not exposed outside libmdbx */
#define MDBX_NO_ROOT (MDBX_LAST_ERRCODE + 10)

/* Debuging output value of a cursor AAH: Negative in a sub-cursor. */
#define DAAH(mc)                                                                                              \
  (((mc)->mc_kind8 & S_SUBCURSOR) ? -(int)(mc)->mc_aht->ahe->ax_ord16 : (int)(mc)->mc_aht->ahe->ax_ord16)

/* Key size which fits in a DKBUF. */
#define DKBUF_MAXKEYSIZE 511 /* FIXME */

#if MDBX_DEBUG
#define DKBUF char _kbuf[DKBUF_MAXKEYSIZE * 4 + 2]
#define DKEY(x) mdbx_dump_iov(x, _kbuf, DKBUF_MAXKEYSIZE * 2 + 1)
#define DVAL(x) mdbx_dump_iov(x, _kbuf + DKBUF_MAXKEYSIZE * 2 + 1, DKBUF_MAXKEYSIZE * 2 + 1)
#else
#define DKBUF ((void)(0))
#define DKEY(x) ("-")
#define DVAL(x) ("-")
#endif

static inline char cmp2char(ptrdiff_t cmp) {
  if (cmp < 0)
    return '<';
  else if (cmp > 0)
    return '>';
  else
    return '=';
}

/* An invalid page number.
 * Mainly used to denote an empty tree. */
#define P_INVALID 0

/* Test if the flags f are set in a flag word w. */
#define F_ISSET(w, f) (((w) & (f)) == (f))

/* Round n up to an even number. */
#define EVEN(n) (((n) + 1U) & -2) /* sign-extending -2 to match n+1U */

/* Default size of memory map.
 * This is certainly too small for any actual applications. Apps should
 * always set  the size explicitly using mdbx_set_mapsize(). */
#define DEFAULT_MAPSIZE 1048576

/* Number of slots in the reader table.
 * This value was chosen somewhat arbitrarily. The 61 is a prime number,
 * and such readers plus a couple mutexes fit into single 4KB page.
 * Applications should set the table size using mdbx_set_maxreaders(). */
#define DEFAULT_READERS 61

/* Address of first usable data byte in a page, after the header */
static inline void *page_data(page_t *page) { return &page->mp_data; }

/* Number of nodes on a page */
static inline indx_t page_numkeys(const page_t *page) { return page->mp_lower >> 1; }

/* The amount of space remaining in the page */
static inline indx_t page_spaceleft(const page_t *page) {
  int diff = page->mp_upper - page->mp_lower;
  assert(diff >= 0 && diff <= UINT16_MAX);
  return (indx_t)diff;
}

/* The percentage of space used in the page, in tenths of a percent. */
#define PAGEFILL(env, p)                                                                                      \
  (1024UL * ((env)->me_psize - PAGEHDRSZ - page_spaceleft(p)) / ((env)->me_psize - PAGEHDRSZ))
/* The minimum page fill factor, in tenths of a percent.
 * Pages emptier than this are candidates for merging. */
#define FILL_THRESHOLD 256

/* Test if a page is a leaf page */
#define IS_LEAF(p) F_ISSET((p)->mp_flags16, P_LEAF)
/* Test if a page is a DFL page */
#define IS_DFL(p) F_ISSET((p)->mp_flags16, P_DFL)
/* Test if a page is a branch page */
#define IS_BRANCH(p) F_ISSET((p)->mp_flags16, P_BRANCH)
/* Test if a page is an overflow page */
#define IS_OVERFLOW(p) unlikely(F_ISSET((p)->mp_flags16, P_OVERFLOW))
/* Test if a page is a sub page */
#define IS_SUBP(p) F_ISSET((p)->mp_flags16, P_SUBP)

/* The number of overflow pages needed to store the given size. */
#define OVPAGES(env, size) (bytes2pgno(env, PAGEHDRSZ - 1 + (size)) + 1)

/* Link in MDBX_txn_t.mt_loose_pages list.
 * Kept outside the page header, which is needed when reusing the page. */
#define NEXT_LOOSE_PAGE(p) (*(page_t **)((p) + 2))

/*  node_t Flags, i.e. 16 bit node_flags8 */
enum mdbx_node_flags {
  NODE_BIG = 0x01 /* data put on overflow page */,
  NODE_SUBTREE = 0x02 /* data is a subtree */,
  NODE_DUP = 0x04 /* data has duplicates */,
  MDBX_NODE_FLAGS = NODE_BIG | NODE_SUBTREE | NODE_DUP,

  /* valid flags for mdbx_node_add() */
  NODE_ADD_FLAGS = (NODE_DUP | NODE_SUBTREE | MDBX_IUD_RESERVE | MDBX_IUD_APPEND),
};

/* Header for a single key/data pair within a page.
 * Used in pages of type P_BRANCH and P_LEAF without P_DFL.
 * We guarantee 2-byte alignment for 'node_t's.
 *
 * mn_lo16 and mn_hi16 are used for data size on leaf nodes, and for child
 * pgno on branch nodes.  On 64 bit platforms, node_flags8 is also maybe used
 * for pgno.  (Branch nodes have no flags).  Lo and hi are in host byte
 * order in case some accesses can be optimized to 32-bit word access.
 *
 * Leaf node flags describe node contents.  NODE_BIGDATA says the node's
 * data part is the page number of an overflow page with actual data.
 * NODE_DUP and NODE_SUBTREE can be combined giving duplicate data
 * in a sub-page/sub-AA, and named AAs (just NODE_SUBTREE). */
typedef struct node {
  union {
    struct {
      union {
        struct {
          uint16_t mn_lo16, mn_hi16; /* part of data size or pgno */
        };
        uint32_t mn_dsize;
      };
      uint8_t mn_salt8;
      uint8_t node_flags8; /* see mdbx_node_flags */
      uint16_t mn_ksize16; /* key size */
    };
    pgno_t mn_ksize_and_pgno;
  };

  uint8_t mn_data[1]; /* key and data are appended here */
} node_t;

/* Size of the node header, excluding dynamic data at the end */
#define NODESIZE offsetof(node_t, mn_data)

/* Size of a node in a branch page with a given key.
 * This is just the node header plus the key, there is no data. */
#define INDXSIZE(k) (NODESIZE + ((k) == NULL ? 0 : (k)->iov_len))

/* Size of a node in a leaf page with a given key and data.
 * This is node header plus key plus data size. */
#define LEAFSIZE(k, d) (NODESIZE + (k)->iov_len + (d)->iov_len)

/* Address of node i in page p */
static inline node_t *node_ptr(page_t *p, unsigned i) {
  assert(page_numkeys(p) > (unsigned)(i));
  return (node_t *)((char *)(p) + (p)->mp_ptrs[i] + PAGEHDRSZ);
}

/* Address of the key for the node */
#define NODEKEY(node) (void *)((node)->mn_data)

/* Address of the data for a node */
#define NODEDATA(node) (void *)((char *)(node)->mn_data + (node)->mn_ksize16)

/* Get the page number pointed to by a branch node */
static inline pgno_t node_get_pgno(const node_t *node) {
  pgno_t pgno;
  if (UNALIGNED_OK && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
    pgno = node->mn_ksize_and_pgno;
    if (sizeof(pgno_t) > 4)
      pgno &= MAX_PAGENO;
  } else {
    pgno = get_le32_aligned2(&node->mn_ksize_and_pgno);
    if (sizeof(pgno_t) > 4) {
      uint64_t high = get_le16_aligned((const uint16_t *)&node->mn_ksize_and_pgno + 2);
      pgno |= (pgno_t)(high << 32);
    }
  }
  assert(pgno == (get_le64_aligned2(&node->mn_ksize_and_pgno) & MAX_PAGENO));
  return pgno;
}

/* Set the page number in a branch node */
static inline void node_set_pgno(node_t *node, pgno_t pgno) {
  assert(pgno <= MAX_PAGENO);

  if (UNALIGNED_OK && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
    if (sizeof(pgno_t) > 4)
      pgno |= (pgno_t)(((uint64_t)node->mn_ksize16) << 48);
    node->mn_ksize_and_pgno = pgno;
  } else {
    set_le32_aligned2(&node->mn_ksize_and_pgno, (uint32_t)pgno);
    if (sizeof(pgno_t) > 4)
      set_le16_aligned((uint16_t *)&node->mn_ksize_and_pgno + 2, (uint16_t)((uint64_t)pgno >> 32));
  }
  assert(pgno == (get_le64_aligned2(&node->mn_ksize_and_pgno) & MAX_PAGENO));
}

/* Get the size of the data in a leaf node */
static inline size_t node_get_datasize(const node_t *node) { return get_le32_aligned2(&node->mn_dsize); }

/* Set the size of the data for a leaf node */
static inline void node_set_datasize(node_t *node, size_t size) {
  assert(size < INT_MAX);
  set_le32_aligned2(&node->mn_dsize, (uint32_t)size);
}

/* The size of a key in a node */
#define node_get_keysize(node) ((node)->mn_ksize16)

/* The address of a key in a DFL page.
 * DFL pages are used for MDBX_DUPFIXED sorted-duplicate sub-DBs.
 * There are no node headers, keys are stored contiguously. */
#define DFLKEY(p, i, ks) ((char *)(p) + PAGEHDRSZ + ((i) * (ks)))

/* Set the node's key into keyptr, if requested. */
#define MDBX_GET_KEY(node, keyptr)                                                                            \
  do {                                                                                                        \
    if ((keyptr) != NULL) {                                                                                   \
      (keyptr)->iov_len = node_get_keysize(node);                                                             \
      (keyptr)->iov_base = NODEKEY(node);                                                                     \
    }                                                                                                         \
  } while (0)

/* Set the node's key into key. */
#define MDBX_GET_KEY2(node, key)                                                                              \
  do {                                                                                                        \
    key.iov_len = node_get_keysize(node);                                                                     \
    key.iov_base = NODEKEY(node);                                                                             \
  } while (0)

/* max number of pages to commit in one writev() call */
#define MDBX_COMMIT_PAGES 64
#if defined(IOV_MAX) && IOV_MAX < MDBX_COMMIT_PAGES /* sysconf(_SC_IOV_MAX) */
#undef MDBX_COMMIT_PAGES
#define MDBX_COMMIT_PAGES IOV_MAX
#endif

///* Check txn and aah arguments to a function */
//#define TXN_AAH_EXIST(txn, aah, validity)
//  ((aah) < (txn)->txn_ax_num && ((txn)->mt_aah_flags8[aah] & (validity)))

///* Check for misused aah handles */
//#define TXN_AAH_CHANGED(txn, aah)
//  ((txn)->mt_aah_seq[aah] != (txn)->mt_env->me_aah_seq[aah])

/* LY: fast enough on most systems
 *
 *                /
 *                | -1, a < b
 * cmp2int(a,b) = <  0, a == b
 *                |  1, a > b
 *                \
 */
#if 1
#define mdbx_cmp2int(a, b) (((b) > (a)) ? -1 : (a) > (b))
#else
#define mdbx_cmp2int(a, b) (((a) > (b)) - ((b) > (a)))
#endif

static inline pgno_t pgno_add(pgno_t base, pgno_t augend) {
  assert(base <= MAX_PAGENO);
  return (augend < MAX_PAGENO - base) ? base + augend : MAX_PAGENO;
}

static inline pgno_t pgno_sub(pgno_t base, pgno_t subtrahend) {
  assert(base >= MIN_PAGENO);
  return (subtrahend < base - MIN_PAGENO) ? base - subtrahend : MIN_PAGENO;
}

static inline MDBX_seize_result_t seize_failed(MDBX_error_t err) {
  assert(MDBX_IS_ERROR(err));
  const MDBX_seize_result_t result = {err, MDBX_SEIZE_FAILED};
  return result;
}

static inline MDBX_seize_result_t seize_done(MDBX_seize_t seize) {
  assert(seize >= MDBX_SEIZE_FAILED && seize <= MDBX_SEIZE_EXCLUSIVE_FIRST);
  const MDBX_seize_result_t result = {MDBX_SUCCESS, seize};
  return result;
}

#define IS_SEIZE_EXCLUSIVE(seize) ((seize) > MDBX_SEIZE_SHARED)
