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

#define list_extra(fmt, ...) log_extra(MDBX_LOG_LIST, fmt, ##__VA_ARGS__)
#define list_trace(fmt, ...) log_trace(MDBX_LOG_LIST, fmt, ##__VA_ARGS__)
#define list_verbose(fmt, ...) log_verbose(MDBX_LOG_LIST, fmt, ##__VA_ARGS__)
#define list_info(fmt, ...) log_info(MDBX_LOG_LIST, fmt, ##__VA_ARGS__)
#define list_notice(fmt, ...) log_notice(MDBX_LOG_LIST, fmt, ##__VA_ARGS__)
#define list_warning(fmt, ...) log_warning(MDBX_LOG_LIST, fmt, ##__VA_ARGS__)
#define list_error(fmt, ...) log_error(MDBX_LOG_LIST, fmt, ##__VA_ARGS__)
#define list_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_LIST, __func__, __LINE__, "%s, error %d", msg, err)

/*----------------------------------------------------------------------------*/

static __inline size_t pnl2bytes(const size_t size) {
  assert(size > 0 && size <= MDBX_PNL_MAX * 2);
  size_t bytes = mdbx_roundup2(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(pgno_t) * (size + 2),
                               MDBX_PNL_GRANULATE * sizeof(pgno_t)) -
                 MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static __inline pgno_t bytes2pnl(const size_t bytes) {
  size_t size = bytes / sizeof(pgno_t);
  assert(size > 2 && size <= MDBX_PNL_MAX * 2);
  return (pgno_t)size - 2;
}

static MDBX_PNL mdbx_pnl_alloc(size_t size) {
  const size_t bytes = pnl2bytes(size);
  MDBX_PNL pl = malloc(bytes);
  if (likely(pl)) {
#if __GLIBC_PREREQ(2, 12)
    const size_t bytes = malloc_usable_size(pl);
#endif
    pl[0] = bytes2pnl(bytes);
    assert(pl[0] >= size);
    pl[1] = 0;
    pl += 1;
  }
  return pl;
}

static void mdbx_pnl_free(MDBX_PNL pl) {
  if (likely(pl))
    free(pl - 1);
}

/* Shrink the PNL to the default size if it has grown larger */
static void mdbx_pnl_shrink(MDBX_PNL *ppl) {
  assert(bytes2pnl(pnl2bytes(MDBX_PNL_INITIAL)) == MDBX_PNL_INITIAL);
  assert(MDBX_PNL_SIZE(*ppl) <= MDBX_PNL_MAX && MDBX_PNL_ALLOCLEN(*ppl) >= MDBX_PNL_SIZE(*ppl));
  MDBX_PNL_SIZE(*ppl) = 0;
  if (unlikely(MDBX_PNL_ALLOCLEN(*ppl) > MDBX_PNL_INITIAL + MDBX_CACHELINE_SIZE / sizeof(pgno_t))) {
    const size_t bytes = pnl2bytes(MDBX_PNL_INITIAL);
    MDBX_PNL pl = realloc(*ppl - 1, bytes);
    if (likely(pl)) {
#if __GLIBC_PREREQ(2, 12)
      const size_t bytes = malloc_usable_size(pl);
#endif
      *pl = bytes2pnl(bytes);
      *ppl = pl + 1;
    }
  }
}

/* Grow the PNL to the size growed to at least given size */
static int mdbx_pnl_reserve(MDBX_PNL *ppl, const size_t wanna) {
  const size_t allocated = MDBX_PNL_ALLOCLEN(*ppl);
  assert(MDBX_PNL_SIZE(*ppl) <= MDBX_PNL_MAX && MDBX_PNL_ALLOCLEN(*ppl) >= MDBX_PNL_SIZE(*ppl));
  if (likely(allocated >= wanna))
    return MDBX_SUCCESS;

  if (unlikely(wanna > /* paranoia */ MDBX_PNL_MAX))
    return MDBX_TXN_FULL;

  const size_t size = (wanna + wanna - allocated < MDBX_PNL_MAX) ? wanna + wanna - allocated : MDBX_PNL_MAX;
  const size_t bytes = pnl2bytes(size);
  MDBX_PNL pl = realloc(*ppl - 1, bytes);
  if (likely(pl)) {
#if __GLIBC_PREREQ(2, 12)
    const size_t bytes = malloc_usable_size(pl);
#endif
    *pl = bytes2pnl(bytes);
    assert(*pl >= wanna);
    *ppl = pl + 1;
    return MDBX_SUCCESS;
  }
  return MDBX_ENOMEM;
}

/* Make room for num additional elements in an PNL */
static __inline int __must_check_result mdbx_pnl_need(MDBX_PNL *ppl, size_t num) {
  assert(MDBX_PNL_SIZE(*ppl) <= MDBX_PNL_MAX && MDBX_PNL_ALLOCLEN(*ppl) >= MDBX_PNL_SIZE(*ppl));
  assert(num <= MDBX_PNL_MAX);
  const size_t wanna = MDBX_PNL_SIZE(*ppl) + num;
  return likely(MDBX_PNL_ALLOCLEN(*ppl) >= wanna) ? MDBX_SUCCESS : mdbx_pnl_reserve(ppl, wanna);
}

static __inline void mdbx_pnl_xappend(MDBX_PNL pl, pgno_t id) {
  assert(MDBX_PNL_SIZE(pl) < MDBX_PNL_ALLOCLEN(pl));
  MDBX_PNL_SIZE(pl) += 1;
  MDBX_PNL_LAST(pl) = id;
}

/* Append an ID onto an PNL */
static int __must_check_result mdbx_pnl_append(MDBX_PNL *ppl, pgno_t id) {
  /* Too big? */
  if (unlikely(MDBX_PNL_SIZE(*ppl) == MDBX_PNL_ALLOCLEN(*ppl))) {
    int rc = mdbx_pnl_need(ppl, MDBX_PNL_GRANULATE);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  mdbx_pnl_xappend(*ppl, id);
  return MDBX_SUCCESS;
}

/* Append an PNL onto an PNL */
static int __must_check_result mdbx_pnl_append_list(MDBX_PNL *ppl, MDBX_PNL append) {
  int rc = mdbx_pnl_need(ppl, MDBX_PNL_SIZE(append));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  memcpy(MDBX_PNL_END(*ppl), MDBX_PNL_BEGIN(append), MDBX_PNL_SIZE(append) * sizeof(pgno_t));
  MDBX_PNL_SIZE(*ppl) += MDBX_PNL_SIZE(append);
  return MDBX_SUCCESS;
}

/* Append an ID range onto an PNL */
static int __must_check_result mdbx_pnl_append_range(MDBX_PNL *ppl, pgno_t id, size_t n) {
  int rc = mdbx_pnl_need(ppl, n);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  pgno_t *ap = MDBX_PNL_END(*ppl);
  MDBX_PNL_SIZE(*ppl) += (unsigned)n;
  for (pgno_t *const end = MDBX_PNL_END(*ppl); ap < end;)
    *ap++ = id++;
  return MDBX_SUCCESS;
}

__maybe_unused static bool mdbx_pnl_check(MDBX_PNL pl, bool allocated) {
  if (pl) {
    assert(MDBX_PNL_SIZE(pl) <= MDBX_PNL_MAX);
    if (allocated) {
      assert(MDBX_PNL_ALLOCLEN(pl) >= MDBX_PNL_SIZE(pl));
    }
    for (const pgno_t *scan = &MDBX_PNL_LAST(pl); --scan > pl;) {
      assert(MDBX_PNL_ORDERED(scan[0], scan[1]));
      assert(scan[0] >= MDBX_NUM_METAS);
      if (unlikely(MDBX_PNL_DISORDERED(scan[0], scan[1]) || scan[0] < MDBX_NUM_METAS))
        return false;
    }
  }
  return true;
}

/* Merge an PNL onto an PNL. The destination PNL must be big enough */
static void __hot mdbx_pnl_xmerge(MDBX_PNL pnl, MDBX_PNL merge) {
  assert(mdbx_pnl_check(pnl, true));
  assert(mdbx_pnl_check(merge, false));
  pgno_t old_id, merge_id, i = MDBX_PNL_SIZE(merge), j = MDBX_PNL_SIZE(pnl), k = i + j, total = k;
  pnl[0] = MDBX_PNL_ASCENDING ? 0 : ~(pgno_t)0; /* delimiter for pl scan below */
  old_id = pnl[j];
  while (i) {
    merge_id = merge[i--];
    for (; MDBX_PNL_ORDERED(merge_id, old_id); old_id = pnl[--j])
      pnl[k--] = old_id;
    pnl[k--] = merge_id;
  }
  MDBX_PNL_SIZE(pnl) = total;
  assert(mdbx_pnl_check(pnl, true));
}

/* Sort an PNL */
static void __hot mdbx_pnl_sort(MDBX_PNL pnl) {
  /* Max possible depth of int-indexed tree * 2 items/level */
  int istack[sizeof(int) * CHAR_BIT * 2];
  int i, j, k, l, ir, jstack;
  pgno_t a;

/* Quicksort + Insertion sort for small arrays */
#define PNL_SMALL 8
#define PNL_SWAP(a, b)                                                                                        \
  do {                                                                                                        \
    pgno_t tmp_pgno = (a);                                                                                    \
    (a) = (b);                                                                                                \
    (b) = tmp_pgno;                                                                                           \
  } while (0)

  ir = (int)MDBX_PNL_SIZE(pnl);
  l = 1;
  jstack = 0;
  while (1) {
    if (ir - l < PNL_SMALL) { /* Insertion sort */
      for (j = l + 1; j <= ir; j++) {
        a = pnl[j];
        for (i = j - 1; i >= 1; i--) {
          if (MDBX_PNL_DISORDERED(a, pnl[i]))
            break;
          pnl[i + 1] = pnl[i];
        }
        pnl[i + 1] = a;
      }
      if (jstack == 0)
        break;
      ir = istack[jstack--];
      l = istack[jstack--];
    } else {
      k = (l + ir) >> 1; /* Choose median of left, center, right */
      PNL_SWAP(pnl[k], pnl[l + 1]);
      if (MDBX_PNL_ORDERED(pnl[ir], pnl[l]))
        PNL_SWAP(pnl[l], pnl[ir]);

      if (MDBX_PNL_ORDERED(pnl[ir], pnl[l + 1]))
        PNL_SWAP(pnl[l + 1], pnl[ir]);

      if (MDBX_PNL_ORDERED(pnl[l + 1], pnl[l]))
        PNL_SWAP(pnl[l], pnl[l + 1]);

      i = l + 1;
      j = ir;
      a = pnl[l + 1];
      while (1) {
        do
          i++;
        while (MDBX_PNL_ORDERED(pnl[i], a));
        do
          j--;
        while (MDBX_PNL_ORDERED(a, pnl[j]));
        if (j < i)
          break;
        PNL_SWAP(pnl[i], pnl[j]);
      }
      pnl[l + 1] = pnl[j];
      pnl[j] = a;
      jstack += 2;
      if (ir - i + 1 >= j - l) {
        istack[jstack] = ir;
        istack[jstack - 1] = i;
        ir = j - 1;
      } else {
        istack[jstack] = j - 1;
        istack[jstack - 1] = l;
        l = i;
      }
    }
  }
#undef PNL_SMALL
#undef PNL_SWAP
  assert(mdbx_pnl_check(pnl, false));
}

/* Search for an ID in an PNL.
 * [in] pl The PNL to search.
 * [in] id The ID to search for.
 * Returns The index of the first ID greater than or equal to id. */
static unsigned __hot mdbx_pnl_search(MDBX_PNL pnl, pgno_t id) {
  assert(mdbx_pnl_check(pnl, true));

  /* binary search of id in pl
   * if found, returns position of id
   * if not found, returns first position greater than id */
  unsigned base = 0;
  unsigned cursor = 1;
  int val = 0;
  unsigned n = MDBX_PNL_SIZE(pnl);

  while (n > 0) {
    unsigned pivot = n >> 1;
    cursor = base + pivot + 1;
    val = MDBX_PNL_ASCENDING ? mdbx_cmp2int(id, pnl[cursor]) : mdbx_cmp2int(pnl[cursor], id);

    if (val < 0) {
      n = pivot;
    } else if (val > 0) {
      base = cursor;
      n -= pivot + 1;
    } else {
      return cursor;
    }
  }

  if (val > 0)
    ++cursor;

  return cursor;
}

/*----------------------------------------------------------------------------*/

static __inline size_t txl2bytes(const size_t size) {
  assert(size > 0 && size <= MDBX_TXL_MAX * 2);
  size_t bytes = mdbx_roundup2(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(txnid_t) * (size + 2),
                               MDBX_TXL_GRANULATE * sizeof(txnid_t)) -
                 MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static __inline size_t bytes2txl(const size_t bytes) {
  size_t size = bytes / sizeof(txnid_t);
  assert(size > 2 && size <= MDBX_TXL_MAX * 2);
  return size - 2;
}

static MDBX_TXL mdbx_txl_alloc(void) {
  const size_t bytes = txl2bytes(MDBX_TXL_INITIAL);
  MDBX_TXL tl = malloc(bytes);
  if (likely(tl)) {
#if __GLIBC_PREREQ(2, 12)
    const size_t bytes = malloc_usable_size(tl);
#endif
    tl[0] = bytes2txl(bytes);
    assert(tl[0] >= MDBX_TXL_INITIAL);
    tl[1] = 0;
    tl += 1;
  }
  return tl;
}

static void mdbx_txl_free(MDBX_TXL tl) {
  if (likely(tl))
    free(tl - 1);
}

static int mdbx_txl_reserve(MDBX_TXL *ptl, const size_t wanna) {
  const size_t allocated = (size_t)MDBX_PNL_ALLOCLEN(*ptl);
  assert(MDBX_PNL_SIZE(*ptl) <= MDBX_TXL_MAX && MDBX_PNL_ALLOCLEN(*ptl) >= MDBX_PNL_SIZE(*ptl));
  if (likely(allocated >= wanna))
    return MDBX_SUCCESS;

  if (unlikely(wanna > /* paranoia */ MDBX_TXL_MAX))
    return MDBX_TXN_FULL;

  const size_t size = (wanna + wanna - allocated < MDBX_TXL_MAX) ? wanna + wanna - allocated : MDBX_TXL_MAX;
  const size_t bytes = txl2bytes(size);
  MDBX_TXL tl = realloc(*ptl - 1, bytes);
  if (likely(tl)) {
#if __GLIBC_PREREQ(2, 12)
    const size_t bytes = malloc_usable_size(tl);
#endif
    *tl = bytes2txl(bytes);
    assert(*tl >= wanna);
    *ptl = tl + 1;
    return MDBX_SUCCESS;
  }
  return MDBX_ENOMEM;
}

static __inline int __must_check_result mdbx_txl_need(MDBX_TXL *ptl, size_t num) {
  assert(MDBX_PNL_SIZE(*ptl) <= MDBX_TXL_MAX && MDBX_PNL_ALLOCLEN(*ptl) >= MDBX_PNL_SIZE(*ptl));
  assert(num <= MDBX_PNL_MAX);
  const size_t wanna = (size_t)MDBX_PNL_SIZE(*ptl) + num;
  return likely(MDBX_PNL_ALLOCLEN(*ptl) >= wanna) ? MDBX_SUCCESS : mdbx_txl_reserve(ptl, wanna);
}

static __inline void mdbx_txl_xappend(MDBX_TXL tl, txnid_t id) {
  assert(MDBX_PNL_SIZE(tl) < MDBX_PNL_ALLOCLEN(tl));
  MDBX_PNL_SIZE(tl) += 1;
  MDBX_PNL_LAST(tl) = id;
}

static int mdbx_txl_cmp(const void *pa, const void *pb) {
  const txnid_t a = *(MDBX_TXL)pa;
  const txnid_t b = *(MDBX_TXL)pb;
  return mdbx_cmp2int(b, a);
}

static void mdbx_txl_sort(MDBX_TXL ptr) {
  /* LY: temporary */
  qsort(ptr + 1, (size_t)ptr[0], sizeof(*ptr), mdbx_txl_cmp);
}

static int __must_check_result mdbx_txl_append(MDBX_TXL *ptl, txnid_t id) {
  if (unlikely(MDBX_PNL_SIZE(*ptl) == MDBX_PNL_ALLOCLEN(*ptl))) {
    int rc = mdbx_txl_need(ptl, MDBX_TXL_GRANULATE);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  mdbx_txl_xappend(*ptl, id);
  return MDBX_SUCCESS;
}

static int __must_check_result mdbx_txl_append_list(MDBX_TXL *ptl, MDBX_TXL append) {
  int rc = mdbx_txl_need(ptl, (size_t)MDBX_PNL_SIZE(append));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  memcpy(MDBX_PNL_END(*ptl), MDBX_PNL_BEGIN(append), (size_t)MDBX_PNL_SIZE(append) * sizeof(txnid_t));
  MDBX_PNL_SIZE(*ptl) += MDBX_PNL_SIZE(append);
  return MDBX_SUCCESS;
}

/*----------------------------------------------------------------------------*/

/* Returns the index of the first dirty-page whose pgno
 * member is greater than or equal to id. */
static unsigned __hot mdbx_dpl_search(MDBX_DPL dl, pgno_t id) {
  /* binary search of id in array
   * if found, returns position of id
   * if not found, returns first position greater than id */
  unsigned base = 0;
  unsigned cursor = 1;
  int val = 0;
  unsigned n = dl->length;

#if MDBX_DEBUG
  for (const MDBX_DP *ptr = dl + dl->length; --ptr > dl;) {
    assert(ptr[0].pgno < ptr[1].pgno);
    assert(ptr[0].pgno >= MDBX_NUM_METAS);
  }
#endif

  while (n > 0) {
    unsigned pivot = n >> 1;
    cursor = base + pivot + 1;
    val = mdbx_cmp2int(id, dl[cursor].pgno);

    if (val < 0) {
      n = pivot;
    } else if (val > 0) {
      base = cursor;
      n -= pivot + 1;
    } else {
      return cursor;
    }
  }

  if (val > 0)
    ++cursor;

  return cursor;
}

static int mdbx_dpl_cmp(const void *pa, const void *pb) {
  const MDBX_DP a = *(MDBX_DPL)pa;
  const MDBX_DP b = *(MDBX_DPL)pb;
  return mdbx_cmp2int(a.pgno, b.pgno);
}

static void mdbx_dpl_sort(MDBX_DPL dl) {
  assert(dl->length <= MDBX_DPL_TXNFULL);
  /* LY: temporary */
  qsort(dl + 1, dl->length, sizeof(*dl), mdbx_dpl_cmp);
}

static int __must_check_result mdbx_dpl_insert(MDBX_DPL dl, pgno_t pgno, page_t *page) {
  assert(dl->length <= MDBX_DPL_TXNFULL);
  unsigned x = mdbx_dpl_search(dl, pgno);
  assert((int)x > 0);
  if (unlikely(dl[x].pgno == pgno && x <= dl->length))
    return /* duplicate */ MDBX_PROBLEM;

  if (unlikely(dl->length == MDBX_DPL_TXNFULL))
    return MDBX_TXN_FULL;

  /* insert page */
  for (unsigned i = dl->length += 1; i > x; --i)
    dl[i] = dl[i - 1];

  dl[x].pgno = pgno;
  dl[x].ptr = page;
  return MDBX_SUCCESS;
}

static int __must_check_result mdbx_dpl_append(MDBX_DPL dl, pgno_t pgno, page_t *page) {
  assert(dl->length <= MDBX_DPL_TXNFULL);
#if MDBX_DEBUG
  for (unsigned i = dl->length; i > 0; --i) {
    assert(dl[i].pgno != pgno);
    if (unlikely(dl[i].pgno == pgno))
      return MDBX_PROBLEM;
  }
#endif

  if (unlikely(dl->length == MDBX_DPL_TXNFULL))
    return MDBX_TXN_FULL;

  /* append page */
  const unsigned i = dl->length += 1;
  dl[i].pgno = pgno;
  dl[i].ptr = page;
  return MDBX_SUCCESS;
}
