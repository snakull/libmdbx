/* mdbx_chk.c - memory-mapped database check tool */

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

#ifdef _MSC_VER
#if _MSC_VER > 1800
#pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#pragma warning(disable : 4996) /* The POSIX name is deprecated... */
#endif                          /* _MSC_VER (warnings) */

#include "../bits.h"

typedef struct flagbit {
  int bit;
  char *name;
} flagbit;

flagbit dbflags[] = {{MDBX_DUPSORT, "dupsort"},
                     {MDBX_INTEGERKEY, "integerkey"},
                     {MDBX_REVERSEKEY, "reversekey"},
                     {MDBX_DUPFIXED, "dupfixed"},
                     {MDBX_REVERSEDUP, "reversedup"},
                     {MDBX_INTEGERDUP, "integerdup"},
                     {0, NULL}};

#if defined(_WIN32) || defined(_WIN64)
#include "wingetopt.h"

static volatile BOOL user_break;
static BOOL WINAPI ConsoleBreakHandlerRoutine(DWORD dwCtrlType) {
  (void)dwCtrlType;
  user_break = true;
  return true;
}

#else /* WINDOWS */

static volatile sig_atomic_t user_break;
static void signal_handler(int sig) {
  (void)sig;
  user_break = 1;
}

#endif /* !WINDOWS */

#define EXIT_INTERRUPTED (EXIT_FAILURE + 4)
#define EXIT_FAILURE_SYS (EXIT_FAILURE + 3)
#define EXIT_FAILURE_MDB (EXIT_FAILURE + 2)
#define EXIT_FAILURE_CHECK_MAJOR (EXIT_FAILURE + 1)
#define EXIT_FAILURE_CHECK_MINOR EXIT_FAILURE

typedef struct {
  MDBX_iov_t ident;
  struct {
    uint64_t branch, large_count, large_volume, leaf;
    uint64_t subleaf_dupsort, leaf_dupfixed, subleaf_dupfixed;
    uint64_t total, empty, other;
  } pages;
  uint64_t payload_bytes;
  uint64_t lost_bytes;
} walk_dbi_t;

struct {
  walk_dbi_t dbi[MAX_AAH];
  short *pagemap;
  uint64_t total_payload_bytes;
  uint64_t pgcount;
} walk;

uint64_t total_unused_bytes;
uint32_t regime, open_flags = MDBX_RDONLY | MDBX_EXCLUSIVE;

MDBX_env_t *env;
MDBX_txn_t *txn;
MDBX_db_info_t bk_info;
size_t userdb_count, skipped_subdb;
uint64_t reclaimable_pages, gc_pages, lastpgno, unused_pages;
unsigned verbose, quiet;
const char *only_subdb;

struct problem {
  struct problem *pr_next;
  uint64_t count;
  const char *caption;
};

struct problem *problems_list;
uint64_t total_problems;

static void
#ifdef __GNUC__
    __attribute__((format(printf, 1, 2)))
#endif
    print(const char *msg, ...) {
  if (!quiet) {
    va_list args;

    fflush(stderr);
    va_start(args, msg);
    vfprintf(stdout, msg, args);
    va_end(args);
  }
}

static void
#ifdef __GNUC__
    __attribute__((format(printf, 1, 2)))
#endif
    error(const char *msg, ...) {
  total_problems++;

  if (!quiet) {
    va_list args;

    fflush(stdout);
    fputs(" ! ", stderr);
    va_start(args, msg);
    vfprintf(stderr, msg, args);
    va_end(args);
    fflush(NULL);
  }
}

static void pagemap_cleanup(void) {
  for (walk_dbi_t *dbi = walk.dbi; dbi < walk.dbi + MAX_AAH && dbi->ident.iov_len; ++dbi) {
    free(dbi->ident.iov_base);
    dbi->ident.iov_base = NULL;
    dbi->ident.iov_len = 0;
  }

  free(walk.pagemap);
  walk.pagemap = NULL;
}

static walk_dbi_t *pagemap_lookup_dbi(const MDBX_iov_t ident, bool silent) {
  static walk_dbi_t *last;

  if (last && mdbx_iov_eq(&last->ident, &ident))
    return last;

  walk_dbi_t *dbi = walk.dbi;
  while (dbi->ident.iov_len) {
    if (mdbx_iov_eq(&dbi->ident, &ident))
      return last = dbi;
    if (++dbi == walk.dbi + MAX_AAH) {
      errno = MDBX_DBS_FULL;
      return NULL;
    }
  }

  dbi->ident = ident;
  int err = mdbx_iov_dup(&dbi->ident);
  if (err != MDBX_SUCCESS) {
    errno = err;
    dbi->ident.iov_len = 0;
    return NULL;
  }

  if (verbose > 1 && !silent) {
    print(" - found '%.*s' area\n", (ident.iov_len > 64) ? 64 : (int)ident.iov_len,
          (const char *)ident.iov_base);
    fflush(NULL);
  }

  return last = dbi;
}

static void
#ifdef __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    problem_add(const char *object, uint64_t entry_number, const char *msg, const char *extra, ...) {
  total_problems++;

  if (!quiet) {
    int need_fflush = 0;
    struct problem *p;

    for (p = problems_list; p; p = p->pr_next)
      if (p->caption == msg)
        break;

    if (!p) {
      p = calloc(1, sizeof(*p));
      p->caption = msg;
      p->pr_next = problems_list;
      problems_list = p;
      need_fflush = 1;
    }

    p->count++;
    if (verbose > 1) {
      print("     %s #%" PRIu64 ": %s", object, entry_number, msg);
      if (extra) {
        va_list args;
        printf(" (");
        va_start(args, extra);
        vfprintf(stdout, extra, args);
        va_end(args);
        printf(")");
      }
      printf("\n");
      if (need_fflush)
        fflush(NULL);
    }
  }
}

static struct problem *problems_push(void) {
  struct problem *p = problems_list;
  problems_list = NULL;
  return p;
}

static uint64_t problems_pop(struct problem *list) {
  uint64_t count = 0;

  if (problems_list) {
    int i;

    print(" - problems: ");
    for (i = 0; problems_list; ++i) {
      struct problem *p = problems_list->pr_next;
      count += problems_list->count;
      print("%s%s (%" PRIu64 ")", i ? ", " : "", problems_list->caption, problems_list->count);
      free(problems_list);
      problems_list = p;
    }
    print("\n");
    fflush(NULL);
  }

  problems_list = list;
  return count;
}

static MDBX_error_t pgvisitor(uint64_t pgno, unsigned pgnumber, void *ctx, int deep, const MDBX_iov_t ident,
                              size_t page_size, MDBX_page_type_t pagetype, size_t nentries,
                              size_t payload_bytes, size_t header_bytes, size_t unused_bytes) {
  (void)ctx;
  (void)deep;
  if (pagetype == MDBX_page_void)
    return MDBX_SUCCESS;

  walk_dbi_t fake, *dbi = &fake;
  dbi = pagemap_lookup_dbi(ident, false);
  if (!dbi)
    return MDBX_ENOMEM;

  const uint64_t page_bytes = payload_bytes + header_bytes + unused_bytes;
  walk.pgcount += pgnumber;

  const char *pagetype_caption;
  switch (pagetype) {
  default:
    problem_add("page", pgno, "unknown page-type", "%u", (unsigned)pagetype);
    pagetype_caption = "unknown";
    dbi->pages.other += pgnumber;
    break;
  case MDBX_page_meta:
    pagetype_caption = "meta";
    dbi->pages.other += pgnumber;
    break;
  case MDBX_page_large:
    pagetype_caption = "large";
    dbi->pages.large_volume += pgnumber;
    dbi->pages.large_count += 1;
    break;
  case MDBX_page_branch:
    pagetype_caption = "branch";
    dbi->pages.branch += pgnumber;
    break;
  case MDBX_page_leaf:
    pagetype_caption = "leaf";
    dbi->pages.leaf += pgnumber;
    break;
  case MDBX_page_dupfixed_leaf:
    pagetype_caption = "leaf-dupfixed";
    dbi->pages.leaf_dupfixed += pgnumber;
    break;
  case MDBX_subpage_leaf:
    pagetype_caption = "subleaf-dupsort";
    dbi->pages.subleaf_dupsort += pgnumber;
    break;
  case MDBX_subpage_dupfixed_leaf:
    pagetype_caption = "subleaf-dupfixed";
    dbi->pages.subleaf_dupfixed += pgnumber;
    break;
  }

  if (pgnumber && verbose > 2 && (!only_subdb || mdbx_iov_eq_str(&ident, only_subdb))) {
    if (pgnumber == 1)
      print("     %s-page %" PRIu64, pagetype_caption, pgno);
    else
      print("     %s-span %" PRIu64 "[%u]", pagetype_caption, pgno, pgnumber);
    print(" of %.*s: header %" PRIiPTR ", payload %" PRIiPTR ", unused %" PRIiPTR "\n",
          (ident.iov_len > 64) ? 64 : (int)ident.iov_len, (const char *)ident.iov_base, header_bytes,
          payload_bytes, unused_bytes);
  }

  if (unused_bytes > page_size)
    problem_add("page", pgno, "illegal unused-bytes", "%s-page: %u < %" PRIuPTR " < %u", pagetype_caption, 0,
                unused_bytes, bk_info.bi_pagesize);

  if (header_bytes < sizeof(long) || header_bytes >= bk_info.bi_pagesize - sizeof(long))
    problem_add("page", pgno, "illegal header-length", "%s-page: %" PRIuPTR " < %" PRIuPTR " < %" PRIuPTR,
                pagetype_caption, sizeof(long), header_bytes, bk_info.bi_pagesize - sizeof(long));

  if (payload_bytes < 1) {
    if (nentries > 1) {
      problem_add("page", pgno, "zero size-of-entry",
                  "%s-page: payload %" PRIuPTR " bytes, %" PRIuPTR " entries", pagetype_caption, payload_bytes,
                  nentries);
      /* if (header_bytes + unused_bytes < page_size) {
        // LY: hush a misuse error
        page_bytes = page_size;
      } */
    } else {
      problem_add("page", pgno, "empty", "%s-page: payload %" PRIuPTR " bytes, %" PRIuPTR " entries",
                  pagetype_caption, payload_bytes, nentries);
      dbi->pages.empty += 1;
    }
  }

  if (pgnumber) {
    if (page_bytes != page_size) {
      problem_add("page", pgno, "misused",
                  "%s-page: %" PRIu64 " != %" PRIu64 " (%" PRIuPTR "h + %" PRIuPTR "p + %" PRIuPTR "u)",
                  pagetype_caption, page_size, page_bytes, header_bytes, payload_bytes, unused_bytes);
      if (page_size > page_bytes)
        dbi->lost_bytes += page_size - page_bytes;
    } else {
      dbi->payload_bytes += payload_bytes + header_bytes;
      walk.total_payload_bytes += payload_bytes + header_bytes;
    }

    do {
      if (pgno >= lastpgno)
        problem_add("page", pgno, "wrong page-no", "%s-page: %" PRIu64 " > %" PRIu64, pagetype_caption, pgno,
                    lastpgno);
      else if (walk.pagemap[pgno]) {
        const MDBX_iov_t usedby = walk.dbi[walk.pagemap[pgno]].ident;
        problem_add("page", pgno, "already used", "%s-page: by %.*s", pagetype_caption,
                    (usedby.iov_len > 64) ? 64 : (int)usedby.iov_len, (const char *)usedby.iov_base);
      } else {
        walk.pagemap[pgno] = (short)(dbi - walk.dbi + 1);
        dbi->pages.total += 1;
      }
      ++pgno;
    } while (--pgnumber);
  }

  return user_break ? MDBX_EINTR : MDBX_SUCCESS;
}

typedef MDBX_error_t(visitor)(const uint64_t record_number, const MDBX_iov_t key, const MDBX_iov_t *data);
static MDBX_error_t process_db(MDBX_aah_t aah, const MDBX_iov_t name, visitor *handler, bool silent);

static MDBX_error_t handle_userdb(const uint64_t record_number, const MDBX_iov_t key, const MDBX_iov_t *data) {
  (void)record_number;
  (void)key;
  (void)data;
  return MDBX_SUCCESS;
}

static MDBX_error_t handle_gc(const uint64_t record_number, const MDBX_iov_t key, const MDBX_iov_t *data) {
  char *bad = "";
  pgno_t *iptr = data->iov_base;
  txnid_t txnid = *(txnid_t *)key.iov_base;

  if (key.iov_len != sizeof(txnid_t))
    problem_add("entry", record_number, "wrong txn-id size", "key-size %" PRIiPTR, key.iov_len);
  else if (txnid < 1 || txnid > bk_info.bi_recent_txnid)
    problem_add("entry", record_number, "wrong txn-id", "%" PRIaTXN, txnid);
  else {
    if (data->iov_len < sizeof(pgno_t) || data->iov_len % sizeof(pgno_t))
      problem_add("entry", txnid, "wrong idl size", "%" PRIuPTR, data->iov_len);
    size_t number = (data->iov_len >= sizeof(pgno_t)) ? *iptr++ : 0;

    if (number < 1 || number > MDBX_PNL_MAX)
      problem_add("entry", txnid, "wrong idl length", "%" PRIuPTR, number);
    else if ((number + 1) * sizeof(pgno_t) > data->iov_len) {
      problem_add("entry", txnid, "trimmed idl", "%" PRIuSIZE " > %" PRIuSIZE " (corruption)",
                  (number + 1) * sizeof(pgno_t), data->iov_len);
      number = data->iov_len / sizeof(pgno_t) - 1;
    } else if (data->iov_len - (number + 1) * sizeof(pgno_t) >=
               /* LY: allow gap upto one page. it is ok
                * and better than shink-and-retry inside mdbx_update_gc() */
               bk_info.bi_pagesize)
      problem_add("entry", txnid, "extra idl space", "%" PRIuSIZE " < %" PRIuSIZE " (minor, not a trouble)",
                  (number + 1) * sizeof(pgno_t), data->iov_len);

    gc_pages += number;
    if (bk_info.bi_latter_reader_txnid > txnid)
      reclaimable_pages += number;

    pgno_t prev = MDBX_PNL_ASCENDING ? MIN_PAGENO : (pgno_t)bk_info.bi_dxb_last_pgno + 1;
    pgno_t span = 1;
    for (unsigned i = 0; i < number; ++i) {
      const pgno_t pgno = iptr[i];
      if (pgno < MDBX_NUM_METAS || pgno > bk_info.bi_dxb_last_pgno)
        problem_add("entry", txnid, "wrong idl entry", "%u < %" PRIaPGNO " < %" PRIu64, MDBX_NUM_METAS, pgno,
                    bk_info.bi_dxb_last_pgno);
      else if (MDBX_PNL_DISORDERED(prev, pgno)) {
        bad = " [bad sequence]";
        problem_add("entry", txnid, "bad sequence", "%" PRIaPGNO " <> %" PRIaPGNO, prev, pgno);
      }

      if (walk.pagemap && walk.pagemap[pgno]) {
        if (walk.pagemap[pgno] > 0) {
          const MDBX_iov_t ident = walk.dbi[walk.pagemap[pgno] - 1].ident;
          problem_add("page", pgno, "already used", "by %.*s", (ident.iov_len > 64) ? 64 : (int)ident.iov_len,
                      (const char *)ident.iov_base);
        } else
          problem_add("page", pgno, "already listed in GC", nullptr);
        walk.pagemap[pgno] = -1;
      }

      prev = pgno;
      while (i + span < number &&
             iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pgno, span) : pgno_sub(pgno, span)))
        ++span;
    }

    if (verbose > 3 && !only_subdb) {
      print("     transaction %" PRIaTXN ", %" PRIuPTR " pages, maxspan %" PRIaPGNO "%s\n", txnid, number,
            span, bad);
      if (verbose > 4) {
        for (unsigned i = 0; i < number; i += span) {
          const pgno_t pgno = iptr[i];
          for (span = 1; i + span < number &&
                         iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pgno, span) : pgno_sub(pgno, span));
               ++span)
            ;
          if (span > 1) {
            print("    %9" PRIaPGNO "[%" PRIaPGNO "]\n", pgno, span);
          } else
            print("    %9" PRIaPGNO "\n", pgno);
        }
      }
    }
  }

  return MDBX_SUCCESS;
}

static MDBX_error_t handle_maindb(const uint64_t record_number, const MDBX_iov_t key, const MDBX_iov_t *data) {
  for (size_t i = 0; i < key.iov_len; ++i) {
    if (((const char *)key.iov_base)[i] < ' ')
      return handle_userdb(record_number, key, data);
  }

  userdb_count++;
  MDBX_error_t err = process_db(MDBX_INVALID_AAH, key, handle_userdb, false);
  if (err != MDBX_INCOMPATIBLE)
    return err;

  return handle_userdb(record_number, key, data);
}

static MDBX_error_t process_db(MDBX_aah_t aah, const MDBX_iov_t ident, visitor *handler, bool silent) {
  MDBX_aa_info_t aa_info;
  MDBX_iov_t key, data;
  MDBX_iov_t prev_key, prev_data;
  int i;
  MDBX_error_t err;
  struct problem *saved_list;
  uint64_t problems_count;

  uint64_t record_count = 0, dups = 0;
  uint64_t key_bytes = 0, data_bytes = 0;

  if (aah == MDBX_INVALID_AAH) {
    MDBX_aah_result_t aah_result = mdbx_aa_open(txn, ident, 0, NULL, NULL);
    if (aah_result.err) {
      err = aah_result.err;
      if (err != MDBX_INCOMPATIBLE) /* LY: mainDB's record is not a user's AA. */ {
        error("mdbx_aa_open '%.*s' failed, error %d %s\n", (ident.iov_len > 64) ? 64 : (int)ident.iov_len,
              (const char *)ident.iov_base, err, mdbx_strerror(err));
      }
      return err;
    }
    aah = aah_result.aah;
  }

  if (aah >= CORE_AAH && only_subdb && !mdbx_iov_eq_str(&ident, only_subdb)) {
    if (verbose) {
      print("Skip processing '%.*s'...\n", (ident.iov_len > 64) ? 64 : (int)ident.iov_len,
            (const char *)ident.iov_base);
      fflush(NULL);
    }
    skipped_subdb++;
    return MDBX_SUCCESS;
  }

  if (!silent && verbose) {
    print("Processing '%.*s'...\n", (ident.iov_len > 64) ? 64 : (int)ident.iov_len,
          (const char *)ident.iov_base);
    fflush(NULL);
  }

  err = mdbx_aa_info(txn, aah, &aa_info, sizeof(aa_info));
  if (err) {
    error("mdbx_aa_stat failed, error %d %s\n", err, mdbx_strerror(err));
    return err;
  }

  if (!silent && verbose) {
    print(" - aah-id %" PRIuFAST32 ", flags:", aah);
    if (!aa_info.ai_flags)
      print(" none");
    else {
      for (i = 0; dbflags[i].bit; i++)
        if (aa_info.ai_flags & dbflags[i].bit)
          print(" %s", dbflags[i].name);
    }
    print(" (0x%02X)\n", aa_info.ai_flags);
    if (verbose > 1) {
      print(" - sequence %" PRIu64 ", entries %" PRIu64 "\n", aa_info.ai_sequence, aa_info.ai_entries);
      print(" - b-tree depth %u, pages: branch %" PRIu64 ", leaf %" PRIu64 ", overflow %" PRIu64 "\n",
            aa_info.ai_tree_depth, aa_info.ai_branch_pages, aa_info.ai_leaf_pages, aa_info.ai_overflow_pages);
    }
  }

  walk_dbi_t *dbi = /*(aah < CORE_AAH) ? &walk.dbi[aah] :*/ pagemap_lookup_dbi(ident, true);
  if (!dbi) {
    error("too many DBIs or out of memory\n");
    return errno;
  }
  const uint64_t subtotal_pages = aa_info.ai_branch_pages + aa_info.ai_leaf_pages + aa_info.ai_overflow_pages;
  if (subtotal_pages != dbi->pages.total)
    error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n", "subtotal", subtotal_pages,
          dbi->pages.total);
  if (aa_info.ai_branch_pages != dbi->pages.branch)
    error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n", "branch", aa_info.ai_branch_pages,
          dbi->pages.branch);
  const uint64_t allleaf_pages = dbi->pages.leaf + dbi->pages.leaf_dupfixed;
  if (aa_info.ai_leaf_pages != allleaf_pages)
    error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n", "all-leaf", aa_info.ai_leaf_pages,
          allleaf_pages);
  if (aa_info.ai_overflow_pages != dbi->pages.large_volume)
    error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n", "large/overlow",
          aa_info.ai_overflow_pages, dbi->pages.large_volume);

  MDBX_cursor_result_t xr = mdbx_cursor_open(txn, aah);
  if (xr.err) {
    error("mdbx_cursor_open failed, error %d %s\n", xr.err, mdbx_strerror(xr.err));
    return xr.err;
  }

  saved_list = problems_push();
  prev_key.iov_base = NULL;
  prev_data.iov_len = 0;
  err = mdbx_cursor_get(xr.cursor, &key, &data, MDBX_FIRST);
  while (err == MDBX_SUCCESS) {
    if (user_break) {
      print(" - interrupted by signal\n");
      fflush(NULL);
      err = MDBX_EINTR;
      goto bailout;
    }

    if (key.iov_len > bk_info.bi_maxkeysize) {
      problem_add("entry", record_count, "key length exceeds max-key-size", "%" PRIuPTR " > %u", key.iov_len,
                  bk_info.bi_maxkeysize);
    } else if ((aa_info.ai_flags & MDBX_INTEGERKEY) && key.iov_len != sizeof(uint64_t) &&
               key.iov_len != sizeof(uint32_t)) {
      problem_add("entry", record_count, "wrong key length", "%" PRIuPTR " != 4or8", key.iov_len);
    }

    if ((aa_info.ai_flags & MDBX_INTEGERDUP) && data.iov_len != sizeof(uint64_t) &&
        data.iov_len != sizeof(uint32_t)) {
      problem_add("entry", record_count, "wrong data length", "%" PRIuPTR " != 4or8", data.iov_len);
    }

    if (prev_key.iov_base) {
      if ((aa_info.ai_flags & MDBX_DUPFIXED) && prev_data.iov_len != data.iov_len) {
        problem_add("entry", record_count, "different data length", "%" PRIuPTR " != %" PRIuPTR,
                    prev_data.iov_len, data.iov_len);
      }

      int cmp = mdbx_cmp(txn, aah, &prev_key, &key);
      if (cmp > 0) {
        problem_add("entry", record_count, "broken ordering of entries", NULL);
      } else if (cmp == 0) {
        ++dups;
        if (!(aa_info.ai_flags & MDBX_DUPSORT))
          problem_add("entry", record_count, "duplicated entries", NULL);
        else if (aa_info.ai_flags & MDBX_INTEGERDUP) {
          cmp = mdbx_dcmp(txn, aah, &prev_data, &data);
          if (cmp > 0)
            problem_add("entry", record_count, "broken ordering of multi-values", NULL);
        }
      }
    } else if (verbose) {
      if (aa_info.ai_flags & MDBX_INTEGERKEY)
        print(" - fixed key-size %" PRIuPTR "\n", key.iov_len);
      if (aa_info.ai_flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED))
        print(" - fixed data-size %" PRIuPTR "\n", data.iov_len);
    }

    if (handler) {
      err = handler(record_count, key, &data);
      if (err)
        goto bailout;
    }

    record_count++;
    key_bytes += key.iov_len;
    data_bytes += data.iov_len;

    prev_key = key;
    prev_data = data;
    err = mdbx_cursor_get(xr.cursor, &key, &data, MDBX_NEXT);
  }
  if (err != MDBX_NOTFOUND)
    error("mdbx_cursor_get failed, error %d %s\n", err, mdbx_strerror(err));
  else
    err = 0;

  if (record_count != aa_info.ai_entries)
    problem_add("entry", record_count, "differentent number of entries", "%" PRIuPTR " != %" PRIuPTR,
                record_count, aa_info.ai_entries);
bailout:
  problems_count = problems_pop(saved_list);
  if (!silent && verbose) {
    print(" - summary: %" PRIu64 " records, %" PRIu64 " dups, %" PRIu64 " key's bytes, %" PRIu64 " data's "
          "bytes, %" PRIu64 " problems\n",
          record_count, dups, key_bytes, data_bytes, problems_count);
    fflush(NULL);
  }

  mdbx_cursor_close(xr.cursor);
  return err || problems_count;
}

static void usage(char *prog) {
  fprintf(stderr,
          "usage: %s dbpath [-V] [-v] [-n] [-q] [-w] [-c] [-d] [-s subdb]\n"
          "  -V\t\tshow version\n"
          "  -v\t\tmore verbose, could be used multiple times\n"
          "  -n\t\tNOSUBDIR mode for open\n"
          "  -q\t\tbe quiet\n"
          "  -w\t\tlock databook for writing while checking\n"
          "  -d\t\tdisable page-by-page traversal of b-tree\n"
          "  -s subdb\tprocess a specific subdatabase only\n"
          "  -c\t\tforce cooperative mode (don't try exclusive)\n",
          prog);
  exit(EXIT_INTERRUPTED);
}

const char *meta_synctype(uint64_t sign) {
  switch (sign) {
  case MDBX_DATASIGN_NONE:
    return "no-sync/legacy";
  case MDBX_DATASIGN_WEAK:
    return "weak";
  default:
    return "steady";
  }
}

static inline bool meta_ot(txnid_t txn_a, uint64_t sign_a, txnid_t txn_b, uint64_t sign_b,
                           const bool roolback2steady) {
  if (txn_a == txn_b)
    return SIGN_IS_STEADY(sign_b);

  if (roolback2steady && SIGN_IS_STEADY(sign_a) != SIGN_IS_STEADY(sign_b))
    return SIGN_IS_STEADY(sign_b);

  return txn_a < txn_b;
}

static inline bool meta_eq(txnid_t txn_a, uint64_t sign_a, txnid_t txn_b, uint64_t sign_b) {
  if (txn_a != txn_b)
    return false;

  if (SIGN_IS_STEADY(sign_a) != SIGN_IS_STEADY(sign_b))
    return false;

  return true;
}

static inline int meta_recent(const bool roolback2steady) {

  if (meta_ot(bk_info.bi_meta[0].txnid, bk_info.bi_meta[0].sign_checksum, bk_info.bi_meta[1].txnid,
              bk_info.bi_meta[1].sign_checksum, roolback2steady))
    return meta_ot(bk_info.bi_meta[2].txnid, bk_info.bi_meta[2].sign_checksum, bk_info.bi_meta[1].txnid,
                   bk_info.bi_meta[1].sign_checksum, roolback2steady)
               ? 1
               : 2;

  return meta_ot(bk_info.bi_meta[0].txnid, bk_info.bi_meta[0].sign_checksum, bk_info.bi_meta[2].txnid,
                 bk_info.bi_meta[2].sign_checksum, roolback2steady)
             ? 2
             : 0;
}

static inline int meta_tail(int head) {

  if (head == 0)
    return meta_ot(bk_info.bi_meta[1].txnid, bk_info.bi_meta[1].sign_checksum, bk_info.bi_meta[2].txnid,
                   bk_info.bi_meta[2].sign_checksum, true)
               ? 1
               : 2;
  if (head == 1)
    return meta_ot(bk_info.bi_meta[0].txnid, bk_info.bi_meta[0].sign_checksum, bk_info.bi_meta[2].txnid,
                   bk_info.bi_meta[2].sign_checksum, true)
               ? 0
               : 2;
  if (head == 2)
    return meta_ot(bk_info.bi_meta[0].txnid, bk_info.bi_meta[0].sign_checksum, bk_info.bi_meta[1].txnid,
                   bk_info.bi_meta[1].sign_checksum, true)
               ? 0
               : 1;
  assert(false);
  return -1;
}

static int meta_steady(void) { return meta_recent(true); }

static int meta_head(void) { return meta_recent(false); }

void verbose_meta(int num, txnid_t txnid, uint64_t sign) {
  print(" - meta-%d: %s %" PRIu64, num, meta_synctype(sign), txnid);
  bool stay = true;

  const int steady = meta_steady();
  const int head = meta_head();
  if (num == steady && num == head) {
    print(", head");
    stay = false;
  } else if (num == steady) {
    print(", head-steady");
    stay = false;
  } else if (num == head) {
    print(", head-weak");
    stay = false;
  }
  if (num == meta_tail(head)) {
    print(", tail");
    stay = false;
  }
  if (stay)
    print(", stay");

  if (txnid > bk_info.bi_recent_txnid && ((regime & MDBX_EXCLUSIVE) && (open_flags & MDBX_RDONLY) == 0))
    print(", rolled-back %" PRIu64 " (%" PRIu64 " >>> %" PRIu64 ")", txnid - bk_info.bi_recent_txnid, txnid,
          bk_info.bi_recent_txnid);
  print("\n");
}

static int check_meta_head(bool steady) {
  switch (meta_recent(steady)) {
  default:
    assert(false);
    error("unexpected internal error (%s)\n", steady ? "meta_steady_head" : "meta_weak_head");
  /* fallthrough */
  case 0:
    if (bk_info.bi_meta[0].txnid != bk_info.bi_recent_txnid) {
      print(" - meta-%d txn-id mismatch recent-txn-id (%" PRIi64 " != %" PRIi64 ")\n", 0,
            bk_info.bi_meta[0].txnid, bk_info.bi_recent_txnid);
      return 1;
    }
    break;
  case 1:
    if (bk_info.bi_meta[1].txnid != bk_info.bi_recent_txnid) {
      print(" - meta-%d txn-id mismatch recent-txn-id (%" PRIi64 " != %" PRIi64 ")\n", 1,
            bk_info.bi_meta[1].txnid, bk_info.bi_recent_txnid);
      return 1;
    }
    break;
  case 2:
    if (bk_info.bi_meta[2].txnid != bk_info.bi_recent_txnid) {
      print(" - meta-%d txn-id mismatch recent-txn-id (%" PRIi64 " != %" PRIi64 ")\n", 2,
            bk_info.bi_meta[2].txnid, bk_info.bi_recent_txnid);
      return 1;
    }
  }
  return 0;
}

static void print_size(const char *prefix, const uint64_t value, const char *suffix) {
  const char sf[] = "KMGTPEZY"; /* LY: Kilo, Mega, Giga, Tera, Peta, Exa, Zetta, Yotta! */
  double k = 1024.0;
  size_t i;
  for (i = 0; sf[i + 1] && value / k > 1000.0; ++i)
    k *= 1024;
  print("%s%" PRIu64 " (%.2f %cb)%s", prefix, value, value / k, sf[i], suffix);
}

int main(int argc, char *argv[]) {
  MDBX_error_t err;
  char *prog = argv[0];
  char *envname;
  int problems_maindb = 0, problems_gc = 0, problems_meta = 0;
  int dont_traversal = 0;
  bool locked = false;

  double elapsed;
#if defined(_WIN32) || defined(_WIN64)
  uint64_t timestamp_start, timestamp_finish;
  timestamp_start = GetTickCount64();
#else
  struct timespec timestamp_start, timestamp_finish;
  if (clock_gettime(CLOCK_MONOTONIC, &timestamp_start)) {
    err = errno;
    error("clock_gettime failed, error %d %s\n", err, mdbx_strerror(err));
    return EXIT_FAILURE_SYS;
  }
#endif

  atexit(pagemap_cleanup);

  if (argc < 2) {
    usage(prog);
  }

  for (int i; (i = getopt(argc, argv, "Vvqnwcds:")) != EOF;) {
    switch (i) {
    case 'V':
      printf("%s (%s, build %s)\n", mdbx_version.git.describe, mdbx_version.git.datetime, mdbx_build.datetime);
      exit(EXIT_SUCCESS);
      break;
    case 'v':
      verbose++;
      break;
    case 'q':
      quiet = 1;
      break;
    case 'n':
      /* silently ignore for compatibility (MDB_NOSUBDIR option) */
      break;
    case 'w':
      open_flags &= ~MDBX_RDONLY;
      break;
    case 'c':
      open_flags &= ~MDBX_EXCLUSIVE;
      break;
    case 'd':
      dont_traversal = 1;
      break;
    case 's':
      if (only_subdb && strcmp(only_subdb, optarg))
        usage(prog);
      only_subdb = optarg;
      break;
    default:
      usage(prog);
    }
  }

  if (optind != argc - 1)
    usage(prog);

#if defined(_WIN32) || defined(_WIN64)
  SetConsoleCtrlHandler(ConsoleBreakHandlerRoutine, true);
#else
#ifdef SIGPIPE
  signal(SIGPIPE, signal_handler);
#endif
#ifdef SIGHUP
  signal(SIGHUP, signal_handler);
#endif
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
#endif /* !WINDOWS */

  envname = argv[optind];
  print("Running mdbx_chk for '%s' in read-%s mode...\n", envname,
        (open_flags & MDBX_RDONLY) ? "only" : "write");
  fflush(NULL);

#if MDBX_DEBUG == 0
  mdbx_set_loglevel(MDBX_LOG_ALL, MDBX_LOGLEVEL_ERROR);
#elif MDBX_DEBUG == 1
  mdbx_set_loglevel(MDBX_LOG_ALL, MDBX_LOGLEVEL_NOTICE);
#else
  mdbx_set_loglevel(MDBX_LOG_ALL, MDBX_LOGLEVEL_EXTRA);
#endif

  err = mdbx_init(&env);
  if (err) {
    error("mdbx_init failed, error %d %s\n", err, mdbx_strerror(err));
    return err < 0 ? EXIT_FAILURE_MDB : EXIT_FAILURE_SYS;
  }

  err = mdbx_set_maxhandles(env, MAX_AAH);
  if (err) {
    error("mdbx_set_maxhandles failed, error %d %s\n", err, mdbx_strerror(err));
    goto bailout;
  }

  err = mdbx_is_directory(envname);
  if (MDBX_IS_ERROR(err)) {
    error("mdbx_is_directory failed, error %d %s\n", err, mdbx_strerror(err));
    goto bailout;
  }

  err = mdbx_open_ex(env, NULL /* required address */, envname /* dxb pathname */,
                     (err == MDBX_SUCCESS) ? NULL : "." /* lck pathname */, NULL /* sld pathname */,
                     open_flags /* regime flags */, 0 /* regime checkmask */, &regime /* regime present */,
                     0664);
  if (err) {
    error("mdbx_open failed, error %d %s\n", err, mdbx_strerror(err));
    if (err == MDBX_WANNA_RECOVERY && (open_flags & MDBX_RDONLY))
      print("Please run %s in the read-write mode (with '-w' option).\n", prog);
    goto bailout;
  }
  if ((open_flags & MDBX_RDONLY) == 0 && (regime & MDBX_EXCLUSIVE) == 0) {
    err = mdbx_lck_writer_lock(env, 0);
    if (err != MDBX_SUCCESS) {
      error("mdbx_lck_writer_lock failed, error %d %s\n", err, mdbx_strerror(err));
      goto bailout;
    }
    locked = true;
  }

  if (verbose)
    print(" - %s mode\n", (regime & MDBX_EXCLUSIVE) ? "monopolistic" : "cooperative");

  const MDBX_txn_result_t tr = mdbx_begin(env, NULL, MDBX_RDONLY);
  txn = tr.txn;
  err = tr.err;
  if (err) {
    error("mdbx_begin(read-only) failed, error %d %s\n", err, mdbx_strerror(err));
    goto bailout;
  }

  err = mdbx_info(env, &bk_info, sizeof(bk_info));
  if (err) {
    error("mdbx_info failed, error %d %s\n", err, mdbx_strerror(err));
    goto bailout;
  }

  lastpgno = bk_info.bi_dxb_last_pgno + 1;
  errno = 0;

  if (verbose) {
    print(" - pagesize %" PRIu32 " (%" PRIu32 " system), max keysize %" PRIu32 ", max readers %" PRIu32 "\n",
          bk_info.bi_pagesize, bk_info.bi_sys_pagesize, bk_info.bi_maxkeysize, bk_info.bi_readers_max);
    print_size(" - mapsize ", bk_info.bi_mapsize, "\n");
    if (bk_info.bi_dxb_geo.lower == bk_info.bi_dxb_geo.upper)
      print_size(" - fixed datafile: ", bk_info.bi_dxb_geo.current, "");
    else {
      print_size(" - dynamic datafile: ", bk_info.bi_dxb_geo.lower, "");
      print_size(" .. ", bk_info.bi_dxb_geo.upper, ", ");
      print_size("+", bk_info.bi_dxb_geo.grow, ", ");
      print_size("-", bk_info.bi_dxb_geo.shrink, "\n");
      print_size(" - current datafile: ", bk_info.bi_dxb_geo.current, "");
    }
    printf(", %" PRIu64 " pages\n", bk_info.bi_dxb_geo.current / bk_info.bi_pagesize);
    print(" - transactions: recent %" PRIu64 ", latter reader %" PRIu64 ", lag %" PRIi64 "\n",
          bk_info.bi_recent_txnid, bk_info.bi_latter_reader_txnid,
          bk_info.bi_recent_txnid - bk_info.bi_latter_reader_txnid);

    verbose_meta(0, bk_info.bi_meta[0].txnid, bk_info.bi_meta[0].sign_checksum);
    verbose_meta(1, bk_info.bi_meta[1].txnid, bk_info.bi_meta[1].sign_checksum);
    verbose_meta(2, bk_info.bi_meta[2].txnid, bk_info.bi_meta[2].sign_checksum);
  }

  if (verbose)
    print(" - performs check for meta-pages clashes\n");
  if (meta_eq(bk_info.bi_meta[0].txnid, bk_info.bi_meta[0].sign_checksum, bk_info.bi_meta[1].txnid,
              bk_info.bi_meta[1].sign_checksum)) {
    print(" - meta-%d and meta-%d are clashed\n", 0, 1);
    ++problems_meta;
  }
  if (meta_eq(bk_info.bi_meta[1].txnid, bk_info.bi_meta[1].sign_checksum, bk_info.bi_meta[2].txnid,
              bk_info.bi_meta[2].sign_checksum)) {
    print(" - meta-%d and meta-%d are clashed\n", 1, 2);
    ++problems_meta;
  }
  if (meta_eq(bk_info.bi_meta[2].txnid, bk_info.bi_meta[2].sign_checksum, bk_info.bi_meta[0].txnid,
              bk_info.bi_meta[0].sign_checksum)) {
    print(" - meta-%d and meta-%d are clashed\n", 2, 0);
    ++problems_meta;
  }

  if (regime & MDBX_EXCLUSIVE) {
    if (verbose)
      print(" - performs full check recent-txn-id with meta-pages\n");
    problems_meta += check_meta_head(true);
  } else if (locked) {
    if (verbose)
      print(" - performs lite check recent-txn-id with meta-pages (not a "
            "monopolistic mode)\n");
    problems_meta += check_meta_head(false);
  } else if (verbose) {
    print(" - skip check recent-txn-id with meta-pages (monopolistic or "
          "read-write mode only)\n");
  }

  if (!dont_traversal) {
    struct problem *saved_list;
    uint64_t traversal_problems;
    uint64_t empty_pages, lost_bytes;

    print("Traversal b-tree by txn#%" PRIaTXN "...\n", txn->mt_txnid);
    fflush(NULL);
    walk.pagemap = calloc((size_t)lastpgno, sizeof(*walk.pagemap));
    if (!walk.pagemap) {
      err = errno ? errno : MDBX_ENOMEM;
      error("calloc failed, error %d %s\n", err, mdbx_strerror(err));
      goto bailout;
    }

    saved_list = problems_push();
    err = mdbx_walk(txn, pgvisitor, NULL);
    traversal_problems = problems_pop(saved_list);

    if (err) {
      if (err == MDBX_EINTR && user_break) {
        print(" - interrupted by signal\n");
        fflush(NULL);
      } else {
        error("mdbx_bk_pgwalk failed, error %d %s\n", err, mdbx_strerror(err));
      }
      goto bailout;
    }

    for (uint64_t n = 0; n < lastpgno; ++n)
      if (!walk.pagemap[n])
        unused_pages += 1;

    empty_pages = lost_bytes = 0;
    for (walk_dbi_t *dbi = walk.dbi; dbi < walk.dbi + MAX_AAH && dbi->ident.iov_len; ++dbi) {
      empty_pages += dbi->pages.empty;
      lost_bytes += dbi->lost_bytes;
    }

    if (verbose) {
      const uint64_t total_page_bytes = walk.pgcount * bk_info.bi_pagesize;
      print(" - pages: total %" PRIu64 ", unused %" PRIu64 "\n", walk.pgcount, unused_pages);

      if (verbose > 1) {
        for (walk_dbi_t *dbi = walk.dbi; dbi < walk.dbi + MAX_AAH && dbi->ident.iov_len; ++dbi) {
          print("     %.*s: subtotal %" PRIu64, (dbi->ident.iov_len > 64) ? 64 : (int)dbi->ident.iov_len,
                (const char *)dbi->ident.iov_base, dbi->pages.total);
          if (dbi->pages.other && dbi->pages.other != dbi->pages.total)
            print(", other %" PRIu64, dbi->pages.other);
          if (dbi->pages.branch)
            print(", branch %" PRIu64, dbi->pages.branch);
          if (dbi->pages.large_count)
            print(", large %" PRIu64, dbi->pages.large_count);
          const uint64_t all_leaf = dbi->pages.leaf + dbi->pages.leaf_dupfixed;
          if (all_leaf) {
            print(", leaf %" PRIu64, all_leaf);
            if (verbose > 2)
              print(" (usual %" PRIu64 ", sub-dupsort %" PRIu64 ", dupfixed %" PRIu64 ", sub-dupfixed %" PRIu64
                    ")",
                    dbi->pages.leaf, dbi->pages.subleaf_dupsort, dbi->pages.leaf_dupfixed,
                    dbi->pages.subleaf_dupfixed);
          }
          print("\n");
        }
      }

      if (verbose > 1) {
        print(" - usage: total %" PRIu64 " bytes, payload %" PRIu64 " (%.1f%%), unused %" PRIu64 " (%.1f%%)\n",
              total_page_bytes, walk.total_payload_bytes, walk.total_payload_bytes * 100.0 / total_page_bytes,
              total_page_bytes - walk.total_payload_bytes,
              (total_page_bytes - walk.total_payload_bytes) * 100.0 / total_page_bytes);
        if (verbose > 2) {
          for (walk_dbi_t *dbi = walk.dbi; dbi < walk.dbi + MAX_AAH && dbi->ident.iov_len; ++dbi) {
            const uint64_t dbi_bytes = dbi->pages.total * bk_info.bi_pagesize;
            print("     %.*s: subtotal %" PRIu64 " bytes (%.1f%%),"
                  " payload %" PRIu64 " (%.1f%%), unused %" PRIu64 " (%.1f%%)",
                  (dbi->ident.iov_len > 64) ? 64 : (int)dbi->ident.iov_len, (const char *)dbi->ident.iov_base,
                  dbi_bytes, dbi_bytes * 100.0 / total_page_bytes, dbi->payload_bytes,
                  dbi->payload_bytes * 100.0 / dbi_bytes, dbi_bytes - dbi->payload_bytes,
                  (dbi_bytes - dbi->payload_bytes) * 100.0 / dbi_bytes);
            if (dbi->pages.empty)
              print(", %" PRIu64 " empty pages", dbi->pages.empty);
            if (dbi->lost_bytes)
              print(", %" PRIu64 " bytes lost", dbi->lost_bytes);
            print("\n");
          }
        }
      }

      print(" - summary: average fill %.1f%%", walk.total_payload_bytes * 100.0 / total_page_bytes);
      if (empty_pages)
        print(", %" PRIuPTR " empty pages", empty_pages);
      if (lost_bytes)
        print(", %" PRIuPTR " bytes lost", lost_bytes);
      print(", %" PRIuPTR " problems\n", traversal_problems);
    }
  } else if (verbose) {
    print("Skipping b-tree walk...\n");
    fflush(NULL);
  }

  if (!verbose)
    print("Iterating Associative Arrays...\n");
  problems_maindb = process_db(MDBX_MAIN_AAH, mdbx_str2iov("@MAIN"), NULL, false);
  problems_gc = process_db(MDBX_GACO_AAH, mdbx_str2iov("@GC"), handle_gc, false);

  if (verbose) {
    uint64_t value = bk_info.bi_mapsize / bk_info.bi_pagesize;
    double percent = value / 100.0;
    print("Pages: %" PRIu64 " total", value);
    value = bk_info.bi_dxb_geo.current / bk_info.bi_pagesize;
    print(", backed %" PRIu64 " (%.1f%%)", value, value / percent);
    print(", allocated %" PRIu64 " (%.1f%%)", lastpgno, lastpgno / percent);

    if (verbose > 1) {
      value = bk_info.bi_mapsize / bk_info.bi_pagesize - lastpgno;
      print(", remained %" PRIu64 " (%.1f%%)", value, value / percent);

      value = lastpgno - gc_pages;
      print(", used %" PRIu64 " (%.1f%%)", value, value / percent);

      print(", gc %" PRIu64 " (%.1f%%)", gc_pages, gc_pages / percent);

      value = gc_pages - reclaimable_pages;
      print(", detained %" PRIu64 " (%.1f%%)", value, value / percent);

      print(", reclaimable %" PRIu64 " (%.1f%%)", reclaimable_pages, reclaimable_pages / percent);
    }

    value = bk_info.bi_mapsize / bk_info.bi_pagesize - lastpgno + reclaimable_pages;
    print(", available %" PRIu64 " (%.1f%%)\n", value, value / percent);
  }

  if (problems_maindb == 0 && problems_gc == 0) {
    if (!dont_traversal && ((regime & MDBX_EXCLUSIVE) || (open_flags & MDBX_RDONLY) == 0)) {
      if (walk.pgcount != lastpgno - gc_pages) {
        error("used pages mismatch (%" PRIu64 " != %" PRIu64 ")\n", walk.pgcount, lastpgno - gc_pages);
      }
      if (unused_pages != gc_pages) {
        error("gc pages mismatch (%" PRIu64 " != %" PRIu64 ")\n", unused_pages, gc_pages);
      }
    } else if (verbose) {
      print(" - skip check used and gc pages (btree-traversal with "
            "monopolistic or read-write mode only)\n");
    }

    if (!process_db(MDBX_MAIN_AAH, mdbx_str2iov("@MAIN"), handle_maindb, true)) {
      if (!userdb_count && verbose)
        print(" - does not contain multiple databases\n");
    }
  }

bailout:
  if (txn)
    mdbx_abort(txn);
  if (locked)
    mdbx_lck_writer_unlock(env);
  if (env)
    mdbx_shutdown(env);
  fflush(NULL);
  if (err) {
    if (err < 0)
      return (user_break) ? EXIT_INTERRUPTED : EXIT_FAILURE_SYS;
    return EXIT_FAILURE_MDB;
  }

#if defined(_WIN32) || defined(_WIN64)
  timestamp_finish = GetTickCount64();
  elapsed = (timestamp_finish - timestamp_start) * 1e-3;
#else
  if (clock_gettime(CLOCK_MONOTONIC, &timestamp_finish)) {
    err = errno;
    error("clock_gettime failed, error %d %s\n", err, mdbx_strerror(err));
    return EXIT_FAILURE_SYS;
  }
  elapsed = timestamp_finish.tv_sec - timestamp_start.tv_sec +
            (timestamp_finish.tv_nsec - timestamp_start.tv_nsec) * 1e-9;
#endif /* !WINDOWS */

  total_problems += problems_meta;
  if (total_problems || problems_maindb || problems_gc) {
    print("Total %" PRIu64 " error%s detected, elapsed %.3f seconds.\n", total_problems,
          (total_problems > 1) ? "s are" : " is", elapsed);
    if (problems_meta || problems_maindb || problems_gc)
      return EXIT_FAILURE_CHECK_MAJOR;
    return EXIT_FAILURE_CHECK_MINOR;
  }
  print("No error is detected, elapsed %.3f seconds\n", elapsed);
  return EXIT_SUCCESS;
}
