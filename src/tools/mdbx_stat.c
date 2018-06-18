/* mdbx_stat.c - memory-mapped database status tool */

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

static void prstat(MDBX_aa_info_t *ms) {
  printf("  Tree depth: %u\n", ms->ai_tree_depth);
  printf("  Branch pages: %" PRIu64 "\n", ms->ai_branch_pages);
  printf("  Leaf pages: %" PRIu64 "\n", ms->ai_leaf_pages);
  printf("  Overflow pages: %" PRIu64 "\n", ms->ai_overflow_pages);
  printf("  Entries: %" PRIu64 "\n", ms->ai_entries);
}

static void usage(char *prog) {
  fprintf(stderr, "usage: %s [-V] [-n] [-e] [-r[r]] [-f[f[f]]] [-a|-s subdb] dbpath\n", prog);
  exit(EXIT_FAILURE);
}

static MDBX_error_t readers_enum_func(void *ctx, unsigned index, MDBX_pid_t pid, MDBX_tid_t tid,
                                      uint64_t txnid) {
  bool *first = (bool *)ctx;
  if (*first && puts("slot    pid     thread     txnid\n") < 0)
    return (MDBX_error_t)errno;
  *first = false;

  int num;
  if (txnid == ~(txnid_t)0)
    num = printf("%3u %10" PRIuPTR " %" PRIxPTR " -\n", index, (uintptr_t)pid, (uintptr_t)tid);
  else
    num =
        printf("%3u %10" PRIuPTR " %" PRIxPTR " %" PRIaTXN "\n", index, (uintptr_t)pid, (uintptr_t)tid, txnid);

  return (num > 0) ? MDBX_SUCCESS : errno;
}

static MDBX_error_t readers_enum(MDBX_env_t *env) {
  bool first = true;
  MDBX_error_t err = mdbx_readers_enum(env, readers_enum_func, &first);
  if (err == MDBX_SUCCESS && first)
    err = (puts("(no active readers)\n") > 0) ? MDBX_SUCCESS : errno;
  return err;
}

int main(int argc, char *argv[]) {
  int o;
  MDBX_error_t err;
  MDBX_env_t *env;
  MDBX_aa_info_t aa_info;
  MDBX_db_info_t bk_info;
  char *prog = argv[0];
  char *envname;
  char *subname = NULL;
  int alldbs = 0, envinfo = 0, envflags = 0, gacoinfo = 0, rdrinfo = 0;

  if (argc < 2) {
    usage(prog);
  }

  /* -a: print stat of main AA and all subAAs
   * -s: print stat of only the named subAA
   * -e: print env info
   * -f: print gaco info
   * -r: print reader info
   * -n: use NOSUBDIR flag on env_open
   * -V: print version and exit
   * (default) print stat of only the main AA
   */
  while ((o = getopt(argc, argv, "Vaefnrs:")) != EOF) {
    switch (o) {
    case 'V':
      printf("%s (%s, build %s)\n", mdbx_version.git.describe, mdbx_version.git.datetime, mdbx_build.datetime);
      exit(EXIT_SUCCESS);
      break;
    case 'a':
      if (subname)
        usage(prog);
      alldbs++;
      break;
    case 'e':
      envinfo++;
      break;
    case 'f':
      gacoinfo++;
      break;
    case 'n':
      /* silently ignore for compatibility (MDB_NOSUBDIR option) */
      break;
    case 'r':
      rdrinfo++;
      break;
    case 's':
      if (alldbs)
        usage(prog);
      subname = optarg;
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

#if MDBX_DEBUG == 0
  mdbx_set_loglevel(MDBX_LOG_ALL, MDBX_LOGLEVEL_ERROR);
#elif MDBX_DEBUG == 1
  mdbx_set_loglevel(MDBX_LOG_ALL, MDBX_LOGLEVEL_NOTICE);
#else
  mdbx_set_loglevel(MDBX_LOG_ALL, MDBX_LOGLEVEL_EXTRA);
#endif

  err = mdbx_init(&env);
  if (err) {
    fprintf(stderr, "mdbx_init failed, error %d %s\n", err, mdbx_strerror(err));
    return EXIT_FAILURE;
  }

  if (alldbs || subname) {
    mdbx_set_maxhandles(env, 4);
  }

  err = mdbx_open(env, envname, envflags | MDBX_RDONLY, 0664);
  if (err) {
    fprintf(stderr, "mdbx_open failed, error %d %s\n", err, mdbx_strerror(err));
    goto env_close;
  }

  if (envinfo || gacoinfo) {
    (void)mdbx_info(env, &bk_info, sizeof(bk_info));
  } else {
    /* LY: zap warnings from gcc */
    memset(&bk_info, 0, sizeof(bk_info));
  }

  if (envinfo) {
    printf("Databook Info\n");
    printf("  Pagesize: %u\n", bk_info.bi_pagesize);
    if (bk_info.bi_dxb_geo.lower != bk_info.bi_dxb_geo.upper) {
      printf("  Dynamic datafile: %" PRIu64 "..%" PRIu64 " bytes (+%" PRIu32 "/-%" PRIu32 "), %" PRIu64
             "..%" PRIu64 " pages (+%" PRIu32 "/-%" PRIu32 ")\n",
             bk_info.bi_dxb_geo.lower, bk_info.bi_dxb_geo.upper, bk_info.bi_dxb_geo.grow,
             bk_info.bi_dxb_geo.shrink, bk_info.bi_dxb_geo.lower / bk_info.bi_pagesize,
             bk_info.bi_dxb_geo.upper / bk_info.bi_pagesize, bk_info.bi_dxb_geo.grow / bk_info.bi_pagesize,
             bk_info.bi_dxb_geo.shrink / bk_info.bi_pagesize);
      printf("  Current datafile: %" PRIu64 " bytes, %" PRIu64 " pages\n", bk_info.bi_dxb_geo.current,
             bk_info.bi_dxb_geo.current / bk_info.bi_pagesize);
    } else {
      printf("  Fixed datafile: %" PRIu64 " bytes, %" PRIu64 " pages\n", bk_info.bi_dxb_geo.current,
             bk_info.bi_dxb_geo.current / bk_info.bi_pagesize);
    }
    printf("  Current mapsize: %" PRIu64 " bytes, %" PRIu64 " pages \n", bk_info.bi_mapsize,
           bk_info.bi_mapsize / bk_info.bi_pagesize);
    printf("  Number of pages used: %" PRIu64 "\n", bk_info.bi_dxb_last_pgno + 1);
    printf("  Last transaction ID: %" PRIu64 "\n", bk_info.bi_recent_txnid);
    printf("  Tail transaction ID: %" PRIu64 " (%" PRIi64 ")\n", bk_info.bi_latter_reader_txnid,
           bk_info.bi_latter_reader_txnid - bk_info.bi_recent_txnid);
    printf("  Max readers: %u\n", bk_info.bi_readers_max);
    printf("  Number of readers used: %u\n", bk_info.bi_readers_num);
  }

  if (rdrinfo) {
    printf("Reader Table Status\n");
    err = readers_enum(env);
    if (err) {
      fprintf(stderr, "mdbx_readers_enum failed, error %d %s\n", err, mdbx_strerror(err));
      goto env_close;
    }
    if (rdrinfo > 1) {
      MDBX_numeric_result_t readers_check_result = mdbx_readers_check(env);
      err = readers_check_result.err;
      if (err) {
        fprintf(stderr, "mdbx_readers_check failed, error %d %s\n", err, mdbx_strerror(err));
        goto env_close;
      }
      printf("  %zu stale readers cleared.\n", readers_check_result.value);
      err = readers_enum(env);
      if (err) {
        fprintf(stderr, "mdbx_readers_enum failed, error %d %s\n", err, mdbx_strerror(err));
        goto env_close;
      }
    }
    if (!(subname || alldbs || gacoinfo))
      goto env_close;
  }

  const MDBX_txn_result_t tr = mdbx_begin(env, NULL, MDBX_RDONLY);
  MDBX_txn_t *const txn = tr.txn;
  err = tr.err;
  if (err) {
    fprintf(stderr, "mdbx_begin failed, error %d %s\n", err, mdbx_strerror(err));
    goto env_close;
  }

  if (gacoinfo) {
    MDBX_iov_t key, data;
    pgno_t pages = 0, *iptr;
    pgno_t reclaimable = 0;

    printf("Freelist Status\n");
    MDBX_cursor_result_t cr = mdbx_cursor_open(txn, MDBX_GACO_AAH);
    if (cr.err != MDBX_SUCCESS) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", cr.err, mdbx_strerror(cr.err));
      goto txn_abort;
    }
    err = mdbx_aa_info(txn, MDBX_GACO_AAH, &aa_info, sizeof(aa_info));
    if (err) {
      fprintf(stderr, "mdbx_aa_stat failed, error %d %s\n", err, mdbx_strerror(err));
      goto txn_abort;
    }
    prstat(&aa_info);
    while ((err = mdbx_cursor_get(cr.cursor, &key, &data, MDBX_NEXT)) == MDBX_SUCCESS) {
      if (user_break) {
        err = MDBX_EINTR;
        break;
      }
      iptr = data.iov_base;
      const pgno_t number = *iptr++;

      pages += number;
      if (envinfo && bk_info.bi_latter_reader_txnid > *(size_t *)key.iov_base)
        reclaimable += number;

      if (gacoinfo > 1) {
        char *bad = "";
        pgno_t prev = MDBX_PNL_ASCENDING ? MIN_PAGENO : (pgno_t)bk_info.bi_dxb_last_pgno + 1;
        pgno_t span = 1;
        for (unsigned i = 0; i < number; ++i) {
          pgno_t pg = iptr[i];
          if (MDBX_PNL_DISORDERED(prev, pg))
            bad = " [bad sequence]";
          prev = pg;
          while (i + span < number &&
                 iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pg, span) : pgno_sub(pg, span)))
            ++span;
        }
        printf("    Transaction %" PRIaTXN ", %" PRIaPGNO " pages, maxspan %" PRIaPGNO "%s\n",
               *(txnid_t *)key.iov_base, number, span, bad);
        if (gacoinfo > 2) {
          for (unsigned i = 0; i < number; i += span) {
            const pgno_t pg = iptr[i];
            for (span = 1; i + span < number &&
                           iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pg, span) : pgno_sub(pg, span));
                 ++span)
              ;
            if (span > 1)
              printf("     %9" PRIaPGNO "[%" PRIaPGNO "]\n", pg, span);
            else
              printf("     %9" PRIaPGNO "\n", pg);
          }
        }
      }
    }
    mdbx_cursor_close(cr.cursor);

    switch (err) {
    case MDBX_SUCCESS:
    case MDBX_NOTFOUND:
      break;
    case MDBX_EINTR:
      fprintf(stderr, "Interrupted by signal/user\n");
      goto txn_abort;
    default:
      fprintf(stderr, "mdbx_cursor_get failed, error %d %s\n", err, mdbx_strerror(err));
      goto txn_abort;
    }

    if (envinfo) {
      uint64_t value = bk_info.bi_mapsize / bk_info.bi_pagesize;
      double percent = value / 100.0;
      printf("Page Allocation Info\n");
      printf("  Max pages: %" PRIu64 " 100%%\n", value);

      value = bk_info.bi_dxb_last_pgno + 1;
      printf("  Pages used: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = bk_info.bi_mapsize / bk_info.bi_pagesize - (bk_info.bi_dxb_last_pgno + 1);
      printf("  Remained: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = bk_info.bi_dxb_last_pgno + 1 - pages;
      printf("  Used now: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = pages;
      printf("  Unallocated: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = pages - reclaimable;
      printf("  Detained: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = reclaimable;
      printf("  Reclaimable: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = bk_info.bi_mapsize / bk_info.bi_pagesize - (bk_info.bi_dxb_last_pgno + 1) + reclaimable;
      printf("  Available: %" PRIu64 " %.1f%%\n", value, value / percent);
    } else
      printf("  Free pages: %" PRIaPGNO "\n", pages);
  }

  MDBX_aah_t aah = MDBX_MAIN_AAH;
  if (subname) {
    MDBX_aah_result_t aah_result = mdbx_aa_open(txn, mdbx_str2iov(subname), 0, NULL, NULL);
    if (aah_result.err) {
      err = aah_result.err;
      fprintf(stderr, "mdbx_aa_open failed, error %d %s\n", err, mdbx_strerror(err));
      goto txn_abort;
    }
    aah = aah_result.aah;
  }

  err = mdbx_aa_info(txn, aah, &aa_info, sizeof(aa_info));
  if (err) {
    fprintf(stderr, "mdbx_aa_stat failed, error %d %s\n", err, mdbx_strerror(err));
    goto txn_abort;
  }
  printf("Status of %s\n", subname ? subname : "Main AA");
  prstat(&aa_info);

  if (alldbs) {
    MDBX_cursor_result_t xr = mdbx_cursor_open(txn, aah);
    if (xr.err) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", xr.err, mdbx_strerror(xr.err));
      goto txn_abort;
    }

    MDBX_iov_t key;
    while ((err = mdbx_cursor_get(xr.cursor, &key, NULL, MDBX_NEXT_NODUP)) == 0) {
      if (key.iov_len < 1 || memchr(key.iov_base, '\0', key.iov_len))
        continue;
      MDBX_aah_result_t aah_result = mdbx_aa_open(txn, key, 0, NULL, NULL);
      if (aah_result.err)
        continue;

      printf("Status of %*s\n", (int)key.iov_len, (const char *)key.iov_base);
      err = mdbx_aa_info(txn, aah_result.aah, &aa_info, sizeof(aa_info));
      if (err) {
        fprintf(stderr, "mdbx_aa_stat failed, error %d %s\n", err, mdbx_strerror(err));
        goto txn_abort;
      }
      prstat(&aa_info);
      mdbx_aa_close(env, aah_result.aah);
    }
    mdbx_cursor_close(xr.cursor);
  }

  if (err == MDBX_NOTFOUND)
    err = MDBX_SUCCESS;

  mdbx_aa_close(env, aah);
txn_abort:
  mdbx_abort(txn);
env_close:
  mdbx_shutdown(env);

  return err ? EXIT_FAILURE : EXIT_SUCCESS;
}
