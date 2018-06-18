/* mdbx_dump.c - memory-mapped database dump tool */

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
#include <ctype.h>

#define PRINT 1
static int mode;

typedef struct flagbit {
  int bit;
  char *name;
} flagbit;

flagbit dbflags[] = {{MDBX_REVERSEKEY, "reversekey"},
                     {MDBX_DUPSORT, "dupsort"},
                     {MDBX_INTEGERKEY, "integerkey"},
                     {MDBX_DUPFIXED, "dupfixed"},
                     {MDBX_INTEGERDUP, "integerdup"},
                     {MDBX_REVERSEDUP, "reversedup"},
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

static const char hexc[] = "0123456789abcdef";

static void dumpbyte(unsigned char c) {
  putchar(hexc[c >> 4]);
  putchar(hexc[c & 0xf]);
}

static void text(MDBX_iov_t *v) {
  unsigned char *c, *end;

  putchar(' ');
  c = v->iov_base;
  end = c + v->iov_len;
  while (c < end) {
    if (isprint(*c) && *c != '\\') {
      putchar(*c);
    } else {
      putchar('\\');
      dumpbyte(*c);
    }
    c++;
  }
  putchar('\n');
}

static void dumpval(MDBX_iov_t *v) {
  unsigned char *c, *end;

  putchar(' ');
  c = v->iov_base;
  end = c + v->iov_len;
  while (c < end) {
    dumpbyte(*c++);
  }
  putchar('\n');
}

/* Dump in BDB-compatible format */
static int dumpit(MDBX_txn_t *txn, MDBX_aah_t aah, const MDBX_iov_t name) {
  MDBX_aa_info_t aa_info;
  MDBX_iov_t key, data;
  MDBX_db_info_t bk_info;
  int err, i;

  err = mdbx_aa_info(txn, aah, &aa_info, sizeof(aa_info));
  if (err)
    return err;

  err = mdbx_info(mdbx_txn_env(txn).env, &bk_info, sizeof(bk_info));
  if (err)
    return err;

  printf("VERSION=3\n");
  printf("format=%s\n", mode & PRINT ? "print" : "bytevalue");
  if (name.iov_len)
    printf("database=%*s\n", (int)name.iov_len, (const char *)name.iov_base);
  printf("type=btree\n");
  printf("mapsize=%" PRIu64 "\n", bk_info.bi_mapsize);
  printf("maxreaders=%u\n", bk_info.bi_readers_max);

  for (i = 0; dbflags[i].bit; i++)
    if (aa_info.ai_flags & dbflags[i].bit)
      printf("%s=1\n", dbflags[i].name);

  printf("db_pagesize=%d\n", bk_info.bi_pagesize);
  printf("HEADER=END\n");

  MDBX_cursor_result_t xr = mdbx_cursor_open(txn, aah);
  if (xr.err != MDBX_SUCCESS)
    return xr.err;

  while ((err = mdbx_cursor_get(xr.cursor, &key, &data, MDBX_NEXT)) == MDBX_SUCCESS) {
    if (user_break) {
      err = MDBX_EINTR;
      break;
    }
    if (mode & PRINT) {
      text(&key);
      text(&data);
    } else {
      dumpval(&key);
      dumpval(&data);
    }
  }
  printf("DATA=END\n");
  if (err == MDBX_NOTFOUND)
    err = MDBX_SUCCESS;
  mdbx_cursor_close(xr.cursor);

  return err;
}

static void usage(char *prog) {
  fprintf(stderr, "usage: %s [-V] [-f output] [-l] [-n] [-p] [-a|-s subdb] dbpath\n", prog);
  exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
  int i;
  MDBX_error_t err;
  char *prog = argv[0];
  char *envname;
  char *subname = NULL;
  int alldbs = 0, envflags = 0, list = 0;

  if (argc < 2) {
    usage(prog);
  }

  /* -a: dump main AA and all subAAs
   * -s: dump only the named subAA
   * -n: use NOSUBDIR flag on env_open
   * -p: use printable characters
   * -f: write to file instead of stdout
   * -V: print version and exit
   * (default) dump only the main AA
   */
  while ((i = getopt(argc, argv, "af:lnps:V")) != EOF) {
    switch (i) {
    case 'V':
      printf("%s (%s, build %s)\n", mdbx_version.git.describe, mdbx_version.git.datetime, mdbx_build.datetime);
      exit(EXIT_SUCCESS);
      break;
    case 'l':
      list = 1;
    /* fallthrough */
    case 'a':
      if (subname)
        usage(prog);
      alldbs++;
      break;
    case 'f':
      if (freopen(optarg, "w", stdout) == NULL) {
        fprintf(stderr, "%s: %s: reopen: %s\n", prog, optarg, strerror(errno));
        exit(EXIT_FAILURE);
      }
      break;
    case 'n':
      /* silently ignore for compatibility (MDB_NOSUBDIR option) */
      break;
    case 'p':
      mode |= PRINT;
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

  MDBX_env_t *env;
  err = mdbx_init(&env);
  if (err) {
    fprintf(stderr, "mdbx_init failed, error %d %s\n", err, mdbx_strerror(err));
    return EXIT_FAILURE;
  }

  if (alldbs || subname)
    mdbx_set_maxhandles(env, 2);

  err = mdbx_open(env, envname, envflags | MDBX_RDONLY, 0664);
  if (err) {
    fprintf(stderr, "mdbx_open failed, error %d %s\n", err, mdbx_strerror(err));
    goto env_close;
  }

  const MDBX_txn_result_t tr = mdbx_begin(env, NULL, MDBX_RDONLY);
  MDBX_txn_t *const txn = tr.txn;
  err = tr.err;
  if (err) {
    fprintf(stderr, "mdbx_begin failed, error %d %s\n", err, mdbx_strerror(err));
    goto env_close;
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

  if (alldbs) {
    MDBX_iov_t key;
    int count = 0;

    MDBX_cursor_result_t xr = mdbx_cursor_open(txn, aah);
    if (xr.err) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", xr.err, mdbx_strerror(xr.err));
      goto txn_abort;
    }
    while ((err = mdbx_cursor_get(xr.cursor, &key, NULL, MDBX_NEXT_NODUP)) == 0) {
      if (user_break) {
        err = MDBX_EINTR;
        break;
      }
      if (key.iov_len < 1 || memchr(key.iov_base, '\0', key.iov_len))
        continue;
      count++;
      MDBX_aah_result_t aah_result = mdbx_aa_open(txn, key, 0, NULL, NULL);
      if (aah_result.err)
        continue;
      if (list) {
        printf("%*s\n", (int)key.iov_len, (const char *)key.iov_base);
        list++;
      } else {
        err = dumpit(txn, aah_result.aah, key);
        if (err)
          break;
      }
      mdbx_aa_close(env, aah_result.aah);
    }
    mdbx_cursor_close(xr.cursor);

    if (!count) {
      fprintf(stderr, "%s: %s does not contain multiple databases\n", prog, envname);
      err = MDBX_NOTFOUND;
    } else if (err == MDBX_INCOMPATIBLE) {
      /* LY: the record it not a named sub-db. */
      err = MDBX_SUCCESS;
    }
  } else {
    err = dumpit(txn, aah, mdbx_str2iov(subname));
  }
  if (err && err != MDBX_NOTFOUND)
    fprintf(stderr, "%s: %s: %s\n", prog, envname, mdbx_strerror(err));

  mdbx_aa_close(env, aah);
txn_abort:
  mdbx_abort(txn);
env_close:
  mdbx_shutdown(env);

  return err ? EXIT_FAILURE : EXIT_SUCCESS;
}
