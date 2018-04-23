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

#include "./bits.h"

/*----------------------------------------------------------------------------*/
/* Internal prototypes */

static txnid_t find_oldest(MDBX_txn_t *txn);
static MDBX_numeric_result_t check_registered_readers(MDBX_env_t *env, int rlocked);

static void mdbx_iov_free(MDBX_iov_t *iov);

static ahe_t *bk_aah2ahe(MDBX_env_t *env, MDBX_aah_t aah);
static MDBX_aah_t bk_ahe2aah(MDBX_env_t *env, ahe_t *ahe);
static aht_t *txn_ahe2aht(MDBX_txn_t *txn, ahe_t *ahe);
static ahe_t *ahe_main(MDBX_env_t *env);
static ahe_t *ahe_gaco(MDBX_env_t *env);
static aht_t *aht_main(MDBX_txn_t *txn);
static aht_t *aht_gaco(MDBX_txn_t *txn);
static MDBX_cursor_t **aa_txn_cursor_tracking_head(MDBX_txn_t *txn, MDBX_aah_t aah);
static bool aht_valid(const aht_t *aht);

/*----------------------------------------------------------------------------*/
/* Validation functions */
static int validate_env(MDBX_env_t *env, bool check_pid);
static int validate_txn_only(MDBX_txn_t *txn);
static int validate_txn_ro(MDBX_txn_t *txn);
static int validate_txn_more4rw(MDBX_txn_t *txn);
static int validate_txn_rw(MDBX_txn_t *txn);
static int validate_cursor4close(const MDBX_cursor_t *cursor);
static int validate_cursor4operation_ro(const MDBX_cursor_t *cursor);
static int validate_cursor4operation_rw(const MDBX_cursor_t *cursor);

static MDBX_TXL mdbx_txl_alloc(void);
static MDBX_PNL mdbx_pnl_alloc(size_t size);
static void mdbx_pnl_free(MDBX_PNL pl);
static void mdbx_txl_free(MDBX_TXL list);
static void mdbx_pnl_xappend(MDBX_PNL pl, pgno_t id);
static bool mdbx_pnl_check(MDBX_PNL pl);
static void mdbx_pnl_sort(MDBX_PNL pnl);
static size_t mdbx_pnl_search(MDBX_PNL pnl, pgno_t id);
static void mdbx_pnl_shrink(MDBX_PNL *ppl);
static int mdbx_txl_grow(MDBX_TXL *ptr, size_t num);
static int mdbx_pnl_need(MDBX_PNL *ppl, size_t num);
static int mdbx_pnl_append(MDBX_PNL *ppl, pgno_t id);
static int mdbx_txl_append(MDBX_TXL *ptr, txnid_t id);
static int mdbx_pnl_append_list(MDBX_PNL *ppl, MDBX_PNL app);
static int mdbx_txl_append_list(MDBX_TXL *ptr, MDBX_TXL append);
static int mdbx_pnl_append_range(MDBX_PNL *ppl, pgno_t id, size_t n);
static void mdbx_pnl_xmerge(MDBX_PNL pnl, MDBX_PNL merge);
static unsigned mdbx_mid2l_search(MDBX_ID2L pnl, pgno_t id);
static int mdbx_mid2l_insert(MDBX_ID2L pnl, MDBX_ID2 *id);
static int mdbx_mid2l_append(MDBX_ID2L pnl, MDBX_ID2 *id);

#define MDBX_ALLOC_CACHE 1
#define MDBX_ALLOC_GC 2
#define MDBX_ALLOC_NEW 4
#define MDBX_ALLOC_KICK 8
#define MDBX_ALLOC_ALL (MDBX_ALLOC_CACHE | MDBX_ALLOC_GC | MDBX_ALLOC_NEW | MDBX_ALLOC_KICK)

static int page_alloc(cursor_t *mc, unsigned num, page_t **mp, int flags);
static int page_new(cursor_t *mc, uint32_t flags, unsigned num, page_t **mp);
static int page_touch(cursor_t *mc);
static int cursor_touch(cursor_t *mc);
static MDBX_numeric_result_t cursor_count(MDBX_cursor_t *bundle);
static void cursor_copy(const cursor_t *src, cursor_t *dst);
static void cursor_copy_clearsub(const cursor_t *src, cursor_t *dst);
static bool cursor_is_nested_inited(const MDBX_cursor_t *bundle);

#define MDBX_END_NAMES                                                                                        \
  { "committed", "empty-commit", "abort", "reset", "reset-tmp", "fail-begin", "fail-beginchild" }
enum {
  /* txn_end operation number, for logging */
  MDBX_END_COMMITTED,
  MDBX_END_EMPTY_COMMIT,
  MDBX_END_ABORT,
  MDBX_END_RESET,
  MDBX_END_RESET_TMP,
  MDBX_END_FAIL_BEGIN,
  MDBX_END_FAIL_BEGINCHILD
};
#define MDBX_END_OPMASK 0x0F   /* mask for txn_end() operation number */
#define MDBX_END_FIXUPAAH 0x10 /* keep/export opended AAh to environment */
#define MDBX_END_FREE 0x20     /* free txn unless it is MDBX_env_t.me_txn0 */
#define MDBX_END_EOTDONE 0x40  /* txn's cursors already closed */
#define MDBX_END_SLOT 0x80     /* release any reader slot if MDBX_NOTLS */
#define MDBX_END_FLAGS (MDBX_END_OPMASK | MDBX_END_FIXUPAAH | MDBX_END_FREE | MDBX_END_EOTDONE | MDBX_END_SLOT)
static int txn_end(MDBX_txn_t *txn, const unsigned mode);

static int page_get(MDBX_txn_t *const txn, pgno_t pgno, page_t **mp, int *lvl);
static int page_search_root(cursor_t *mc, MDBX_iov_t *key, int modify);

#define MDBX_PS_MODIFY 1
#define MDBX_PS_ROOTONLY 2
#define MDBX_PS_FIRST 4
#define MDBX_PS_LAST 8
#define MDBX_PS_FLAGS (MDBX_PS_MODIFY | MDBX_PS_ROOTONLY | MDBX_PS_FIRST | MDBX_PS_LAST)
static int page_search(cursor_t *mc, MDBX_iov_t *key, int flags);
static int page_merge(cursor_t *csrc, cursor_t *cdst);

#define MDBX_SPLIT_REPLACE MDBX_IUD_CURRENT /* newkey is not new, don't save current */
static int page_split(cursor_t *mc, MDBX_iov_t *newkey, MDBX_iov_t *newdata, pgno_t newpgno, unsigned nflags);

static int mdbx_read_header(MDBX_env_t *env, meta_t *meta);
static void env_destroy(MDBX_env_t *env);
static int env_shutdown(MDBX_env_t *env, MDBX_shutdown_mode_t mode);

static node_rc_t node_search(cursor_t *mc, MDBX_iov_t key);
static node_rc_t node_search_hilo(cursor_t *mc, MDBX_iov_t key, int low, int high);
static int node_add(cursor_t *mc, unsigned indx, MDBX_iov_t *key, MDBX_iov_t *data, pgno_t pgno,
                    unsigned flags);
static void node_del(cursor_t *mc, size_t keysize);
static void node_shrink(page_t *mp, unsigned indx);
static int node_move(cursor_t *csrc, cursor_t *cdst, int fromleft);
static int node_read(cursor_t *mc, node_t *leaf, MDBX_iov_t *data);
static size_t leaf_size(MDBX_env_t *env, MDBX_iov_t *key, MDBX_iov_t *data);
static size_t branch_size(MDBX_env_t *env, MDBX_iov_t *key);

static int tree_rebalance(cursor_t *mc);
static int update_key(cursor_t *mc, MDBX_iov_t *key);

static meta_t *metapage(const MDBX_env_t *env, unsigned n);
static txnid_t meta_txnid_stable(const MDBX_env_t *env, const meta_t *meta);

static txnid_t meta_txnid_fluid(const MDBX_env_t *env, const meta_t *meta);

static void meta_update_begin(const MDBX_env_t *env, meta_t *meta, txnid_t txnid);

static void meta_update_end(const MDBX_env_t *env, meta_t *meta, txnid_t txnid);

static void meta_set_txnid(const MDBX_env_t *env, meta_t *meta, txnid_t txnid);
static uint64_t meta_sign(const meta_t *meta);

enum meta_choise_mode { prefer_last, prefer_noweak, prefer_steady };

static bool meta_ot(const enum meta_choise_mode mode, const MDBX_env_t *env, const meta_t *a, const meta_t *b);
static bool meta_eq(const MDBX_env_t *env, const meta_t *a, const meta_t *b);
static int meta_eq_mask(const MDBX_env_t *env);
static meta_t *meta_recent(const enum meta_choise_mode mode, const MDBX_env_t *env, meta_t *a, meta_t *b);
static meta_t *meta_ancient(const enum meta_choise_mode mode, const MDBX_env_t *env, meta_t *a, meta_t *b);
static meta_t *meta_mostrecent(const enum meta_choise_mode mode, const MDBX_env_t *env);
static meta_t *meta_steady(const MDBX_env_t *env);
static meta_t *meta_head(const MDBX_env_t *env);
static txnid_t reclaiming_detent(const MDBX_env_t *env);
static const char *durable_str(const meta_t *const meta);

////////////////////
static int txn_renew(MDBX_txn_t *txn, unsigned flags);
static int txn_end(MDBX_txn_t *txn, unsigned mode);
static int prep_backlog(MDBX_txn_t *txn, cursor_t *mc);
static MDBX_error_t freelist_save(MDBX_txn_t *txn);
static int mdbx_read_header(MDBX_env_t *env, meta_t *meta);
static page_t *meta_model(const MDBX_env_t *env, page_t *model, unsigned num);
static page_t *init_metas(const MDBX_env_t *env, void *buffer);
static int mdbx_sync_locked(MDBX_env_t *env, unsigned flags, meta_t *const pending);
static void setup_pagesize(MDBX_env_t *env, const size_t pagesize);
static int mdbx_bk_map(MDBX_env_t *env, size_t usedsize);
static MDBX_error_t mdbx_setup_dxb(MDBX_env_t *env, const MDBX_seize_t seize);
static MDBX_seize_result_t setup_lck(MDBX_env_t *env, const char *lck_pathname, mode_t mode);
static void env_destroy(MDBX_env_t *env);
static int tree_drop(cursor_t *mc, int subs);

enum aat_format { af_gaco, af_main, af_user, af_nested };

static int aa_db2txn(const MDBX_env_t *env, const aatree_t *src, aht_t *aht, const enum aat_format format);
static void aa_txn2db(const MDBX_env_t *env, const aht_t *aht, aatree_t *dst, const enum aat_format format);
static MDBX_aah_t bk_ahe2aah(MDBX_env_t *env, ahe_t *ahe);
// static int aa_fetch(MDBX_txn_t *txn, ahe_t *slot);
static aht_rc_t aa_take(MDBX_txn_t *txn, MDBX_aah_t aah);
static unsigned aa_state(MDBX_txn_t *txn, ahe_t *ahe);
static void aa_release(MDBX_env_t *env, ahe_t *ahe);
static int aa_create(MDBX_txn_t *txn, aht_t *aht);
static ahe_rc_t aa_open(MDBX_env_t *env, MDBX_txn_t *txn, const MDBX_iov_t aa_ident, unsigned flags,
                        MDBX_comparer_t *keycmp, MDBX_comparer_t *datacmp);
static int aa_addref(MDBX_env_t *env, MDBX_aah_t aah);
static int aa_close(MDBX_env_t *env, MDBX_aah_t aah);
static int aa_drop(MDBX_txn_t *txn, mdbx_drop_flags_t flags, aht_t *aht);
static int aa_return(MDBX_txn_t *txn, unsigned txn_end_flags);
static MDBX_sequence_result_t aa_sequence(MDBX_txn_t *txn, aht_t *aht, uint64_t increment);
////////////////////

static size_t cursor_size(aht_t *aht);
static int cursor_open(MDBX_txn_t *txn, aht_t *aht, MDBX_cursor_t *bc);
static int cursor_close(MDBX_cursor_t *bc);
static void cursor_untrack(MDBX_cursor_t *bundle);
static void cursor_pop(cursor_t *mc);
static int cursor_push(cursor_t *mc, page_t *mp);
static int txn_shadow_cursors(MDBX_txn_t *src, MDBX_txn_t *dst);
static void cursor_unshadow(MDBX_cursor_t *mc, unsigned commit);

static void cursors_eot(MDBX_txn_t *txn, unsigned merge);

static int cursor_put(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, unsigned flags);
static int cursor_delete(cursor_t *mc, unsigned flags);

static int cursor_sibling(cursor_t *mc, bool move_right);
static int cursor_next(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_cursor_op_t op);
static int cursor_prev(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_cursor_op_t op);
static int cursor_set(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_cursor_op_t op, int *exactp);
static int cursor_first(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data);
static int cursor_last(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data);
static int cursor_get(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_cursor_op_t op);
static int cursor_init(MDBX_cursor_t *bundle, MDBX_txn_t *txn, aht_t *aht);
static cursor_t *nested_setup(cursor_t *mc, node_t *node);
static void subcursor_fixup(MDBX_cursor_t *mc, cursor_t *src_mx, bool new_dupdata);

static MDBX_comparer_t cmp_int_aligned, cmp_int_aligned_to2;
static MDBX_comparer_t *default_keycmp(unsigned flags);
static MDBX_comparer_t *default_datacmp(unsigned flags);

static txnid_t rbr(MDBX_env_t *env, const txnid_t laggard);
static int audit(MDBX_txn_t *txn);
static page_t *page_malloc(MDBX_txn_t *txn, unsigned num);
static void dpage_free(MDBX_env_t *env, page_t *mp);
static void dlist_free(MDBX_txn_t *txn);
static void kill_page(MDBX_env_t *env, pgno_t pgno);
static int page_loose(cursor_t *mc, page_t *mp);
static int page_flush(MDBX_txn_t *txn, pgno_t keep);
static int page_spill(cursor_t *curor, MDBX_iov_t *key, MDBX_iov_t *data);
static void mdbx_page_dirty(MDBX_txn_t *txn, page_t *mp);
static int page_alloc(cursor_t *mc, unsigned num, page_t **mp, int flags);
static void page_copy(page_t *dst, page_t *src, unsigned psize);
static int page_unspill(MDBX_txn_t *txn, page_t *mp, page_t **ret);
static int page_touch(cursor_t *mc);

static txnid_t find_oldest(MDBX_txn_t *txn);
static int mdbx_mapresize(MDBX_env_t *env, const pgno_t size_pgno, const pgno_t limit_pgno);

//-----------------------------------------------------------------------------

static MDBX_pid_t mdbx_getpid(void);
static MDBX_tid_t mdbx_thread_self(void);

static int mdbx_condmutex_init(mdbx_condmutex_t *condmutex);
static int mdbx_condmutex_lock(mdbx_condmutex_t *condmutex);
static int mdbx_condmutex_unlock(mdbx_condmutex_t *condmutex);
static int mdbx_condmutex_signal(mdbx_condmutex_t *condmutex);
static int mdbx_condmutex_wait(mdbx_condmutex_t *condmutex);
static int mdbx_condmutex_destroy(mdbx_condmutex_t *condmutex);

static int mdbx_fastmutex_init(mdbx_fastmutex_t *fastmutex);
static int mdbx_fastmutex_acquire(mdbx_fastmutex_t *fastmutex, unsigned flags);
static int mdbx_fastmutex_release(mdbx_fastmutex_t *fastmutex);
static int mdbx_fastmutex_destroy(mdbx_fastmutex_t *fastmutex);

static int mdbx_pwritev(MDBX_filehandle_t fd, struct iovec *iov, int iovcnt, uint64_t offset,
                        size_t expected_written);
static int mdbx_pread(MDBX_filehandle_t fd, void *buf, size_t count, uint64_t offset);
static int mdbx_pwrite(MDBX_filehandle_t fd, const void *buf, size_t count, uint64_t offset);
static int mdbx_write(MDBX_filehandle_t fd, const void *buf, size_t count);

static int mdbx_thread_create(mdbx_thread_t *thread, THREAD_RESULT(THREAD_CALL *start_routine)(void *),
                              void *arg);
static int mdbx_thread_join(mdbx_thread_t thread);

static int rthc_alloc(mdbx_thread_key_t *key, MDBX_reader_t *begin, MDBX_reader_t *end);
static void rthc_release(const mdbx_thread_key_t key);
static void rthc_global_ctor(void);
static void rthc_global_dtor(void);
static void rthc_thread_dtor(void *ptr);

static int mdbx_filesync(MDBX_filehandle_t fd, bool fullsync);
static int mdbx_filesize_sync(MDBX_filehandle_t fd);
static int mdbx_ftruncate(MDBX_filehandle_t fd, uint64_t length);
static int mdbx_filesize(MDBX_filehandle_t fd, uint64_t *length);
static int mdbx_openfile(const char *pathname, int flags, mode_t mode, MDBX_filehandle_t *fd);
static int mdbx_closefile(MDBX_filehandle_t fd);

static int mdbx_mmap(int flags, mdbx_mmap_t *map, size_t must, size_t limit);
static int mdbx_munmap(mdbx_mmap_t *map);
static int mdbx_mresize(int flags, mdbx_mmap_t *map, size_t current, size_t wanna);
static int mdbx_msync(mdbx_mmap_t *map, size_t offset, size_t length, int async);

/*----------------------------------------------------------------------------*/
/* Internal prototypes and inlines */

static void paranoia_barrier(void);
static bool is_power_of_2(size_t x);
static size_t mdbx_roundup2(size_t value, size_t granularity);
static size_t mdbx_roundup_ptrsize(size_t value);
static size_t bytes_align2os_bytes(const MDBX_env_t *env, size_t bytes);
static unsigned uint_log2_ceil(size_t value);

static MDBX_cursor_t **cursor_tracking_head(const MDBX_cursor_t *cursor);
static MDBX_comparer_t *cursor_key_comparer(const cursor_t *cursor);
static ptrdiff_t cursor_compare_keys(const cursor_t *cursor, const MDBX_iov_t *a, const MDBX_iov_t *b);
static MDBX_comparer_t *cursor_data_comparer(const cursor_t *cursor);
static ptrdiff_t cursor_compare_data(const cursor_t *cursor, const MDBX_iov_t *a, const MDBX_iov_t *b);
static bool cursor_is_aah_valid(const MDBX_cursor_t *cursor);

static MDBX_cursor_t *cursor_bundle(const cursor_t *cursor);
static cursor_t *cursor_primal(cursor_t *cursor);
static subcur_t *cursor_subcur(const cursor_t *cursor);
static subcur_t *nested_subcursor(const cursor_t *cursor);
static cursor_t *cursor_nested(const cursor_t *cursor);
static cursor_t *cursor_nested_or_null(const cursor_t *cursor);
static void cursor_refresh_subcursor(MDBX_cursor_t *cursor, const unsigned top, page_t *page);
typedef struct copy_ctx_ copy_ctx_t;
static int bk_copy_compact(MDBX_env_t *env, MDBX_filehandle_t fd);
static int bk_copy_asis(MDBX_env_t *env, MDBX_filehandle_t fd);
static int bk_copy_walk(copy_ctx_t *my, pgno_t *pg, int flags);

typedef struct walk_ctx_ {
  MDBX_txn_t *mw_txn;
  void *mw_user;
  MDBX_walk_func_t *mw_visitor;
} walk_ctx_t;
static int do_walk(walk_ctx_t *ctx, const MDBX_iov_t ident, pgno_t pg, int deep);

/* Perform act while tracking temporary cursor mn */
#define WITH_CURSOR_TRACKING(mn, act)                                                                         \
  do {                                                                                                        \
    MDBX_cursor_t *_tracked = &(mn);                                                                          \
    MDBX_cursor_t **_head = cursor_tracking_head(_tracked);                                                   \
    _tracked->mc_next = *_head;                                                                               \
    *_head = _tracked;                                                                                        \
    { act; }                                                                                                  \
    *_head = _tracked->mc_next;                                                                               \
  } while (0)

static void jitter4testing(bool tiny);
static size_t pgno2bytes(const MDBX_env_t *env, pgno_t pgno);
static page_t *pgno2page(const MDBX_env_t *env, pgno_t pgno);
static pgno_t bytes2pgno(const MDBX_env_t *env, size_t bytes);
static pgno_t pgno_add(pgno_t base, pgno_t augend);
static pgno_t pgno_sub(pgno_t base, pgno_t subtrahend);
static size_t pgno_align2os_bytes(const MDBX_env_t *env, pgno_t pgno);
static pgno_t pgno_align2os_pgno(const MDBX_env_t *env, pgno_t pgno);
static void set_pgno_aligned2(void *ptr, pgno_t pgno);
static pgno_t get_pgno_aligned2(void *ptr);

static ptrdiff_t cmp_none(const MDBX_iov_t a, const MDBX_iov_t b);

#define mdbx_nodemax(pagesize) (((((pagesize)-PAGEHDRSZ) / MDBX_MINKEYS) & -(intptr_t)2) - sizeof(indx_t))

#define mdbx_maxkey(nodemax) ((nodemax) - (NODESIZE + sizeof(aatree_t)))

#define mdbx_maxfree1pg(pagesize) (((pagesize)-PAGEHDRSZ) / sizeof(pgno_t) - 1)

static inline void set_signature(const size_t *ptr, size_t value) { *(size_t *)ptr = value; }

static inline MDBX_numeric_result_t numeric_result(MDBX_error_t err, uintptr_t value) {
  MDBX_numeric_result_t result = {value, err};
  return result;
}

static inline void mdbx_jitter4testing(bool tiny) {
#ifndef NDEBUG
  if (MDBX_DBG_JITTER & mdbx_debug_bits)
    mdbx_jitter(tiny);
#else
  (void)tiny;
#endif
}
