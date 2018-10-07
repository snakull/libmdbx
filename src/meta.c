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

#define meta_extra(fmt, ...) log_extra(MDBX_LOG_META, fmt, ##__VA_ARGS__)
#define meta_trace(fmt, ...) log_trace(MDBX_LOG_META, fmt, ##__VA_ARGS__)
#define meta_verbose(fmt, ...) log_verbose(MDBX_LOG_META, fmt, ##__VA_ARGS__)
#define meta_info(fmt, ...) log_info(MDBX_LOG_META, fmt, ##__VA_ARGS__)
#define meta_notice(fmt, ...) log_notice(MDBX_LOG_META, fmt, ##__VA_ARGS__)
#define meta_warning(fmt, ...) log_warning(MDBX_LOG_META, fmt, ##__VA_ARGS__)
#define meta_error(fmt, ...) log_error(MDBX_LOG_META, fmt, ##__VA_ARGS__)
#define meta_panic(env, msg, err) mdbx_panic(env, MDBX_LOG_META, __func__, __LINE__, "%s, error %d", msg, err)

/*----------------------------------------------------------------------------*/

static inline meta_t *metapage(const MDBX_env_t *env, unsigned n) { return &pgno2page(env, n)->mp_meta; }

#define METAPAGE_END(env) metapage(env, MDBX_NUM_METAS)

static inline txnid_t meta_txnid(const MDBX_env_t *env, const meta_t *meta, bool allow_volatile) {
  mdbx_assert(env, meta >= metapage(env, 0) || meta < METAPAGE_END(env));
  txnid_t a = meta->mm_txnid_a;
  txnid_t b = meta->mm_txnid_b;
  if (allow_volatile)
    return (a == b) ? a : 0;
  mdbx_assert(env, a == b);
  return a;
}

static inline txnid_t meta_txnid_stable(const MDBX_env_t *env, const meta_t *meta) {
  return meta_txnid(env, meta, false);
}

static inline txnid_t meta_txnid_fluid(const MDBX_env_t *env, const meta_t *meta) {
  return meta_txnid(env, meta, true);
}

static inline void meta_update_begin(const MDBX_env_t *env, meta_t *meta, txnid_t txnid) {
  mdbx_assert(env, meta >= metapage(env, 0) || meta < METAPAGE_END(env));
  mdbx_assert(env, meta->mm_txnid_a < txnid && meta->mm_txnid_b < txnid);
  meta->mm_txnid_a = txnid;
  (void)env;
  mdbx_coherent_barrier();
}

static inline void meta_update_end(const MDBX_env_t *env, meta_t *meta, txnid_t txnid) {
  mdbx_assert(env, meta >= metapage(env, 0) || meta < METAPAGE_END(env));
  mdbx_assert(env, meta->mm_txnid_a == txnid);
  mdbx_assert(env, meta->mm_txnid_b < txnid);

  jitter4testing(true);
  meta->mm_txnid_b = txnid;
  mdbx_coherent_barrier();
}

static inline void meta_set_txnid(const MDBX_env_t *env, meta_t *meta, txnid_t txnid) {
  mdbx_assert(env, meta < metapage(env, 0) || meta > METAPAGE_END(env));
  meta->mm_txnid_a = txnid;
  meta->mm_txnid_b = txnid;
}

static inline uint64_t meta_sign(const meta_t *meta) {
  uint64_t sign = MDBX_DATASIGN_NONE;
#if 0 /* TODO */
  sign = hippeus_hash64(...);
#else
  (void)meta;
#endif
  /* LY: newer returns MDBX_DATASIGN_NONE or MDBX_DATASIGN_WEAK */
  return (sign > MDBX_DATASIGN_WEAK) ? sign : ~sign;
}

static inline bool meta_ot(const enum meta_choise_mode mode, const MDBX_env_t *env, const meta_t *a,
                           const meta_t *b) {
  jitter4testing(true);
  txnid_t txnid_a = meta_txnid_fluid(env, a);
  txnid_t txnid_b = meta_txnid_fluid(env, b);

  jitter4testing(true);
  switch (mode) {
  default:
    assert(false);
  /* fall through */
  case prefer_steady:
    if (META_IS_STEADY(a) != META_IS_STEADY(b))
      return META_IS_STEADY(b);
  /* fall through */
  case prefer_noweak:
    if (META_IS_WEAK(a) != META_IS_WEAK(b))
      return !META_IS_WEAK(b);
  /* fall through */
  case prefer_last:
    jitter4testing(true);
    if (txnid_a == txnid_b)
      return META_IS_STEADY(b) || (META_IS_WEAK(a) && !META_IS_WEAK(b));
    return txnid_a < txnid_b;
  }
}

static inline bool meta_eq(const MDBX_env_t *env, const meta_t *a, const meta_t *b) {
  jitter4testing(true);
  const txnid_t txnid = meta_txnid_fluid(env, a);
  if (!txnid || txnid != meta_txnid_fluid(env, b))
    return false;

  jitter4testing(true);
  if (META_IS_STEADY(a) != META_IS_STEADY(b))
    return false;

  jitter4testing(true);
  return true;
}

static int meta_eq_mask(const MDBX_env_t *env) {
  meta_t *m0 = metapage(env, 0);
  meta_t *m1 = metapage(env, 1);
  meta_t *m2 = metapage(env, 2);

  int rc = meta_eq(env, m0, m1) ? 1 : 0;
  if (meta_eq(env, m1, m2))
    rc += 2;
  if (meta_eq(env, m2, m0))
    rc += 4;
  return rc;
}

static inline meta_t *meta_recent(const enum meta_choise_mode mode, const MDBX_env_t *env, meta_t *a,
                                  meta_t *b) {
  const bool a_older_that_b = meta_ot(mode, env, a, b);
  mdbx_assert(env, !meta_eq(env, a, b));
  return a_older_that_b ? b : a;
}

static inline meta_t *meta_ancient(const enum meta_choise_mode mode, const MDBX_env_t *env, meta_t *a,
                                   meta_t *b) {
  const bool a_older_that_b = meta_ot(mode, env, a, b);
  mdbx_assert(env, !meta_eq(env, a, b));
  return a_older_that_b ? a : b;
}

static inline meta_t *meta_mostrecent(const enum meta_choise_mode mode, const MDBX_env_t *env) {
  meta_t *m0 = metapage(env, 0);
  meta_t *m1 = metapage(env, 1);
  meta_t *m2 = metapage(env, 2);

  meta_t *head = meta_recent(mode, env, m0, m1);
  head = meta_recent(mode, env, head, m2);
  return head;
}

static __hot meta_t *meta_steady(const MDBX_env_t *env) { return meta_mostrecent(prefer_steady, env); }

static __hot meta_t *meta_head(const MDBX_env_t *env) { return meta_mostrecent(prefer_last, env); }

static __hot txnid_t reclaiming_detent(const MDBX_env_t *env) {
  if (F_ISSET(env->me_flags32, MDBX_UTTERLY_NOSYNC))
    return env->me_current_txn->mt_txnid - 1;

  return meta_txnid_stable(env, meta_steady(env));
}

static const char *durable_str(const meta_t *const meta) {
  if (META_IS_WEAK(meta))
    return "Weak";
  if (META_IS_STEADY(meta))
    return (meta->mm_sign_checksum == meta_sign(meta)) ? "Steady" : "Tainted";
  return "Legacy";
}

static page_t *__cold meta_model(const MDBX_env_t *env, page_t *model, unsigned num) {

  mdbx_ensure(env, is_power_of_2(env->me_psize));
  mdbx_ensure(env, env->me_psize >= MIN_PAGESIZE);
  mdbx_ensure(env, env->me_psize <= MAX_PAGESIZE);
  mdbx_ensure(env, env->me_dxb_geo.lower >= MIN_MAPSIZE);
  mdbx_ensure(env, env->me_dxb_geo.upper <= MAX_MAPSIZE);
  mdbx_ensure(env, env->me_dxb_geo.now >= env->me_dxb_geo.lower);
  mdbx_ensure(env, env->me_dxb_geo.now <= env->me_dxb_geo.upper);

  memset(model, 0, sizeof(*model));
  model->mp_pgno = num;
  model->mp_flags16 = P_META;
  model->mp_meta.mm_magic_and_version = MDBX_DATA_MAGIC;

  model->mp_meta.mm_dxb_geo.lower = bytes2pgno(env, env->me_dxb_geo.lower);
  model->mp_meta.mm_dxb_geo.upper = bytes2pgno(env, env->me_dxb_geo.upper);
  model->mp_meta.mm_dxb_geo.grow16 = (uint16_t)bytes2pgno(env, env->me_dxb_geo.grow);
  model->mp_meta.mm_dxb_geo.shrink16 = (uint16_t)bytes2pgno(env, env->me_dxb_geo.shrink);
  model->mp_meta.mm_dxb_geo.now = bytes2pgno(env, env->me_dxb_geo.now);
  model->mp_meta.mm_dxb_geo.next = MDBX_NUM_METAS;

  mdbx_ensure(env, model->mp_meta.mm_dxb_geo.lower >= MIN_PAGENO);
  mdbx_ensure(env, model->mp_meta.mm_dxb_geo.upper <= MAX_PAGENO);
  mdbx_ensure(env, model->mp_meta.mm_dxb_geo.now >= model->mp_meta.mm_dxb_geo.lower);
  mdbx_ensure(env, model->mp_meta.mm_dxb_geo.now <= model->mp_meta.mm_dxb_geo.upper);
  mdbx_ensure(env, model->mp_meta.mm_dxb_geo.next >= MIN_PAGENO);
  mdbx_ensure(env, model->mp_meta.mm_dxb_geo.next <= model->mp_meta.mm_dxb_geo.now);
  mdbx_ensure(env, model->mp_meta.mm_dxb_geo.grow16 == bytes2pgno(env, env->me_dxb_geo.grow));
  mdbx_ensure(env, model->mp_meta.mm_dxb_geo.shrink16 == bytes2pgno(env, env->me_dxb_geo.shrink));

  const txnid_t txnid = MIN_TXNID + num;
  const MDBX_time_t now_timestamp = {0};
  const uint16_t db_flags16 = (uint16_t)(env->me_flags32 & (MDBX_DB_FLAGS | MDBX_AA_FLAGS));
  const uint16_t db_features16 = 0 /* TODO */;
  const uint16_t main_keysize_if_fixed = 0 /* FIXME */;

  aatree_t *const aa_gaco = &model->mp_meta.mm_aas[MDBX_GACO_AAH];
  set_le16_aligned(&aa_gaco->aa_flags16, db_features16);
  set_le32_aligned(&aa_gaco->aa_xsize32, env->me_psize);
  set_le64_aligned(&aa_gaco->aa_modification_txnid, txnid);
  set_le64_aligned(&aa_gaco->aa_modification_time.fixedpoint, now_timestamp.fixedpoint);
  /* TODO: meta_gaco_reserved */
  set_le32_aligned(&aa_gaco->aa_root, P_INVALID);
  set_le64_aligned(&aa_gaco->aa_merkle, aa_checksum(env, aa_gaco));

  aatree_t *const aa_main = &model->mp_meta.mm_aas[MDBX_MAIN_AAH];
  set_le16_aligned(&aa_main->aa_flags16, db_flags16);
  set_le32_aligned(&aa_main->aa_xsize32, main_keysize_if_fixed);
  set_le64_aligned(&aa_main->aa_creation_txnid, txnid);
  set_le64_aligned(&aa_main->aa_creation_time.fixedpoint, now_timestamp.fixedpoint);
  set_le64_aligned(&aa_main->aa_modification_txnid, txnid);
  set_le64_aligned(&aa_main->aa_modification_time.fixedpoint, now_timestamp.fixedpoint);
  set_le32_aligned(&aa_main->aa_root, P_INVALID);
  set_le64_aligned(&aa_main->aa_merkle, aa_checksum(env, aa_main));

  meta_set_txnid(env, &model->mp_meta, txnid);
  model->mp_meta.mm_sign_checksum = meta_sign(&model->mp_meta);
  return (page_t *)((uint8_t *)model + env->me_psize);
}

/* Fill in most of the zeroed meta-pages for an empty database databook.
 * Return pointer to recenly (head) meta-page. */
static page_t *__cold init_metas(const MDBX_env_t *env, void *buffer) {
  page_t *page0 = (page_t *)buffer;
  page_t *page1 = meta_model(env, page0, 0);
  page_t *page2 = meta_model(env, page1, 1);
  meta_model(env, page2, 2);
  if (MDBX_DEVEL)
    page2->mp_meta.mm_sign_checksum = MDBX_DATASIGN_WEAK;
  mdbx_assert(env, !meta_eq(env, &page0->mp_meta, &page1->mp_meta));
  mdbx_assert(env, !meta_eq(env, &page1->mp_meta, &page2->mp_meta));
  mdbx_assert(env, !meta_eq(env, &page2->mp_meta, &page0->mp_meta));
  return MDBX_DEVEL ? page1 : page2;
}
