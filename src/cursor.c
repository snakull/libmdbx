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

#define cursor_extra(fmt, ...) log_extra(MDBX_LOG_CURSOR, fmt, ##__VA_ARGS__)
#define cursor_trace(fmt, ...) log_trace(MDBX_LOG_CURSOR, fmt, ##__VA_ARGS__)
#define cursor_verbose(fmt, ...) log_verbose(MDBX_LOG_CURSOR, fmt, ##__VA_ARGS__)
#define cursor_info(fmt, ...) log_info(MDBX_LOG_CURSOR, fmt, ##__VA_ARGS__)
#define cursor_notice(fmt, ...) log_notice(MDBX_LOG_CURSOR, fmt, ##__VA_ARGS__)
#define cursor_warning(fmt, ...) log_warning(MDBX_LOG_CURSOR, fmt, ##__VA_ARGS__)
#define cursor_error(fmt, ...) log_error(MDBX_LOG_CURSOR, fmt, ##__VA_ARGS__)
#define cursor_panic(env, msg, err)                                                                           \
  mdbx_panic(env, MDBX_LOG_CURSOR, __func__, __LINE__, "%s, error %d", msg, err)

//-----------------------------------------------------------------------------

static inline MDBX_cursor_t *cursor_primal2bundle(const cursor_t *cursor) {
  assert((cursor->mc_kind8 & S_SUBCURSOR) == 0);
  return container_of(cursor, MDBX_cursor_t, primal);
}

static inline MDBX_cursor_t *cursor_nested2bundle(const cursor_t *cursor) {
  assert((cursor->mc_kind8 & S_SUBCURSOR) != 0);
  return container_of(cursor, MDBX_cursor_t, subcursor.mx_cursor);
}

static inline MDBX_cursor_t *cursor_bundle(const cursor_t *cursor) {
  return (cursor->mc_kind8 & S_SUBCURSOR) ? cursor_nested2bundle(cursor) : cursor_primal2bundle(cursor);
}

static inline cursor_t *cursor_primal(cursor_t *cursor) {
  return (cursor->mc_kind8 & S_SUBCURSOR) ? &cursor_nested2bundle(cursor)->primal : cursor;
}

static inline subcur_t *cursor_subcur(const cursor_t *cursor) {
  return &cursor_primal2bundle(cursor)->subcursor;
}

static inline cursor_t *cursor_nested(const cursor_t *cursor) {
  assert(cursor->mc_kind8 & S_HAVESUB);
  return &cursor_subcur(cursor)->mx_cursor;
}

static inline cursor_t *cursor_nested_or_null(const cursor_t *cursor) {
  return (cursor->mc_kind8 & S_HAVESUB) ? &cursor_subcur(cursor)->mx_cursor : nullptr;
}

static inline subcur_t *nested_subcursor(const cursor_t *cursor) {
  assert(cursor->mc_kind8 & S_SUBCURSOR);
  return container_of(cursor, subcur_t, mx_cursor);
}

static inline MDBX_cursor_t **cursor_tracking_head(const MDBX_cursor_t *bundle) {
  MDBX_txn_t *txn = bundle->primal.mc_txn;
  assert(txn->mt_cursors != nullptr /* must be not rdonly txt */);
  aht_t *aht = bundle->primal.mc_aht;
  return &txn->mt_cursors[aht->ahe->ax_ord16];
}

static inline MDBX_comparer_t *cursor_key_comparer(const cursor_t *cursor) {
  return cursor->mc_aht->ahe->ax_kcmp;
}

static inline ptrdiff_t cursor_compare_keys(const cursor_t *cursor, const MDBX_iov_t *a, const MDBX_iov_t *b) {
  return cursor_key_comparer(cursor)(*a, *b);
}

static inline MDBX_comparer_t *cursor_data_comparer(const cursor_t *cursor) {
  return cursor->mc_aht->ahe->ax_dcmp;
}

static inline ptrdiff_t cursor_compare_data(const cursor_t *cursor, const MDBX_iov_t *a, const MDBX_iov_t *b) {
  return cursor_data_comparer(cursor)(*a, *b);
}

static inline bool cursor_is_core(const cursor_t *cursor) {
  return cursor->mc_aht <= aht_main(cursor->mc_txn);
}

static inline bool cursor_is_aah_valid(const MDBX_cursor_t *cursor) {
  if (unlikely(!aht_valid(cursor->primal.mc_aht)))
    return false;
  if (cursor->primal.mc_kind8 & S_HAVESUB) {
    if (unlikely(!aht_valid(&cursor->subcursor.mx_aht_body)))
      return false;
  }
  return true;
}

/* Initialize a cursor for a given transaction and database. */
static int cursor_init(MDBX_cursor_t *bundle, MDBX_txn_t *txn, aht_t *aht) {
  assert(aht->ah.state8 & MDBX_AAH_VALID);
  assert(aht->ahe != nullptr);

  set_signature(&bundle->mc_signature, MDBX_MC_SIGNATURE);
  bundle->mc_next = nullptr;
  bundle->mc_backup = nullptr;

  bundle->primal.mc_aht = aht;
  *(MDBX_txn_t **)&bundle->mc_base.txn = bundle->primal.mc_txn = txn;
  bundle->primal.mc_snum = 0;
  bundle->primal.mc_top = 0;
  bundle->primal.mc_pg[0] = 0;
  bundle->primal.mc_ki[0] = 0;
  bundle->primal.mc_kind_and_state = 0;
  bundle->subcursor.mx_cursor.mc_kind_and_state = 0;
  if (aht->aa.flags16 & MDBX_DUPSORT) {
    /* Initialize sorted-dups nested.
     *
     * Sorted duplicates are implemented as a sub-database for the given key.
     * The duplicate data items are actually keys of the sub-database.
     * Operations on the duplicate data items are performed using a sub-cursor
     * initialized when the sub-database is first accessed. This function does
     * the preliminary setup of the sub-cursor, filling in the fields that
     * depend only on the parent AA. */
    bundle->primal.mc_kind8 = (aht->aa.flags16 & MDBX_DUPFIXED) ? S_HAVESUB | S_DUPFIXED : S_HAVESUB;
    bundle->subcursor.mx_cursor.mc_kind8 = S_SUBCURSOR;
    bundle->subcursor.mx_ahe_body.ax_refcounter16 = 1;
    bundle->subcursor.mx_ahe_body.ax_flags16 = 0;
    bundle->subcursor.mx_ahe_body.ax_aah = UINT32_MAX;
    bundle->subcursor.mx_ahe_body.ax_kcmp = bundle->primal.mc_aht->ahe->ax_dcmp;
    bundle->subcursor.mx_ahe_body.ax_dcmp = nullptr;
    bundle->subcursor.mx_ahe_body.ax_since = 0;
    bundle->subcursor.mx_ahe_body.ax_until = MAX_TXNID;
    bundle->subcursor.mx_ahe_body.ax_ident.iov_len = 0;
    bundle->subcursor.mx_ahe_body.ax_ident.iov_base = nullptr;

    bundle->subcursor.mx_aht_body.ahe = &bundle->subcursor.mx_ahe_body;
    bundle->subcursor.mx_aht_body.ah.seq16 = bundle->subcursor.mx_ahe_body.ax_seqaah16;
    bundle->subcursor.mx_aht_body.ah.kind_and_state16 = MDBX_AAH_STALE | MDBX_AAH_DUPS;

    bundle->subcursor.mx_cursor.mc_txn = bundle->primal.mc_txn;
    bundle->subcursor.mx_cursor.mc_aht = &bundle->subcursor.mx_aht_body;
    bundle->subcursor.mx_cursor.mc_snum = 0;
    bundle->subcursor.mx_cursor.mc_top = 0;
  }

  assert(txn->mt_txnid >= txn->mt_env->me_oldest[0]);
  // if (unlikely(mc->mc_aht->ah.state8 == MDBX_AAH_STALE))
  //  return aa_fetch(txn, mc->mc_xa);
  assert(cursor_is_aah_valid(bundle));
  return MDBX_SUCCESS;
}

/* Final setup of a sorted-dups cursor.
 * Sets up the fields that depend on the data from the main cursor.
 * [in] mc The main cursor whose sorted-dups cursor is to be initialized.
 * [in] node The data containing the aatree_t record for the sorted-dup
 * database. */
static cursor_t *nested_setup(cursor_t *cursor, node_t *node) {
  subcur_t *subcursor = cursor_subcur(cursor);

  assert(cursor->mc_txn->mt_txnid >= cursor->mc_txn->mt_env->me_oldest[0]);
  if (node->node_flags8 & NODE_SUBTREE) {
    aa_db2txn(cursor->mc_txn->mt_env, (aatree_t *)NODEDATA(node), &subcursor->mx_aht_body, af_nested);
    subcursor->mx_cursor.mc_snum = 0;
    subcursor->mx_cursor.mc_pg[0] = nullptr;
  } else {
    page_t *fp = (page_t *)NODEDATA(node);
    subcursor->mx_aht_body.aa.flags16 = 0;
    subcursor->mx_aht_body.aa.depth16 = 1;
    subcursor->mx_aht_body.aa.xsize32 = 0;
    subcursor->mx_aht_body.aa.root = fp->mp_pgno;
    subcursor->mx_aht_body.aa.branch_pages = 0;
    subcursor->mx_aht_body.aa.leaf_pages = 1;
    subcursor->mx_aht_body.aa.overflow_pages = 0;
    subcursor->mx_aht_body.aa.genseq = INT64_MAX;
    subcursor->mx_aht_body.aa.entries = page_numkeys(fp);

    subcursor->mx_cursor.mc_snum = 1;
    subcursor->mx_cursor.mc_pg[0] = fp;
    subcursor->mx_cursor.mc_ki[0] = 0;
    if (cursor->mc_aht->aa.flags16 & MDBX_DUPFIXED) {
      subcursor->mx_aht_body.aa.flags16 =
          (cursor->mc_aht->aa.flags16 & MDBX_INTEGERDUP) ? MDBX_DUPFIXED | MDBX_INTEGERKEY : MDBX_DUPFIXED;
      subcursor->mx_aht_body.aa.xsize32 = fp->mp_leaf2_ksize16;
    }
  }
  mdbx_debug("setup-sub-cursor for %u root-page %" PRIaPGNO "", cursor->mc_aht->ahe->ax_ord16,
             subcursor->mx_aht_body.aa.root);

  subcursor->mx_aht_body.ah.kind_and_state16 = MDBX_AAH_VALID | MDBX_AAH_DUPS;
  subcursor->mx_cursor.mc_top = 0;
  subcursor->mx_cursor.mc_state8 = C_INITIALIZED;

  /* FIXME: #if UINT_MAX < SIZE_MAX
          if (mx->mx_dbx.aa_cmp == mdbx_cmp_int && mx->mx_db.aa_pad ==
  sizeof(size_t))
                  mx->mx_dbx.aa_cmp = mdbx_cmp_clong;
  #endif */
  return &subcursor->mx_cursor;
}

/* Fixup a sorted-dups cursor due to underlying update.
 * Sets up some fields that depend on the data from the main cursor.
 * Almost the same as init1, but skips initialization steps if the
 * xcursor had already been used.
 * [in] mc The main cursor whose sorted-dups cursor is to be fixed up.
 * [in] src_mx The xcursor of an up-to-date cursor.
 * [in] new_dupdata True if converting from a non-NODE_DUP item. */
static void subcursor_fixup(MDBX_cursor_t *dst, cursor_t *src, bool new_dupdata) {
  subcur_t *dst_sub = &dst->subcursor;
  subcur_t *src_sub = cursor_subcur(src);
  assert(dst->primal.mc_txn->mt_txnid >= dst->primal.mc_txn->mt_env->me_oldest[0]);

  if (new_dupdata) {
    dst_sub->mx_cursor.mc_snum = 1;
    dst_sub->mx_cursor.mc_top = 0;
    dst_sub->mx_cursor.mc_state8 |= C_INITIALIZED;
    dst_sub->mx_cursor.mc_ki[0] = 0;
    dst_sub->mx_aht_body.ah.state8 = MDBX_AAH_VALID /*| MDBX_AAH_USER | MDBX_AAH_DUPS*/;
    dst_sub->mx_ahe_body.ax_kcmp = src_sub->mx_ahe_body.ax_kcmp;
  } else if (!(dst_sub->mx_cursor.mc_state8 & C_INITIALIZED)) {
    return;
  }
  dst_sub->mx_aht_body.aa = src_sub->mx_aht_body.aa;
  dst_sub->mx_cursor.mc_pg[0] = src_sub->mx_cursor.mc_pg[0];
  mdbx_debug("fixup-sub-cursor for %u root-page %" PRIaPGNO "", dst->primal.mc_aht->ahe->ax_ord16,
             dst_sub->mx_aht_body.aa.root);
}

/* Copy the contents of a cursor.
 * [in] csrc The cursor to copy from.
 * [out] cdst The cursor to copy to. */
static void cursor_copy(const cursor_t *src, cursor_t *dst) {
  assert(src->mc_txn->mt_txnid >= src->mc_txn->mt_env->me_oldest[0]);
  dst->mc_txn = src->mc_txn;
  dst->mc_aht = src->mc_aht;
  dst->mc_snum = src->mc_snum;
  dst->mc_top = src->mc_top;
  dst->mc_kind_and_state = src->mc_kind_and_state;

  for (size_t i = 0; i < src->mc_snum; ++i) {
    dst->mc_pg[i] = src->mc_pg[i];
    dst->mc_ki[i] = src->mc_ki[i];
  }
}

static inline void cursor_copy_clearsub(const cursor_t *src, cursor_t *dst) {
  cursor_copy(src, dst);
  dst->mc_kind8 &= ~(S_HAVESUB | S_SUBCURSOR);
}

/* Back up parent txn's cursors, then grab the originals for tracking */
static int txn_shadow_cursors(MDBX_txn_t *src, MDBX_txn_t *dst) {
  for (size_t i = src->txn_ah_num; i > 0;) {
    MDBX_cursor_t *shadow = nullptr;
    for (MDBX_cursor_t *mc = src->mt_cursors[--i]; mc; mc = shadow->mc_next) {
      shadow = (MDBX_cursor_t *)malloc(sizeof(MDBX_cursor_t));
      if (unlikely(!shadow))
        return MDBX_ENOMEM;

      shadow->primal = mc->primal;
      if (mc->primal.mc_state8 & S_HAVESUB)
        shadow->subcursor = mc->subcursor;

      /* fixup aht-pointer to target txn's aht_array */
      mc->primal.mc_aht += dst->txn_aht_array - src->txn_aht_array;

      /* Kill pointers into src to reduce abuse: The
       * user may not use mc until dst ends. But we need a valid
       * txn pointer here for cursor fixups to keep working. */
      mc->primal.mc_txn = mc->subcursor.mx_cursor.mc_txn = dst;

      shadow->mc_backup = mc->mc_backup;
      shadow->mc_next = mc->mc_next;
      mc->mc_backup = shadow;
      mc->mc_next = dst->mt_cursors[i];
      dst->mt_cursors[i] = mc;
      set_signature(&shadow->mc_signature, MDBX_MC_BACKUP);
    }
  }
  return MDBX_SUCCESS;
}

static void cursor_unshadow(MDBX_cursor_t *mc, unsigned commit) {
  MDBX_cursor_t *shadow = mc->mc_backup;
  if (commit) {
    /* Commit changes to parent txn */
    mc->primal.mc_txn = mc->subcursor.mx_cursor.mc_txn = shadow->primal.mc_txn;
    mc->primal.mc_aht = shadow->primal.mc_aht;
  } else {
    mc->primal = shadow->primal;
    /* Rollback nested txn */
    if (mc->primal.mc_state8 & S_HAVESUB)
      mc->subcursor = shadow->subcursor;
  }
  if (mc->primal.mc_state8 & S_HAVESUB) {
    assert(mc->subcursor.mx_cursor.mc_aht == &mc->subcursor.mx_aht_body);
    assert(mc->subcursor.mx_cursor.mc_aht->ahe == &mc->subcursor.mx_ahe_body);
  }

  mc->mc_backup = shadow->mc_backup;
  mc->mc_next = shadow->mc_next;
  set_signature(&mc->mc_signature, ~0u);
  free(shadow);
}

/* Replace the key for a branch node with a new key.
 * Set MDBX_TXN_ERROR on failure.
 * [in] mc Cursor pointing to the node to operate on.
 * [in] key The new key to use.
 * Returns 0 on success, non-zero on failure. */
static int update_key(cursor_t *mc, MDBX_iov_t *key) {
  page_t *mp;
  node_t *node;
  char *base;
  size_t len;
  indx_t ptr, i, numkeys, indx;
  DKBUF;

  indx = mc->mc_ki[mc->mc_top];
  mp = mc->mc_pg[mc->mc_top];
  node = node_ptr(mp, indx);
  ptr = mp->mp_ptrs[indx];
  if (MDBX_DEBUG) {
    char kbuf2[DKBUF_MAXKEYSIZE * 2 + 1];
    MDBX_iov_t k2 = {NODEKEY(node), node->mn_ksize16};
    mdbx_debug("update key %u (ofs %u) [%s] to [%s] on page %" PRIaPGNO "", indx, ptr,
               mdbx_dump_iov(&k2, kbuf2, sizeof(kbuf2)), DKEY(key), mp->mp_pgno);
  }

  /* Sizes must be 2-byte aligned. */
  int keysize = EVEN(key->iov_len);
  int oksize = EVEN(node->mn_ksize16);
  int delta = keysize - oksize;

  /* Shift node contents if EVEN(key length) changed. */
  if (delta) {
    if (delta > 0 && page_spaceleft(mp) < delta) {
      /* not enough space left, do a delete and split */
      mdbx_debug("Not enough room, delta = %d, splitting...", delta);
      pgno_t pgno = node_get_pgno(node);
      node_del(mc, 0);
      return page_split(mc, key, nullptr, pgno, MDBX_SPLIT_REPLACE);
    }

    numkeys = page_numkeys(mp);
    for (i = 0; i < numkeys; i++) {
      if (mp->mp_ptrs[i] <= ptr) {
        assert(mp->mp_ptrs[i] >= delta);
        mp->mp_ptrs[i] -= (indx_t)delta;
      }
    }

    base = (char *)mp + mp->mp_upper + PAGEHDRSZ;
    len = ptr - mp->mp_upper + NODESIZE;
    memmove(base - delta, base, len);
    assert(mp->mp_upper >= delta);
    mp->mp_upper -= (indx_t)delta;

    node = node_ptr(mp, indx);
  }

  /* But even if no shift was needed, update keysize */
  if (node->mn_ksize16 != key->iov_len)
    node->mn_ksize16 = (uint16_t)key->iov_len;

  if (key->iov_len)
    memcpy(NODEKEY(node), key->iov_base, key->iov_len);

  return MDBX_SUCCESS;
}

/*----------------------------------------------------------------------------*/

/* Check if there is an inited xcursor, so cursor_refresh_subcursor() is proper
 */
static inline bool cursor_is_nested_inited(const MDBX_cursor_t *bundle) {
  return (bundle->primal.mc_kind8 & S_HAVESUB) && (bundle->subcursor.mx_cursor.mc_state8 & C_INITIALIZED);
}

/* Update the sub-cursor's sub-page pointer, if any, in mc.
 * Needed when the node which contains the sub-page may have moved.
 * Called with leaf page mp = mc->mc_pg[top]. */
static inline void cursor_refresh_subcursor(MDBX_cursor_t *bundle, const unsigned top, page_t *page) {
  assert(bundle->primal.mc_state8 & C_INITIALIZED);
  assert(top < bundle->primal.mc_snum);
  if (cursor_is_nested_inited(bundle) && bundle->primal.mc_ki[top] < page_numkeys(page)) {
    const node_t *node = node_ptr(page, bundle->primal.mc_ki[top]);
    if ((node->node_flags8 & (NODE_DUP | NODE_SUBTREE)) == NODE_DUP)
      bundle->subcursor.mx_cursor.mc_pg[0] = (page_t *)NODEDATA(node);
  }
}

/*----------------------------------------------------------------------------*/

/* Touch a page: make it dirty and re-insert into tree with updated pgno.
 * Set MDBX_TXN_ERROR on failure.
 *
 * [in] mc  cursor pointing to the page to be touched
 *
 * Returns 0 on success, non-zero on failure. */
static int page_touch(cursor_t *mc) {
  page_t *mp = mc->mc_pg[mc->mc_top], *np;
  MDBX_txn_t *txn = mc->mc_txn;
  pgno_t pgno;
  int rc;

  assert(!IS_OVERFLOW(mp));
  if (!F_ISSET(mp->mp_flags16, P_DIRTY)) {
    if (txn->mt_flags & MDBX_TXN_SPILLS) {
      np = nullptr;
      rc = page_unspill(txn, mp, &np);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
      if (likely(np))
        goto done;
    }

    rc = mdbx_pnl_need(&txn->mt_befree_pages, 1);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    rc = page_alloc(mc, 1, &np, MDBX_ALLOC_ALL);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;

    pgno = np->mp_pgno;
    mdbx_debug("touched db %d page %" PRIaPGNO " -> %" PRIaPGNO, DAAH(mc), mp->mp_pgno, pgno);
    assert(mp->mp_pgno != pgno);
    mdbx_pnl_xappend(txn->mt_befree_pages, mp->mp_pgno);
    /* Update the parent page, if any, to point to the new page */
    if (mc->mc_top) {
      page_t *parent = mc->mc_pg[mc->mc_top - 1];
      node_t *node = node_ptr(parent, mc->mc_ki[mc->mc_top - 1]);
      node_set_pgno(node, pgno);
    } else {
      mc->mc_aht->aa.root = pgno;
    }
  } else if (txn->mt_parent && !IS_SUBP(mp)) {
    mdbx_assert(txn->mt_env, (txn->mt_env->me_flags32 & MDBX_WRITEMAP) == 0);
    MDBX_ID2 mid, *dl = txn->mt_rw_dirtylist;
    pgno = mp->mp_pgno;
    /* If txn has a parent, make sure the page is in our dirty list. */
    if (dl[0].mid) {
      unsigned x = mdbx_mid2l_search(dl, pgno);
      if (x <= dl[0].mid && dl[x].mid == pgno) {
        if (unlikely(mp != dl[x].mptr)) { /* bad cursor? */
          mdbx_error("wrong page 0x%p #%" PRIaPGNO " in the dirtylist[%d], expecting %p", dl[x].mptr, pgno, x,
                     mp);
          mc->mc_state8 &= ~(C_INITIALIZED | C_EOF);
          txn->mt_flags |= MDBX_TXN_ERROR;
          return MDBX_PROBLEM;
        }
        return MDBX_SUCCESS;
      }
    }

    mdbx_debug("clone db %d page %" PRIaPGNO, DAAH(mc), mp->mp_pgno);
    assert(dl[0].mid < MDBX_PNL_UM_MAX);
    /* No - copy it */
    np = page_malloc(txn, 1);
    if (unlikely(!np))
      return MDBX_ENOMEM;
    mid.mid = pgno;
    mid.mptr = np;
    rc = mdbx_mid2l_insert(dl, &mid);
    assert(rc == 0);
  } else {
    return MDBX_SUCCESS;
  }

  page_copy(np, mp, txn->mt_env->me_psize);
  np->mp_pgno = pgno;
  np->mp_flags16 |= P_DIRTY;

done:
  /* Adjust cursors pointing to mp */
  mc->mc_pg[mc->mc_top] = np;
  for (MDBX_cursor_t *bundle = *cursor_tracking_head(cursor_bundle(mc)); bundle; bundle = bundle->mc_next) {
    cursor_t *scan = (mc->mc_kind8 & S_SUBCURSOR) ? &bundle->subcursor.mx_cursor : &bundle->primal;
    if (scan == mc)
      continue;
    if (mc->mc_top >= scan->mc_snum /* scan->mc_snum < mc->mc_snum */)
      continue;
    if (scan->mc_pg[mc->mc_top] != mp)
      continue;

    scan->mc_pg[mc->mc_top] = np;
    if (IS_LEAF(np))
      cursor_refresh_subcursor(bundle, mc->mc_top, np);
  }
  return MDBX_SUCCESS;

fail:
  txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

/*----------------------------------------------------------------------------*/

static inline size_t cursor_size(aht_t *aht) {
  size_t bytes = sizeof(MDBX_cursor_t);
  if (aht->aa.flags16 & MDBX_DUPSORT)
    bytes += sizeof(subcur_t);
  return bytes;
}

static int cursor_open(MDBX_txn_t *txn, aht_t *aht, MDBX_cursor_t *bundle) {
  int rc = cursor_init(bundle, txn, aht);
  if (likely(rc == MDBX_SUCCESS)) {
    if (txn->mt_cursors /* only for write txn */) {
      MDBX_cursor_t **head = cursor_tracking_head(bundle);
      bundle->mc_next = *head;
      *head = bundle;
      bundle->primal.mc_state8 |= C_UNTRACK;
    }
  }
  return rc;
}

static void cursor_untrack(MDBX_cursor_t *bundle) {
  assert(bundle->mc_backup == nullptr);
  /* Remove from txn, if tracked.
   * A read-only txn (!C_UNTRACK) may have been freed already,
   * so do not peek inside it.  Only write txns track cursors. */
  if (bundle->primal.mc_state8 & C_UNTRACK) {
    assert(bundle->primal.mc_txn->mt_signature == MDBX_MT_SIGNATURE);
    assert(bundle->primal.mc_txn->mt_cursors);
    MDBX_cursor_t **prev = cursor_tracking_head(bundle);
    while (*prev && *prev != bundle)
      prev = &(*prev)->mc_next;
    if (*prev == bundle)
      *prev = bundle->mc_next;
    bundle->primal.mc_state8 -= C_UNTRACK;
    set_signature(&bundle->mc_signature, MDBX_MC_READY4CLOSE);
  }
}

static int cursor_close(MDBX_cursor_t *bc) {
  if (!bc->mc_backup) {
    cursor_untrack(bc);
    set_signature(&bc->mc_signature, 0);
    return MDBX_SUCCESS;
  } else {
    /* cursor closed before nested txn ends */
    assert(bc->mc_signature == MDBX_MC_SIGNATURE);
    set_signature(&bc->mc_signature, MDBX_MC_WAIT4EOT);
    return MDBX_SIGN;
  }
}

/* Search for key within a page, using binary search.
 * Returns the smallest entry larger or equal to the key.
 * Updates the cursor index with the index of the found entry.
 * If no entry larger or equal to the key is found, returns nullptr. */
static inline node_rc_t node_search(cursor_t *mc, MDBX_iov_t key) {
  page_t *mp = mc->mc_pg[mc->mc_top];
  assert(page_numkeys(mp) > 0);
  int low = IS_LEAF(mp) ? 0 : 1;
  int high = page_numkeys(mp) - 1;
  return node_search_hilo(mc, key, low, high);
}

static __hot node_rc_t node_search_hilo(cursor_t *mc, MDBX_iov_t key, int low, int high) {
  DKBUF;
  page_t *mp = mc->mc_pg[mc->mc_top];
  const int nkeys = page_numkeys(mp);
  mdbx_debug("searching %u keys in %s %spage %" PRIaPGNO "", nkeys, IS_LEAF(mp) ? "leaf" : "branch",
             IS_SUBP(mp) ? "sub-" : "", mp->mp_pgno);

  MDBX_comparer_t *comparer = cursor_key_comparer(mc);
#if !UNALIGNED_OK || MDBX_DEBUG
  /* Branch pages have no data, so if using integer keys,
   * alignment is guaranteed. Use faster mdbx_cmp_int_ai. */
  if (comparer == cmp_int_aligned_to2 && IS_BRANCH(mp))
    comparer = cmp_int_aligned;
#endif

  assert(low >= 0 && low < nkeys);
  int i = low;
  ptrdiff_t cmp = -1;
  if (likely(!IS_DFL(mp))) {
    while (low <= high) {
      i = (low + high) >> 1;
      node_t *node = node_ptr(mp, i);
      MDBX_iov_t nodekey;
      nodekey.iov_len = node_get_keysize(node);
      nodekey.iov_base = NODEKEY(node);

      cmp = comparer(key, nodekey);
      if (IS_LEAF(mp))
        mdbx_debug("key %c leaf index %u [%s]", cmp2char(cmp), i, DKEY(&nodekey));
      else
        mdbx_debug("key %c branch index %u [%s -> %" PRIaPGNO "]", cmp2char(cmp), i, DKEY(&nodekey),
                   node_get_pgno(node));
      if (unlikely(cmp == 0))
        break;
      const int inc_i = i + 1;
      const int dec_i = i - 1;
      high = (cmp > 0) ? high : dec_i;
      i = (cmp > 0) ? inc_i : i;
      low = (cmp > 0) ? inc_i : low;
    }
  } else {
    while (low <= high) {
      i = (low + high) >> 1;
      const MDBX_iov_t nodekey = {DFLKEY(mp, i, mc->mc_aht->aa.xsize32), mc->mc_aht->aa.xsize32};
      cmp = comparer(key, nodekey);
      mdbx_debug("key %c leaf index %u [%s]", cmp2char(cmp), i, DKEY(&nodekey));
      if (unlikely(cmp == 0))
        break;

      const int inc_i = i + 1;
      const int dec_i = i - 1;
      high = (cmp > 0) ? high : dec_i;
      i = (cmp > 0) ? inc_i : i;
      low = (cmp > 0) ? inc_i : low;
    }
  }

  /* store the key index */
  assert(i <= UINT16_MAX);
  mc->mc_ki[mc->mc_top] = (indx_t)i;

  node_rc_t result;
  result.node = nullptr;
  result.exact = false;

  if (likely(i < nkeys)) {
    result.exact = (cmp == 0);
    /* node is fake for DFL */
    result.node = node_ptr(mp, unlikely(IS_DFL(mp)) ? /* fake */ 0 : i);
  } else {
    /* There is no entry larger or equal to the key. */
  }
  return result;
}

#if 0 /* unused for now */
static void cursor_adjust(MDBX_cursor_t *mc, func) {
  MDBX_cursor_t *m2;

  for (m2 = mc->mc_txn->mt_cursors[mc->mc_ah]; m2; m2 = m2->mc_next) {
    if (m2->mc_pg[m2->mc_top] == mc->mc_pg[mc->mc_top]) {
      func(mc, m2);
    }
  }
}
#endif

/* Pop a page off the top of the cursor's stack. */
static void cursor_pop(cursor_t *mc) {
  if (mc->mc_snum) {
    mdbx_debug("popped page %" PRIaPGNO " off db %d cursor %p", mc->mc_pg[mc->mc_top]->mp_pgno, DAAH(mc),
               (void *)mc);

    mc->mc_snum--;
    if (mc->mc_snum) {
      mc->mc_top--;
    } else {
      mc->mc_state8 &= ~C_INITIALIZED;
    }
  }
}

/* Push a page onto the top of the cursor's stack.
 * Set MDBX_TXN_ERROR on failure. */
static int cursor_push(cursor_t *mc, page_t *mp) {
  mdbx_debug("pushing page %" PRIaPGNO " on db %d cursor %p", mp->mp_pgno, DAAH(mc), (void *)mc);

  if (unlikely(mc->mc_snum >= CURSOR_STACK)) {
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_CURSOR_FULL;
  }

  assert(mc->mc_snum < UINT16_MAX);
  mc->mc_top = mc->mc_snum++;
  mc->mc_pg[mc->mc_top] = mp;
  mc->mc_ki[mc->mc_top] = 0;

  return MDBX_SUCCESS;
}

/* Find the address of the page corresponding to a given page number.
 * Set MDBX_TXN_ERROR on failure.
 *
 * [in] mc    the cursor accessing the page.
 * [in] pgno  the page number for the page to retrieve.
 * [out] ret  address of a pointer where the page's address will be
 *            stored.
 * [out] lvl  dirtylist inheritance level of found page. 1=current txn,
 *            0=mapped page.
 *
 * Returns 0 on success, non-zero on failure. */
static int page_get(MDBX_txn_t *const txn, pgno_t pgno, page_t **ret, int *lvl) {
  MDBX_env_t *env = txn->mt_env;
  page_t *p = nullptr;
  int level;

  if (!(txn->mt_flags & (MDBX_RDONLY | MDBX_WRITEMAP))) {
    MDBX_txn_t *tx2 = txn;
    level = 1;
    do {
      MDBX_ID2L dl = tx2->mt_rw_dirtylist;
      /* Spilled pages were dirtied in this txn and flushed
       * because the dirty list got full. Bring this page
       * back in from the map (but don't unspill it here,
       * leave that unless page_touch happens again). */
      if (tx2->mt_spill_pages) {
        const pgno_t pgno2 = pgno << 1;
        const size_t pos = mdbx_pnl_search(tx2->mt_spill_pages, pgno2);
        if (pos <= tx2->mt_spill_pages[0] && tx2->mt_spill_pages[pos] == pgno2)
          goto mapped;
      }
      if (dl[0].mid) {
        unsigned y = mdbx_mid2l_search(dl, pgno);
        if (y <= dl[0].mid && dl[y].mid == pgno) {
          p = dl[y].mptr;
          goto done;
        }
      }
      level++;
    } while ((tx2 = tx2->mt_parent) != nullptr);
  }

  if (unlikely(pgno >= txn->mt_next_pgno)) {
    mdbx_debug("page %" PRIaPGNO " not found", pgno);
    txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_PAGE_NOTFOUND;
  }
  level = 0;

mapped:
  p = pgno2page(env, pgno);
/* TODO: check p->page_checksum here */

done:
  *ret = p;
  if (lvl)
    *lvl = level;
  return MDBX_SUCCESS;
}

/* Finish page_search() / page_search_lowest().
 * The cursor is at the root page, set up the rest of it. */
static int page_search_root(cursor_t *mc, MDBX_iov_t *key, int flags) {
  page_t *mp = mc->mc_pg[mc->mc_top];
  int rc;
  DKBUF;

  while (IS_BRANCH(mp)) {
    node_t *node;
    indx_t i;

    mdbx_debug("branch page %" PRIaPGNO " has %u keys", mp->mp_pgno, page_numkeys(mp));
    if (mc->mc_aht != aht_gaco(mc->mc_txn)) {
      /* Don't assert on branch pages in the GACO. We can get here
       * while in the process of rebalancing a GACO branch page; we must
       * let that proceed. ITS#8336 */
      assert(page_numkeys(mp) > 1);
    }
    mdbx_debug("found index 0 to page %" PRIaPGNO "", node_get_pgno(node_ptr(mp, 0)));

    if (flags & (MDBX_PS_FIRST | MDBX_PS_LAST)) {
      i = 0;
      if (flags & MDBX_PS_LAST) {
        i = page_numkeys(mp) - 1;
        /* if already init'd, see if we're already in right place */
        if (mc->mc_state8 & C_INITIALIZED) {
          if (mc->mc_ki[mc->mc_top] == i) {
            mc->mc_top = mc->mc_snum++;
            mp = mc->mc_pg[mc->mc_top];
            goto ready;
          }
        }
      }
    } else {
      node_rc_t rp = node_search(mc, *key);
      if (rp.node == nullptr) {
        assert(rp.exact == 0);
        i = page_numkeys(mp);
      } else {
        assert(rp.exact == 0 || rp.exact == 1);
        i = mc->mc_ki[mc->mc_top];
      }
      assert(rp.exact || i > 0);
      i = i + rp.exact - 1;
      mdbx_debug("following index %u for key [%s]", i, DKEY(key));
    }

    assert(i < page_numkeys(mp));
    node = node_ptr(mp, i);

    rc = page_get(mc->mc_txn, node_get_pgno(node), &mp, nullptr);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    mc->mc_ki[mc->mc_top] = i;
    rc = cursor_push(mc, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

  ready:
    if (flags & MDBX_PS_MODIFY) {
      rc = page_touch(mc);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      mp = mc->mc_pg[mc->mc_top];
    }
  }

  if (unlikely(!IS_LEAF(mp))) {
    mdbx_debug("internal error, index points to a page with 0x%02x flags!?", mp->mp_flags16);
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_CORRUPTED;
  }

  mdbx_debug("found leaf page %" PRIaPGNO " for key [%s]", mp->mp_pgno, DKEY(key));
  mc->mc_state8 |= C_INITIALIZED;
  mc->mc_state8 &= ~C_EOF;

  return MDBX_SUCCESS;
}

/* Search for the lowest key under the current branch page.
 * This just bypasses a page_numkeys check in the current page
 * before calling page_search_root(), because the callers
 * are all in situations where the current page is known to
 * be underfilled. */
static int page_search_lowest(cursor_t *mc) {
  page_t *mp = mc->mc_pg[mc->mc_top];
  node_t *node = node_ptr(mp, 0);
  int rc = page_get(mc->mc_txn, node_get_pgno(node), &mp, nullptr);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  mc->mc_ki[mc->mc_top] = 0;
  rc = cursor_push(mc, mp);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return page_search_root(mc, nullptr, MDBX_PS_FIRST);
}

/* Search for the page a given key should be in.
 * Push it and its parent pages on the cursor stack.
 *
 * [in,out] mc  the cursor for this operation.
 * [in] key     the key to search for, or nullptr for first/last page.
 * [in] flags   If MDBX_PS_MODIFY is set, visited pages in the databook
 *              are touched (updated with new page numbers).
 *              If MDBX_PS_FIRST or MDBX_PS_LAST is set, find first or last
 * leaf.
 *              This is used by mdbx_cr_first() and mdbx_cr_last().
 *              If MDBX_PS_ROOTONLY set, just fetch root node, no further
 *              lookups.
 *
 * Returns 0 on success, non-zero on failure. */
static int page_search(cursor_t *mc, MDBX_iov_t *key, int flags) {
  /* Make sure the txn is still viable, then find the root from
   * the txn's db table and set it as the root of the cursor's stack. */
  if (unlikely(mc->mc_txn->mt_flags & MDBX_TXN_BLOCKED)) {
    mdbx_debug("transaction has failed, must abort");
    return MDBX_BAD_TXN;
  }

  assert(mc->mc_txn->mt_txnid >= mc->mc_txn->mt_env->me_oldest[0]);
  /* Make sure we're using an up-to-date root */
  assert(mc->mc_aht->ah.state8 & MDBX_AAH_VALID);
  pgno_t root = mc->mc_aht->aa.root;

  if (unlikely(root == P_INVALID)) { /* Tree is empty. */
    mdbx_debug("tree is empty");
    return MDBX_NOTFOUND;
  }

  assert(mc->mc_txn->mt_txnid >= mc->mc_txn->mt_env->me_oldest[0]);
  assert(root >= NUM_METAS);
  if (!mc->mc_pg[0] || mc->mc_pg[0]->mp_pgno != root) {
    int rc = page_get(mc->mc_txn, root, &mc->mc_pg[0], nullptr);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  mc->mc_snum = 1;
  mc->mc_top = 0;

  mdbx_debug("db %d root page %" PRIaPGNO " has flags 0x%X", DAAH(mc), root, mc->mc_pg[0]->mp_flags16);

  if (flags & MDBX_PS_MODIFY) {
    int rc = page_touch(mc);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  if (flags & MDBX_PS_ROOTONLY)
    return MDBX_SUCCESS;

  return page_search_root(mc, key, flags);
}

static int ovpage_free(cursor_t *mc, page_t *mp) {
  MDBX_txn_t *const txn = mc->mc_txn;
  MDBX_env_t *const env = txn->mt_env;
  unsigned const npages = mp->mp_pages;
  MDBX_PNL const spill_list = txn->mt_spill_pages;
  pgno_t pgno = mp->mp_pgno;
  const pgno_t pgno2 = pgno << 1;
  int rc;

  mdbx_debug("free ov page %" PRIaPGNO " (%u)", pgno, npages);
  /* If the page is dirty or on the spill list we just acquired it,
   * so we should give it back to our current free list, if any.
   * Otherwise put it onto the list of pages we freed in this txn.
   *
   * Won't create me_reclaimed_pglist: me_last_reclaimed must be inited along
   * with it.
   * Unsupported in nested txns: They would need to hide the page
   * range in ancestor txns' dirty and spilled lists. */
  size_t spill_pos = 0;
  if (env->me_reclaimed_pglist && !txn->mt_parent &&
      ((mp->mp_flags16 & P_DIRTY) ||
       (spill_list && (spill_pos = mdbx_pnl_search(spill_list, pgno2)) <= spill_list[0] &&
        spill_list[spill_pos] == pgno2))) {

    rc = mdbx_pnl_need(&env->me_reclaimed_pglist, npages);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    if (!(mp->mp_flags16 & P_DIRTY)) {
      /* This page is no longer spilled */
      if (spill_pos == spill_list[0])
        spill_list[0]--;
      else
        spill_list[spill_pos] |= 1;
      goto release;
    }

    /* Remove from dirty list */
    MDBX_ID2 *dl = txn->mt_rw_dirtylist;
    size_t last = dl[0].mid--;
    for (MDBX_ID2 iy, ix = dl[last]; ix.mptr != mp; ix = iy) {
      if (likely(last > 1)) {
        last--;
        iy = dl[last];
        dl[last] = ix;
      } else {
        mdbx_error("not found page 0x%p #%" PRIaPGNO " in the dirtylist", mp, mp->mp_pgno);
        dl[dl[0].mid += 1] = ix; /* Unsorted. OK when MDBX_TXN_ERROR. */
        txn->mt_flags |= MDBX_TXN_ERROR;
        return MDBX_PROBLEM;
      }
    }
    txn->mt_dirtyroom++;
    if (!(env->me_flags32 & MDBX_WRITEMAP))
      dpage_free(env, mp);
  release:;
    /* Insert in me_reclaimed_pglist */
    pgno_t *mop = env->me_reclaimed_pglist;
    unsigned i, j;
    for (i = mop[0], j = mop[0] + npages; i && mop[i] < pgno; i--)
      mop[j--] = mop[i];
    while (j > i)
      mop[j--] = pgno++;
    mop[0] += npages;
  } else {
    rc = mdbx_pnl_append_range(&txn->mt_befree_pages, pgno, npages);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  mc->mc_aht->aa.overflow_pages -= npages;
  return 0;
}

/* Return the data associated with a given node.
 *
 * [in] mc      The cursor for this operation.
 * [in] leaf    The node being read.
 * [out] data   Updated to point to the node's data.
 *
 * Returns 0 on success, non-zero on failure. */
static inline int node_read(cursor_t *mc, node_t *leaf, MDBX_iov_t *data) {
  assert(mc->mc_txn->mt_txnid >= mc->mc_txn->mt_env->me_oldest[0]);
  if (likely(!(leaf->node_flags8 & NODE_BIG))) {
    data->iov_len = node_get_datasize(leaf);
    data->iov_base = NODEDATA(leaf);
    return MDBX_SUCCESS;
  }

  /* Read overflow data. */
  data->iov_len = node_get_datasize(leaf);
  pgno_t pgno = get_pgno_aligned2(NODEDATA(leaf));
  page_t *omp; /* overflow page */
  int rc = page_get(mc->mc_txn, pgno, &omp, nullptr);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_debug("read overflow page %" PRIaPGNO " failed", pgno);
    return rc;
  }
  data->iov_base = page_data(omp);
  return MDBX_SUCCESS;
}

/* Find a sibling for a page.
 * Replaces the page at the top of the cursor's stack with the specified
 * sibling, if one exists.
 *
 * [in] mc          The cursor for this operation.
 * [in] move_right  Non-zero if the right sibling is requested,
 *                  otherwise the left sibling.
 *
 * Returns 0 on success, non-zero on failure. */
static int cursor_sibling(cursor_t *mc, bool move_right) {
  int rc;
  node_t *indx;
  page_t *mp;

  assert(mc->mc_txn->mt_txnid >= mc->mc_txn->mt_env->me_oldest[0]);
  if (unlikely(mc->mc_snum < 2)) {
    return MDBX_NOTFOUND; /* root has no siblings */
  }

  cursor_pop(mc);
  mdbx_debug("parent page is page %" PRIaPGNO ", index %u", mc->mc_pg[mc->mc_top]->mp_pgno,
             mc->mc_ki[mc->mc_top]);

  if (move_right ? (mc->mc_ki[mc->mc_top] + 1u >= page_numkeys(mc->mc_pg[mc->mc_top]))
                 : (mc->mc_ki[mc->mc_top] == 0)) {
    mdbx_debug("no more keys left, moving to %s sibling", move_right ? "right" : "left");
    if (unlikely((rc = cursor_sibling(mc, move_right)) != MDBX_SUCCESS)) {
      /* undo cursor_pop before returning */
      mc->mc_top++;
      mc->mc_snum++;
      return rc;
    }
  } else {
    if (move_right)
      mc->mc_ki[mc->mc_top]++;
    else
      mc->mc_ki[mc->mc_top]--;
    mdbx_debug("just moving to %s index key %u", move_right ? "right" : "left", mc->mc_ki[mc->mc_top]);
  }
  assert(IS_BRANCH(mc->mc_pg[mc->mc_top]));

  indx = node_ptr(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
  if (unlikely((rc = page_get(mc->mc_txn, node_get_pgno(indx), &mp, nullptr)) != 0)) {
    /* mc will be inconsistent if caller does mc_snum++ as above */
    mc->mc_state8 &= ~(C_INITIALIZED | C_EOF);
    return rc;
  }

  cursor_push(mc, mp);
  if (!move_right)
    mc->mc_ki[mc->mc_top] = page_numkeys(mp) - 1;

  return MDBX_SUCCESS;
}

/* Move the cursor to the next data item. */
static int cursor_next(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_cursor_op_t op) {
  page_t *mp;
  node_t *leaf;
  int rc;

  if ((mc->mc_state8 & C_AFTERDELETE) && op == MDBX_NEXT_DUP)
    return MDBX_NOTFOUND;

  if (!(mc->mc_state8 & C_INITIALIZED))
    return cursor_first(mc, key, data);

  mp = mc->mc_pg[mc->mc_top];
  if (mc->mc_state8 & C_EOF) {
    if (mc->mc_ki[mc->mc_top] + 1u >= page_numkeys(mp))
      return MDBX_NOTFOUND;
    mc->mc_state8 ^= C_EOF;
  }

  if (mc->mc_kind8 & S_HAVESUB) {
    leaf = node_ptr(mp, mc->mc_ki[mc->mc_top]);
    if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
      if (op == MDBX_NEXT || op == MDBX_NEXT_DUP) {
        rc = cursor_next(&cursor_subcur(mc)->mx_cursor, data, nullptr, MDBX_NEXT);
        if (op != MDBX_NEXT || rc != MDBX_NOTFOUND) {
          if (likely(rc == MDBX_SUCCESS))
            MDBX_GET_KEY(leaf, key);
          return rc;
        }
      }
    } else {
      cursor_subcur(mc)->mx_cursor.mc_state8 &= ~(C_INITIALIZED | C_EOF);
      if (op == MDBX_NEXT_DUP)
        return MDBX_NOTFOUND;
    }
  }

  mdbx_debug("cursor_next: top page is %" PRIaPGNO " in cursor %p", mp->mp_pgno, (void *)mc);
  if (mc->mc_state8 & C_AFTERDELETE) {
    mc->mc_state8 ^= C_AFTERDELETE;
    goto skip;
  }

  if (mc->mc_ki[mc->mc_top] + 1u >= page_numkeys(mp)) {
    mdbx_debug("=====> move to next sibling page");
    if (unlikely((rc = cursor_sibling(mc, 1)) != MDBX_SUCCESS)) {
      mc->mc_state8 |= C_EOF;
      return rc;
    }
    mp = mc->mc_pg[mc->mc_top];
    mdbx_debug("next page is %" PRIaPGNO ", key index %u", mp->mp_pgno, mc->mc_ki[mc->mc_top]);
  } else
    mc->mc_ki[mc->mc_top]++;

skip:
  mdbx_debug("==> cursor points to page %" PRIaPGNO " with %u keys, key index %u", mp->mp_pgno,
             page_numkeys(mp), mc->mc_ki[mc->mc_top]);

  if (IS_DFL(mp)) {
    key->iov_len = mc->mc_aht->aa.xsize32;
    key->iov_base = DFLKEY(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    return MDBX_SUCCESS;
  }

  assert(IS_LEAF(mp));
  leaf = node_ptr(mp, mc->mc_ki[mc->mc_top]);

  if (F_ISSET(leaf->node_flags8, NODE_DUP))
    nested_setup(mc, leaf);

  if (data) {
    if (unlikely((rc = node_read(mc, leaf, data)) != MDBX_SUCCESS))
      return rc;

    if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
      rc = cursor_first(&cursor_subcur(mc)->mx_cursor, data, nullptr);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
  }

  MDBX_GET_KEY(leaf, key);
  return MDBX_SUCCESS;
}

/* Move the cursor to the previous data item. */
static int cursor_prev(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_cursor_op_t op) {
  page_t *mp;
  node_t *leaf;
  int rc;

  if ((mc->mc_state8 & C_AFTERDELETE) && op == MDBX_PREV_DUP)
    return MDBX_NOTFOUND;

  if (!(mc->mc_state8 & C_INITIALIZED)) {
    rc = cursor_last(mc, key, data);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mc->mc_ki[mc->mc_top]++;
  }

  mp = mc->mc_pg[mc->mc_top];
  if ((mc->mc_kind8 & S_HAVESUB) && mc->mc_ki[mc->mc_top] < page_numkeys(mp)) {
    leaf = node_ptr(mp, mc->mc_ki[mc->mc_top]);
    if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
      if (op == MDBX_PREV || op == MDBX_PREV_DUP) {
        rc = cursor_prev(&cursor_subcur(mc)->mx_cursor, data, nullptr, MDBX_PREV);
        if (op != MDBX_PREV || rc != MDBX_NOTFOUND) {
          if (likely(rc == MDBX_SUCCESS)) {
            MDBX_GET_KEY(leaf, key);
            mc->mc_state8 &= ~C_EOF;
          }
          return rc;
        }
      }
    } else {
      cursor_subcur(mc)->mx_cursor.mc_state8 &= ~(C_INITIALIZED | C_EOF);
      if (op == MDBX_PREV_DUP)
        return MDBX_NOTFOUND;
    }
  }

  mdbx_debug("cursor_prev: top page is %" PRIaPGNO " in cursor %p", mp->mp_pgno, (void *)mc);

  mc->mc_state8 &= ~(C_EOF | C_AFTERDELETE);

  if (mc->mc_ki[mc->mc_top] == 0) {
    mdbx_debug("=====> move to prev sibling page");
    if ((rc = cursor_sibling(mc, 0)) != MDBX_SUCCESS) {
      return rc;
    }
    mp = mc->mc_pg[mc->mc_top];
    mc->mc_ki[mc->mc_top] = page_numkeys(mp) - 1;
    mdbx_debug("prev page is %" PRIaPGNO ", key index %u", mp->mp_pgno, mc->mc_ki[mc->mc_top]);
  } else
    mc->mc_ki[mc->mc_top]--;

  mdbx_debug("==> cursor points to page %" PRIaPGNO " with %u keys, key index %u", mp->mp_pgno,
             page_numkeys(mp), mc->mc_ki[mc->mc_top]);

  if (IS_DFL(mp)) {
    key->iov_len = mc->mc_aht->aa.xsize32;
    key->iov_base = DFLKEY(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    return MDBX_SUCCESS;
  }

  assert(IS_LEAF(mp));
  leaf = node_ptr(mp, mc->mc_ki[mc->mc_top]);

  if (F_ISSET(leaf->node_flags8, NODE_DUP))
    nested_setup(mc, leaf);

  if (data) {
    if (unlikely((rc = node_read(mc, leaf, data)) != MDBX_SUCCESS))
      return rc;

    if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
      rc = cursor_last(&cursor_subcur(mc)->mx_cursor, data, nullptr);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
  }

  MDBX_GET_KEY(leaf, key);
  return MDBX_SUCCESS;
}

/* Set the cursor on a specific data item. */
static int cursor_set(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_cursor_op_t op, int *exactp) {
  DKBUF;
  assert(mc->mc_txn->mt_txnid >= mc->mc_txn->mt_env->me_oldest[0]);
  if ((mc->mc_aht->aa.flags16 & MDBX_INTEGERKEY) &&
      unlikely(key->iov_len != sizeof(uint32_t) && key->iov_len != sizeof(uint64_t))) {
    assert(!"key-size is invalid for MDBX_INTEGERKEY");
    return MDBX_BAD_VALSIZE;
  }

  if (mc->mc_kind8 & S_HAVESUB)
    cursor_subcur(mc)->mx_cursor.mc_state8 &= ~(C_INITIALIZED | C_EOF);

  int low_on_page, high_on_page;
  page_t *mp;

  node_rc_t rp;
  rp.node = nullptr;
  rp.exact = false;

  /* See if we're already on the right page */
  if (likely(mc->mc_state8 & C_INITIALIZED)) {
    mp = mc->mc_pg[mc->mc_top];
    assert(IS_LEAF(mp));

    const unsigned nkeys = page_numkeys(mp);
    if (unlikely(nkeys == 0)) {
      /* page is empty */
      assert(mc->mc_top == 0 && mc->mc_ki[mc->mc_top] == 0);
      /* paranoia: mc->mc_ki[mc->mc_top] = 0; */
      return MDBX_NOTFOUND;
    }

    MDBX_iov_t nodekey;
    if (unlikely(mp->mp_flags16 & P_DFL)) {
      nodekey.iov_len = mc->mc_aht->aa.xsize32;
      nodekey.iov_base = DFLKEY(mp, 0, nodekey.iov_len);
    } else {
      rp.node = node_ptr(mp, 0);
      MDBX_GET_KEY2(rp.node, nodekey);
    }

    const ptrdiff_t cmp_first_on_page = cursor_compare_keys(mc, key, &nodekey);
    if (cmp_first_on_page > 0 /* key > first_node */) {
      if (likely(nkeys > 1)) {
        low_on_page = 1;
        high_on_page = nkeys - 1;
        if (unlikely(mp->mp_flags16 & P_DFL)) {
          nodekey.iov_base = DFLKEY(mp, high_on_page, nodekey.iov_len);
        } else {
          rp.node = node_ptr(mp, high_on_page);
          MDBX_GET_KEY2(rp.node, nodekey);
        }
        const ptrdiff_t cmp_last_on_page = cursor_compare_keys(mc, key, &nodekey);
        if (cmp_last_on_page < 0 /* key < last_node */) {
          /* This is definitely the right page, skip search_page */
          const int current = mc->mc_ki[mc->mc_top];
          if (current < high_on_page) {
            if (unlikely(mp->mp_flags16 & P_DFL)) {
              nodekey.iov_base = DFLKEY(mp, current, nodekey.iov_len);
            } else {
              rp.node = node_ptr(mp, current);
              MDBX_GET_KEY2(rp.node, nodekey);
            }
            const ptrdiff_t cmp_current = cursor_compare_keys(mc, key, &nodekey);
            if (cmp_current > 0 /* key > current_node */) {
              low_on_page = current + 1;
            } else if (cmp_current < 0 /* key < current_node */) {
              high_on_page = current - 1;
            } else /* cmp_current == 0 */ {
              /* current node was the one we wanted */
              rp.exact = true;
              goto found;
            }
          }
          mc->mc_state8 &= ~C_EOF;
          goto search_on_page;
        } else if (unlikely(cmp_last_on_page == 0)) {
          /* last node was the one we wanted. */
          mc->mc_ki[mc->mc_top] = (indx_t)(nkeys - 1);
          rp.exact = true;
          goto found;
        }
      }

      /* If any parents have right-sibs, search.
       * Otherwise, there's nothing further. */
      unsigned i;
      for (i = 0; i < mc->mc_top; i++)
        if (mc->mc_ki[i] < page_numkeys(mc->mc_pg[i]) - 1)
          break;
      if (i == mc->mc_top) {
        /* There are no other pages */
        mc->mc_ki[mc->mc_top] = (uint16_t)nkeys;
        return MDBX_NOTFOUND;
      }
    } else if (unlikely(cmp_first_on_page == 0)) {
      /* Happens rarely, but first node on the page was the one we wanted. */
      mc->mc_ki[mc->mc_top] = 0;
      rp.exact = true;
      goto found;
    }

    if (!mc->mc_top) {
      /* There are no other pages */
      mc->mc_ki[mc->mc_top] = 0;
      if (op == MDBX_SET_RANGE && !exactp)
        goto found;
      return MDBX_NOTFOUND;
    }
  } else {
    mc->mc_pg[0] = 0;
  }

  do {
    const int err = page_search(mc, key, 0);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    mp = mc->mc_pg[mc->mc_top];
    assert(IS_LEAF(mp));
    low_on_page = 0;
    high_on_page = page_numkeys(mp) - 1;
  } while (0);

search_on_page:;
  rp = node_search_hilo(mc, *key, low_on_page, high_on_page);
  if (exactp != nullptr && rp.exact == 0)
    return MDBX_NOTFOUND /* MDBX_SET specified and not an exact match. */;

  if (rp.node == nullptr) {
    mdbx_debug("===> inexact leaf not found, goto sibling");
    int rc = cursor_sibling(mc, 1);
    if (unlikely(rc != MDBX_SUCCESS)) {
      mc->mc_state8 |= C_EOF;
      return rc; /* no entries matched */
    }
    mp = mc->mc_pg[mc->mc_top];
    assert(IS_LEAF(mp));
    rp.node = node_ptr(mp, 0);
  }

found:
  mc->mc_state8 |= C_INITIALIZED;
  mc->mc_state8 &= ~C_EOF;
  if (exactp != nullptr)
    *exactp = rp.exact;

  if (unlikely(IS_DFL(mp))) {
    if (op == MDBX_SET_RANGE || op == MDBX_SET_KEY) {
      key->iov_len = mc->mc_aht->aa.xsize32;
      key->iov_base = DFLKEY(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    }
    goto done;
  }

  if (F_ISSET(rp.node->node_flags8, NODE_DUP))
    nested_setup(mc, rp.node);

  if (likely(data)) {
    if (F_ISSET(rp.node->node_flags8, NODE_DUP)) {
      if (op == MDBX_SET || op == MDBX_SET_KEY || op == MDBX_SET_RANGE) {
        const int err = cursor_first(&cursor_subcur(mc)->mx_cursor, data, nullptr);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      } else {
        int exact2_unused = 0;
        const int err = cursor_set(&cursor_subcur(mc)->mx_cursor, data, nullptr, MDBX_SET_RANGE,
                                   (op == MDBX_GET_BOTH) ? &exact2_unused : nullptr);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }
    } else if (op == MDBX_GET_BOTH || op == MDBX_GET_BOTH_RANGE) {
      MDBX_iov_t olddata;
      const int err = node_read(mc, rp.node, &olddata);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      const ptrdiff_t cmp = cursor_compare_data(mc, data, &olddata);
      if (cmp) {
        if (op == MDBX_GET_BOTH || cmp > 0)
          return MDBX_NOTFOUND;
      }
      *data = olddata;
    } else {
      if (mc->mc_kind8 & S_HAVESUB)
        cursor_subcur(mc)->mx_cursor.mc_state8 &= ~(C_INITIALIZED | C_EOF);
      const int err = node_read(mc, rp.node, data);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }
  }

  /* The key already matches in all other cases */
  if (op == MDBX_SET_RANGE || op == MDBX_SET_KEY)
    MDBX_GET_KEY(rp.node, key);

done:
  mdbx_debug("==> cursor placed on key [%s], data [%s]", DKEY(key), DVAL(data));
  return MDBX_SUCCESS;
}

/* Move the cursor to the first item in the database. */
static int cursor_first(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data) {
  if (mc->mc_kind8 & S_HAVESUB)
    cursor_subcur(mc)->mx_cursor.mc_state8 &= ~(C_INITIALIZED | C_EOF);

  if (!(mc->mc_state8 & C_INITIALIZED) || mc->mc_top) {
    int rc = page_search(mc, nullptr, MDBX_PS_FIRST);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  assert(IS_LEAF(mc->mc_pg[mc->mc_top]));

  node_t *leaf = node_ptr(mc->mc_pg[mc->mc_top], 0);
  mc->mc_state8 |= C_INITIALIZED;
  mc->mc_state8 &= ~C_EOF;

  mc->mc_ki[mc->mc_top] = 0;

  if (IS_DFL(mc->mc_pg[mc->mc_top])) {
    key->iov_len = mc->mc_aht->aa.xsize32;
    key->iov_base = DFLKEY(mc->mc_pg[mc->mc_top], 0, key->iov_len);
    return MDBX_SUCCESS;
  }

  if (likely(data)) {
    int rc;
    if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
      rc = cursor_first(nested_setup(mc, leaf), data, nullptr);
    } else {
      rc = node_read(mc, leaf, data);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  MDBX_GET_KEY(leaf, key);
  return MDBX_SUCCESS;
}

/* Move the cursor to the last item in the database. */
static int cursor_last(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data) {
  if (mc->mc_kind8 & S_HAVESUB)
    cursor_subcur(mc)->mx_cursor.mc_state8 &= ~(C_INITIALIZED | C_EOF);

  if (likely((mc->mc_state8 & (C_EOF | C_AFTERDELETE)) != C_EOF)) {
    if (!(mc->mc_state8 & C_INITIALIZED) || mc->mc_top) {
      int rc = page_search(mc, nullptr, MDBX_PS_LAST);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    assert(IS_LEAF(mc->mc_pg[mc->mc_top]));
  }

  mc->mc_ki[mc->mc_top] = page_numkeys(mc->mc_pg[mc->mc_top]) - 1;
  mc->mc_state8 |= C_INITIALIZED | C_EOF;
  node_t *leaf = node_ptr(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);

  if (IS_DFL(mc->mc_pg[mc->mc_top])) {
    key->iov_len = mc->mc_aht->aa.xsize32;
    key->iov_base = DFLKEY(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top], key->iov_len);
    return MDBX_SUCCESS;
  }

  if (likely(data)) {
    int rc;
    if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
      rc = cursor_last(nested_setup(mc, leaf), data, nullptr);
    } else {
      rc = node_read(mc, leaf, data);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  MDBX_GET_KEY(leaf, key);
  return MDBX_SUCCESS;
}

/* Delete the specified node from a page.
 * [in] mc Cursor pointing to the node to delete.
 * [in] keysize The size of a node. Only used if the page is
 * part of a MDBX_DUPFIXED database. */
static void node_del(cursor_t *mc, size_t keysize) {
  page_t *mp = mc->mc_pg[mc->mc_top];
  indx_t indx = mc->mc_ki[mc->mc_top];
  indx_t i, j, ptr;
  node_t *node;
  char *base;

  mdbx_debug("delete node %u on %s page %" PRIaPGNO "", indx, IS_LEAF(mp) ? "leaf" : "branch", mp->mp_pgno);
  const unsigned numkeys = page_numkeys(mp);
  assert(indx < numkeys);

  if (IS_DFL(mp)) {
    assert(keysize >= sizeof(indx_t));
    unsigned diff = numkeys - 1 - indx;
    base = DFLKEY(mp, indx, keysize);
    if (diff)
      memmove(base, base + keysize, diff * keysize);
    assert(mp->mp_lower >= sizeof(indx_t));
    mp->mp_lower -= sizeof(indx_t);
    assert((size_t)UINT16_MAX - mp->mp_upper >= keysize - sizeof(indx_t));
    mp->mp_upper += (indx_t)(keysize - sizeof(indx_t));
    return;
  }

  node = node_ptr(mp, indx);
  size_t sz = NODESIZE + node->mn_ksize16;
  if (IS_LEAF(mp)) {
    if (unlikely(node->node_flags8 & NODE_BIG))
      sz += sizeof(pgno_t);
    else
      sz += node_get_datasize(node);
  }
  sz = EVEN(sz);

  ptr = mp->mp_ptrs[indx];
  for (i = j = 0; i < numkeys; i++) {
    if (i != indx) {
      mp->mp_ptrs[j] = mp->mp_ptrs[i];
      if (mp->mp_ptrs[i] < ptr) {
        assert((size_t)UINT16_MAX - mp->mp_ptrs[j] >= sz);
        mp->mp_ptrs[j] += (indx_t)sz;
      }
      j++;
    }
  }

  base = (char *)mp + mp->mp_upper + PAGEHDRSZ;
  memmove(base + sz, base, ptr - mp->mp_upper);

  assert(mp->mp_lower >= sizeof(indx_t));
  mp->mp_lower -= sizeof(indx_t);
  assert((size_t)UINT16_MAX - mp->mp_upper >= sz);
  mp->mp_upper += (indx_t)sz;
}

/* Compact the main page after deleting a node on a subpage.
 * [in] mp The main page to operate on.
 * [in] indx The index of the subpage on the main page. */
static void node_shrink(page_t *mp, unsigned indx) {
  node_t *node;
  page_t *sp, *xp;
  char *base;
  size_t nsize, delta, len, ptr;
  int i;

  node = node_ptr(mp, indx);
  sp = (page_t *)NODEDATA(node);
  delta = page_spaceleft(sp);
  nsize = node_get_datasize(node) - delta;

  /* Prepare to shift upward, set len = length(subpage part to shift) */
  if (IS_DFL(sp)) {
    len = nsize;
    if (nsize & 1)
      return; /* do not make the node uneven-sized */
  } else {
    xp = (page_t *)((char *)sp + delta); /* destination subpage */
    for (i = page_numkeys(sp); --i >= 0;) {
      assert(sp->mp_ptrs[i] >= delta);
      xp->mp_ptrs[i] = (indx_t)(sp->mp_ptrs[i] - delta);
    }
    len = PAGEHDRSZ;
  }
  sp->mp_upper = sp->mp_lower;
  sp->mp_pgno = mp->mp_pgno;
  node_set_datasize(node, nsize);

  /* Shift <lower nodes...initial part of subpage> upward */
  base = (char *)mp + mp->mp_upper + PAGEHDRSZ;
  memmove(base + delta, base, (char *)sp + len - base);

  ptr = mp->mp_ptrs[indx];
  for (i = page_numkeys(mp); --i >= 0;) {
    if (mp->mp_ptrs[i] <= ptr) {
      assert((size_t)UINT16_MAX - mp->mp_ptrs[i] >= delta);
      mp->mp_ptrs[i] += (indx_t)delta;
    }
  }
  assert((size_t)UINT16_MAX - mp->mp_upper >= delta);
  mp->mp_upper += (indx_t)delta;
}

static int cursor_get(cursor_t *mc, MDBX_iov_t *key, MDBX_iov_t *data, MDBX_cursor_op_t op) {
  int rc, exact = 0;
  assert(mc->mc_txn->mt_txnid >= mc->mc_txn->mt_env->me_oldest[0]);

  cursor_t *const nested_or_null = cursor_nested_or_null(mc);
  switch (op) {
  default:
    mdbx_debug("unhandled/unimplemented cursor operation %u", op);
    return MDBX_EINVAL;
  case MDBX_GET_CURRENT: {
    if (unlikely(!(mc->mc_state8 & C_INITIALIZED)))
      return MDBX_EINVAL;
    page_t *mp = mc->mc_pg[mc->mc_top];
    unsigned nkeys = page_numkeys(mp);
    if (mc->mc_ki[mc->mc_top] >= nkeys) {
      assert(nkeys <= UINT16_MAX);
      mc->mc_ki[mc->mc_top] = (uint16_t)nkeys;
      return MDBX_NOTFOUND;
    }
    assert(nkeys > 0);

    rc = MDBX_SUCCESS;
    if (IS_DFL(mp)) {
      key->iov_len = mc->mc_aht->aa.xsize32;
      key->iov_base = DFLKEY(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    } else {
      node_t *leaf = node_ptr(mp, mc->mc_ki[mc->mc_top]);
      MDBX_GET_KEY(leaf, key);
      if (data) {
        if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
          if (unlikely(nested_or_null == nullptr))
            return MDBX_CORRUPTED;
          if (unlikely(!(nested_or_null->mc_state8 & C_INITIALIZED))) {
            rc = cursor_first(nested_setup(mc, leaf), data, nullptr);
            if (unlikely(rc != MDBX_SUCCESS))
              return rc;
          }
          rc = cursor_get(nested_or_null, data, nullptr, MDBX_GET_CURRENT);
        } else {
          rc = node_read(mc, leaf, data);
        }
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
      }
    }
    break;
  }
  case MDBX_GET_BOTH:
  case MDBX_GET_BOTH_RANGE:
    if (unlikely(data == nullptr))
      return MDBX_EINVAL;
    if (unlikely((mc->mc_kind8 & S_HAVESUB) == 0))
      return MDBX_INCOMPATIBLE;
  /* FALLTHRU */
  case MDBX_SET:
  case MDBX_SET_KEY:
  case MDBX_SET_RANGE:
    if (unlikely(key == nullptr))
      return MDBX_EINVAL;
    rc = cursor_set(mc, key, data, op, op == MDBX_SET_RANGE ? nullptr : &exact);
    break;
  case MDBX_GET_MULTIPLE:
    if (unlikely(data == nullptr || !(mc->mc_state8 & C_INITIALIZED)))
      return MDBX_EINVAL;
    if (unlikely((mc->mc_kind8 & S_DUPFIXED) == 0))
      return MDBX_INCOMPATIBLE;
    rc = MDBX_SUCCESS;
  fetch_multiple:
    assert(rc == MDBX_SUCCESS && data && nested_or_null);
    if ((nested_or_null->mc_state8 & (C_INITIALIZED | C_EOF)) != C_INITIALIZED) {
      rc = MDBX_NOTFOUND;
      break;
    }
    data->iov_len = page_numkeys(nested_or_null->mc_pg[nested_or_null->mc_top]) *
                    (size_t)nested_or_null->mc_aht->aa.xsize32;
    data->iov_base = page_data(nested_or_null->mc_pg[nested_or_null->mc_top]);
    nested_or_null->mc_ki[nested_or_null->mc_top] =
        page_numkeys(nested_or_null->mc_pg[nested_or_null->mc_top]) - 1;
    break;

  case MDBX_NEXT_MULTIPLE:
    if (unlikely(data == nullptr))
      return MDBX_EINVAL;
    if (unlikely((mc->mc_kind8 & S_DUPFIXED) == 0))
      return MDBX_INCOMPATIBLE;
    rc = cursor_next(mc, key, data, MDBX_NEXT_DUP);
    if (likely(rc == MDBX_SUCCESS))
      goto fetch_multiple;
    break;
  case MDBX_PREV_MULTIPLE:
    if (data == nullptr)
      return MDBX_EINVAL;
    if (unlikely((mc->mc_kind8 & S_DUPFIXED) == 0))
      return MDBX_INCOMPATIBLE;
    rc = MDBX_SUCCESS;
    if (!(mc->mc_state8 & C_INITIALIZED)) {
      rc = cursor_last(mc, key, data);
      if (unlikely(rc != MDBX_SUCCESS))
        break;
    }
    if (!(nested_or_null->mc_state8 & C_INITIALIZED)) {
      rc = MDBX_NOTFOUND;
      break;
    }
    rc = cursor_sibling(nested_or_null, 0);
    if (likely(rc == MDBX_SUCCESS))
      goto fetch_multiple;
    break;

  case MDBX_NEXT:
  case MDBX_NEXT_DUP:
  case MDBX_NEXT_NODUP:
    rc = cursor_next(mc, key, data, op);
    break;
  case MDBX_PREV:
  case MDBX_PREV_DUP:
  case MDBX_PREV_NODUP:
    rc = cursor_prev(mc, key, data, op);
    break;
  case MDBX_FIRST_DUP:
  case MDBX_LAST_DUP: {
    if (unlikely(data == nullptr || !(mc->mc_state8 & C_INITIALIZED)))
      return MDBX_EINVAL;
    if (unlikely(nested_or_null == nullptr))
      return MDBX_INCOMPATIBLE;
    if (mc->mc_ki[mc->mc_top] >= page_numkeys(mc->mc_pg[mc->mc_top]))
      return MDBX_NOTFOUND;

    node_t *leaf = node_ptr(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
    if (!F_ISSET(leaf->node_flags8, NODE_DUP)) {
      MDBX_GET_KEY(leaf, key);
      rc = node_read(mc, leaf, data);
    } else {
      if (unlikely(!(nested_or_null->mc_state8 & C_INITIALIZED)))
        return MDBX_NOTFOUND;
      rc = (op == MDBX_FIRST_DUP) ? cursor_first(nested_or_null, data, nullptr)
                                  : cursor_last(nested_or_null, data, nullptr);
    }
    break;
  }

  case MDBX_FIRST:
    rc = cursor_first(mc, key, data);
    break;
  case MDBX_LAST:
    rc = cursor_last(mc, key, data);
    break;
  }

  mc->mc_state8 &= ~C_AFTERDELETE;
  return rc;
}

/* Touch all the pages in the cursor stack. Set mc_top.
 * Makes sure all the pages are writable, before attempting a write operation.
 * [in] mc The cursor to operate on. */
static int cursor_touch(cursor_t *mc) {
  assert(cursor_is_aah_valid(cursor_bundle(mc)));

  if ((mc->mc_aht->ah.kind_and_state16 & (MDBX_AAH_DIRTY | MDBX_AAH_DUPS | MDBX_AAH_GACO | MDBX_AAH_MAIN)) ==
      0) {
    /* Touch record of named AA */
    MDBX_cursor_t bc;
    int rc = cursor_init(&bc, mc->mc_txn, aht_main(mc->mc_txn));
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    rc = page_search(&bc.primal, &mc->mc_aht->ahe->ax_ident, MDBX_PS_MODIFY);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mc->mc_aht->ah.state8 |= MDBX_AAH_DIRTY;
  }

  mc->mc_top = 0;
  if (mc->mc_snum) {
    do {
      int rc = page_touch(mc);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    } while (++(mc->mc_top) < mc->mc_snum);
    mc->mc_top = mc->mc_snum - 1;
  }

  return MDBX_SUCCESS;
}

static MDBX_numeric_result_t cursor_count(MDBX_cursor_t *bundle) {
  MDBX_numeric_result_t result;

  if (!bundle->primal.mc_snum) {
    result.value = 0;
    result.err = MDBX_EOF;
    return result;
  }

  page_t *mp = bundle->primal.mc_pg[bundle->primal.mc_top];
  if ((bundle->primal.mc_state8 & C_EOF) && bundle->primal.mc_ki[bundle->primal.mc_top] >= page_numkeys(mp)) {
    result.value = 0;
    result.err = MDBX_EOF;
    return result;
  }

  result.value = 1;
  result.err = MDBX_SUCCESS;
  if (bundle->subcursor.mx_cursor.mc_state8 & C_INITIALIZED) {
    node_t *leaf = node_ptr(mp, bundle->primal.mc_ki[bundle->primal.mc_top]);
    if (F_ISSET(leaf->node_flags8, NODE_DUP)) {
      result.value = unlikely(bundle->subcursor.mx_aht_body.aa.entries > SIZE_MAX)
                         ? SIZE_MAX
                         : (size_t)bundle->subcursor.mx_aht_body.aa.entries;
    }
  }

  return result;
}