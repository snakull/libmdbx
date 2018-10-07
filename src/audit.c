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

#define audit_extra(fmt, ...) log_extra(MDBX_LOG_AUDIT, fmt, ##__VA_ARGS__)
#define audit_trace(fmt, ...) log_trace(MDBX_LOG_AUDIT, fmt, ##__VA_ARGS__)
#define audit_verbose(fmt, ...) log_verbose(MDBX_LOG_AUDIT, fmt, ##__VA_ARGS__)
#define audit_info(fmt, ...) log_info(MDBX_LOG_AUDIT, fmt, ##__VA_ARGS__)
#define audit_notice(fmt, ...) log_notice(MDBX_LOG_AUDIT, fmt, ##__VA_ARGS__)
#define audit_warning(fmt, ...) log_warning(MDBX_LOG_AUDIT, fmt, ##__VA_ARGS__)
#define audit_error(fmt, ...) log_error(MDBX_LOG_AUDIT, fmt, ##__VA_ARGS__)
#define audit_panic(env, msg, err)                                                                            \
  mdbx_panic(env, MDBX_LOG_AUDIT, __func__, __LINE__, "%s, error %d", msg, err)

/*----------------------------------------------------------------------------*/

#if MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_DBG_AUDIT

static const char *leafnode_type(node_t *n) {
  static char *const tp[2][2] = {{"", ": AA"}, {": sub-page", ": sub-AA"}};
  return unlikely(n->node_flags8 & NODE_BIG)
             ? ": overflow page"
             : tp[F_ISSET(n->node_flags8, NODE_DUP)][F_ISSET(n->node_flags8, NODE_SUBTREE)];
}

/* Display all the keys in the page. */
__maybe_unused static void page_list(const MDBX_env_t *env, page_t *mp) {
  pgno_t pgno = mp->mp_pgno;
  const char *type, *state = IS_DIRTY(mp) ? ", dirty" : "";
  node_t *node;
  unsigned i, nkeys;
  MDBX_iov_t key;
  size_t nsize, total = 0;
  DKBUF;

  switch (mp->mp_flags16 & (P_BRANCH | P_LEAF | P_DFL | P_META | P_OVERFLOW | P_SUBP)) {
  case P_BRANCH:
    type = "branch";
    break;
  case P_LEAF:
    type = "leaf";
    break;
  case P_LEAF | P_DFL:
    type = "leaf-dupfixed";
    break;
  case P_LEAF | P_SUBP:
    type = "subleaf-dupsort";
    break;
  case P_LEAF | P_DFL | P_SUBP:
    type = "subleaf-dupfixed";
    break;
  case P_OVERFLOW:
    audit_verbose("large-page #%" PRIaPGNO " *%u%s\n", pgno, mp->mp_pages, state);
    return;
  case P_META:
    audit_verbose("meta-page #%" PRIaPGNO " txnid %" PRIaTXN "\n", pgno,
                  meta_txnid_fluid(env, (const meta_t *)page_data(mp)));
    return;
  default:
    audit_error("unknown-type-page %" PRIaPGNO " flags 0x%X\n", pgno, mp->mp_flags16);
    return;
  }

  nkeys = page_numkeys(mp);
  audit_info("%s page #%" PRIaPGNO " numkeys %u%s\n", type, pgno, nkeys, state);

  for (i = 0; i < nkeys; i++) {
    if (IS_DFL(mp)) { /* DFL pages have no mp_ptrs[] or node headers */
      key.iov_len = nsize = mp->mp_leaf2_ksize16;
      key.iov_base = DFLKEY(mp, i, nsize);
      total += nsize;
      audit_trace("key %u: nsize %zu, %s\n", i, nsize, DKEY(&key));
      continue;
    }
    node = node_ptr(mp, i);
    key.iov_len = node->mn_ksize16;
    key.iov_base = node->mn_data;
    nsize = NODESIZE + key.iov_len;
    if (IS_BRANCH(mp)) {
      audit_verbose("key %u: page %" PRIaPGNO ", %s\n", i, node_get_pgno(node), DKEY(&key));
      total += nsize;
    } else {
      if (unlikely(node->node_flags8 & NODE_BIG))
        nsize += sizeof(pgno_t);
      else
        nsize += node_get_datasize(node);
      total += nsize;
      nsize += sizeof(indx_t);
      audit_verbose("key %u: nsize %zu, %s%s\n", i, nsize, DKEY(&key), leafnode_type(node));
    }
    total = EVEN(total);
  }
  audit_info("Total: header %u + contents %zu + unused %u\n",
             IS_DFL(mp) ? PAGEHDRSZ : PAGEHDRSZ + mp->mp_lower, total, page_spaceleft(mp));
}

static __cold MDBX_error_t cursor_check(cursor_t *mc, bool pending) {
  if (unlikely((mc->mc_state8 & C_INITIALIZED) == 0))
    return MDBX_SUCCESS;
  if (likely((mc->mc_kind8 & S_STASH) == 0)) {
    assert(((mc->mc_aht->aa.flags16 & MDBX_DUPSORT) != 0) == ((mc->mc_kind8 & S_HAVESUB) != 0));
    if (((mc->mc_aht->aa.flags16 & MDBX_DUPSORT) != 0) != ((mc->mc_kind8 & S_HAVESUB) != 0))
      return MDBX_CURSOR_FULL;
  }
  assert(mc->mc_top == mc->mc_snum - 1);
  if (unlikely(mc->mc_top != mc->mc_snum - 1))
    return MDBX_CURSOR_FULL;
  assert(pending ? mc->mc_snum <= mc->mc_aht->aa.depth16 : mc->mc_snum == mc->mc_aht->aa.depth16);
  if (unlikely(pending ? mc->mc_snum > mc->mc_aht->aa.depth16 : mc->mc_snum != mc->mc_aht->aa.depth16))
    return MDBX_CURSOR_FULL;

  for (unsigned n = 0; n < mc->mc_snum; ++n) {
    page_t *mp = mc->mc_pg[n];
    const unsigned numkeys = page_numkeys(mp);
    const bool expect_branch = (n < mc->mc_aht->aa.depth16 - 1u) ? true : false;
    const bool expect_nested_leaf = (n + 1 == mc->mc_aht->aa.depth16 - 1u) ? true : false;
    const bool branch = IS_BRANCH(mp) ? true : false;
    assert(branch == expect_branch);
    if (unlikely(branch != expect_branch))
      return MDBX_CURSOR_FULL;
    if (!pending) {
      assert(numkeys > mc->mc_ki[n] || (!branch && numkeys == mc->mc_ki[n] && (mc->mc_state8 & C_EOF) != 0));
      if (unlikely(numkeys <= mc->mc_ki[n] &&
                   !(!branch && numkeys == mc->mc_ki[n] && (mc->mc_state8 & C_EOF) != 0)))
        return MDBX_CURSOR_FULL;
    } else {
      assert(numkeys + 1 >= mc->mc_ki[n]);
      if (unlikely(numkeys + 1 < mc->mc_ki[n]))
        return MDBX_CURSOR_FULL;
    }

    for (unsigned i = 0; i < numkeys; ++i) {
      node_t *node = node_ptr(mp, i);
      if (branch) {
        assert(node->node_flags8 == 0);
        if (unlikely(node->node_flags8 != 0))
          return MDBX_CURSOR_FULL;
        pgno_t pgno = node_get_pgno(node);
        page_t *np;
        int rc = page_get(mc->mc_txn, pgno, &np, NULL);
        assert(rc == MDBX_SUCCESS);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
        const bool nested_leaf = IS_LEAF(np) ? true : false;
        assert(nested_leaf == expect_nested_leaf);
        if (unlikely(nested_leaf != expect_nested_leaf))
          return MDBX_CURSOR_FULL;
      }
    }
  }
  return MDBX_SUCCESS;
}
#endif /* MDBX_CONFIG_DBG_AUDIT */

/* Count all the pages in each AA and in the GACO and make sure
 * it matches the actual number of pages being used.
 * All named AAs must be open for a correct count. */
static int audit(MDBX_txn_t *txn, unsigned befree_stored) {
  MDBX_cursor_t mc;
  int rc = cursor_init(&mc, txn, aht_gaco(txn));
  if (unlikely(rc != MDBX_SUCCESS)) {
    audit_error("%s failed %d\n", "cursor_init(GACO)", rc);
    return rc;
  }

  const uint64_t pending =
      (txn->mt_flags & MDBX_RDONLY)
          ? 0
          : txn->mt_loose_count +
                (txn->mt_env->me_reclaimed_pglist ? MDBX_PNL_SIZE(txn->mt_env->me_reclaimed_pglist) : 0) +
                (txn->mt_befree_pages ? MDBX_PNL_SIZE(txn->mt_befree_pages) - befree_stored : 0);

  uint64_t gaco = 0, tree = 0;
  aht_t *gaco_aht = aht_gaco(txn);
  if (gaco_aht->aa.root != P_INVALID) {
    MDBX_iov_t key, data;
    while ((rc = mdbx_cursor_get(&mc, &key, &data, MDBX_NEXT)) == MDBX_SUCCESS) {
      pgno_t count;
      if (unlikely(data.iov_len < sizeof(pgno_t) || data.iov_len % sizeof(pgno_t) != 0))
        return MDBX_CORRUPTED;
      memcpy(&count, data.iov_base, sizeof(pgno_t));
      if (unlikely(data.iov_len < sizeof(pgno_t) * (count + 1)))
        return MDBX_CORRUPTED;
      gaco += count;
    }
    if (unlikely(rc != MDBX_NOTFOUND)) {
      audit_error("%s failed %d\n", "mdbx_cursor_get(GACO, MDBX_NEXT)", rc);
      return rc;
    }
    tree = gaco_aht->aa.branch_pages + gaco_aht->aa.leaf_pages + gaco_aht->aa.overflow_pages;
  }

  aht_t *main_aht = aht_main(txn);
  if (main_aht->aa.root != P_INVALID) {
    tree += main_aht->aa.branch_pages + main_aht->aa.leaf_pages + main_aht->aa.overflow_pages;

    rc = cursor_init(&mc, txn, main_aht);
    if (unlikely(rc != MDBX_SUCCESS)) {
      audit_error("%s failed %d\n", "cursor_init(MAIN)", rc);
      return rc;
    }

    rc = page_search(&mc.primal, nullptr, MDBX_PS_FIRST);
    if (unlikely(rc != MDBX_SUCCESS)) {
      audit_error("%s failed %d\n", "page_search(MDBX_PS_FIRST)", rc);
      return rc;
    }

    do {
      page_t *mp = mc.primal.mc_pg[mc.primal.mc_top];
      for (unsigned n = 0; n < page_numkeys(mp); n++) {
        node_t *leaf = node_ptr(mp, n);
        if (leaf->node_flags8 != NODE_SUBTREE)
          continue;

        aatree_t *entry = NODEDATA(leaf);
        if ((txn->mt_flags & MDBX_RDONLY) == 0) {
          const MDBX_iov_t aa_ident = {NODEKEY(leaf), node_get_keysize(leaf)};
          ahe_rc_t rp = aa_lookup(txn->mt_env, txn, aa_ident);
          if (rp.err == MDBX_SUCCESS) {
            aht_t *aht = txn_ahe2aht(txn, rp.ahe);
            tree += aht->aa.branch_pages + aht->aa.leaf_pages + aht->aa.overflow_pages;
            continue;
          }
          if (rp.err != MDBX_EOF)
            return rp.err;
        }

        if (sizeof(pgno_t) == 4) {
          tree += get_le32_unaligned(&entry->aa_branch_pages) + get_le32_unaligned(&entry->aa_leaf_pages) +
                  get_le32_unaligned(&entry->aa_overflow_pages);
        } else {
          tree += get_le64_unaligned(&entry->aa_branch_pages) + get_le64_unaligned(&entry->aa_leaf_pages) +
                  get_le64_unaligned(&entry->aa_overflow_pages);
        }
      }
      rc = cursor_sibling(&mc.primal, true);
    } while (rc == MDBX_SUCCESS);

    if (unlikely(rc != MDBX_NOTFOUND)) {
      audit_error("%s failed %d\n", "cursor_sibling()", rc);
      return rc;
    }
  }

  if (pending + gaco + tree + MDBX_NUM_METAS == txn->mt_next_pgno)
    return MDBX_SUCCESS;

  if ((txn->mt_flags & MDBX_RDONLY) == 0)
    audit_error("@%" PRIaTXN ": %" PRIu64 "(pending) = %u(loose-count) + "
                "%u(reclaimed-list) + %u(befree-pending) - %u(befree-stored)",
                txn->mt_txnid, pending, txn->mt_loose_count,
                txn->mt_env->me_reclaimed_pglist ? MDBX_PNL_SIZE(txn->mt_env->me_reclaimed_pglist) : 0,
                txn->mt_befree_pages ? MDBX_PNL_SIZE(txn->mt_befree_pages) : 0, befree_stored);
  audit_error("@%" PRIaTXN ": %" PRIu64 "(pending) + %" PRIu64 "(free) + %" PRIu64 "(count) = %" PRIu64
              "(total) <> %" PRIaPGNO "(next-pgno)",
              txn->mt_txnid, pending, gaco, tree + MDBX_NUM_METAS, pending + gaco + tree + MDBX_NUM_METAS,
              txn->mt_next_pgno);
  return MDBX_PROBLEM;
}

__cold MDBX_error_t mdbx_get4testing(MDBX_txn_t *txn, MDBX_aah_t aah, MDBX_iov_t *key, MDBX_iov_t *data) {
  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  MDBX_error_t rc = validate_txn_ro(txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  DKBUF;
  mdbx_debug("===> get db %" PRIuFAST32 " key [%s]", aah, DKEY(key));

  const aht_rc_t rp = aa_take(txn, aah);
  if (unlikely(rp.err != MDBX_SUCCESS))
    return rp.err;

  MDBX_cursor_t mc;
  rc = cursor_init(&mc, txn, rp.aht);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  int exact = false;
  rc = cursor_set(&mc.primal, key, data, (rp.aht->aa.flags16 & MDBX_DUPSORT) ? MDBX_GET_BOTH : MDBX_SET_KEY,
                  &exact);
  return (rc == MDBX_SUCCESS && !exact) ? MDBX_SIGN : rc;
}
