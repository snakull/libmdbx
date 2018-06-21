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
  const char *type, *state = (mp->mp_flags16 & P_DIRTY) ? ", dirty" : "";
  node_t *node;
  unsigned i, nkeys;
  MDBX_iov_t key;
  size_t nsize, total = 0;
  DKBUF;

  switch (mp->mp_flags16 & (P_BRANCH | P_LEAF | P_DFL | P_META | P_OVERFLOW | P_SUBP)) {
  case P_BRANCH:
    type = "Branch page";
    break;
  case P_LEAF:
    type = "Leaf page";
    break;
  case P_LEAF | P_SUBP:
    type = "Sub-page";
    break;
  case P_LEAF | P_DFL:
    type = "DFL page";
    break;
  case P_LEAF | P_DFL | P_SUBP:
    type = "DFL sub-page";
    break;
  case P_OVERFLOW:
    audit_verbose("Overflow page %" PRIaPGNO " pages %u%s\n", pgno, mp->mp_pages, state);
    return;
  case P_META:
    audit_verbose("Meta-page %" PRIaPGNO " txnid %" PRIaTXN "\n", pgno,
                  meta_txnid_fluid(env, (const meta_t *)page_data(mp)));
    return;
  default:
    audit_error("Bad page %" PRIaPGNO " flags 0x%X\n", pgno, mp->mp_flags16);
    return;
  }

  nkeys = page_numkeys(mp);
  audit_info("%s %" PRIaPGNO " page_numkeys %u%s\n", type, pgno, nkeys, state);

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

__maybe_unused static void cursor_check(MDBX_cursor_t *mc) {
  unsigned i;
  node_t *node;
  page_t *mp;

  if (!mc->primal.mc_snum || !(mc->primal.mc_state8 & C_INITIALIZED))
    return;
  for (i = 0; i < mc->primal.mc_top; i++) {
    mp = mc->primal.mc_pg[i];
    node = node_ptr(mp, mc->primal.mc_ki[i]);
    if (unlikely(node_get_pgno(node) != mc->primal.mc_pg[i + 1]->mp_pgno))
      audit_error("oops: node_get_pgno(node) {%" PRIaPGNO "} != {%" PRIaPGNO
                  "} mc->primal.mc_pg[i + 1]->mp_pgno",
                  node_get_pgno(node), mc->primal.mc_pg[i + 1]->mp_pgno);
  }
  if (unlikely(mc->primal.mc_ki[i] >= page_numkeys(mc->primal.mc_pg[i])))
    audit_error("oops: mc->primal.mc_ki[i] {%u} >= {%u} page_numkeys(mc->primal.mc_pg[i]", mc->primal.mc_ki[i],
                page_numkeys(mc->primal.mc_pg[i]));
  if (cursor_is_nested_inited(mc)) {
    node = node_ptr(mc->primal.mc_pg[mc->primal.mc_top], mc->primal.mc_ki[mc->primal.mc_top]);
    if (((node->node_flags8 & (NODE_DUP | NODE_SUBTREE)) == NODE_DUP) &&
        mc->subcursor.mx_cursor.mc_pg[0] != NODEDATA(node)) {
      audit_error("oops: mc->subcursor.mx_cursor.mc_pg[0] != NODEDATA(node)");
    }
  }
}
#endif /* MDBX_CONFIG_DBG_AUDIT */

/* Count all the pages in each AA and in the GACO and make sure
 * it matches the actual number of pages being used.
 * All named AAs must be open for a correct count. */
static int audit(MDBX_txn_t *txn) {
  MDBX_cursor_t mc;
  int rc = cursor_init(&mc, txn, aht_gaco(txn));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_iov_t key, data;
  uint64_t freecount = 0;
  while ((rc = mdbx_cursor_get(&mc, &key, &data, MDBX_NEXT)) == 0)
    freecount += *(pgno_t *)data.iov_base;
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  uint64_t count = 0;
  aht_t *aht = txn->txn_aht_array;
  for (MDBX_aah_t aah = 0; aah < txn->txn_ah_num; ++aah, ++aht) {
    if (!(aht->ah.state8 & MDBX_AAH_VALID))
      continue;
    rc = cursor_init(&mc, txn, aht);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    if (aht->aa.root == P_INVALID)
      continue;
    count += aht->aa.branch_pages + aht->aa.leaf_pages + aht->aa.overflow_pages;
    if (aht->aa.flags16 & MDBX_DUPSORT) {
      rc = page_search(&mc.primal, nullptr, MDBX_PS_FIRST);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      while (true) {
        page_t *mp = mc.primal.mc_pg[mc.primal.mc_top];
        for (unsigned n = 0; n < page_numkeys(mp); n++) {
          node_t *leaf = node_ptr(mp, n);
          if (leaf->node_flags8 & NODE_SUBTREE) {
            aatree_t *entry = NODEDATA(leaf);
            count += get_le64_aligned2(&entry->aa_branch_pages) + get_le64_aligned2(&entry->aa_leaf_pages) +
                     get_le64_aligned2(&entry->aa_overflow_pages);
          }
        }
        rc = cursor_sibling(&mc.primal, true);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
      }
    }
  }
  if (freecount + count + MDBX_NUM_METAS != txn->mt_next_pgno) {
    log_error(MDBX_LOG_AUDIT,
              "audit: %" PRIaTXN " gaco: %" PRIu64 " count: %" PRIu64 " total: %" PRIu64
              " next_pgno: %" PRIaPGNO "\n",
              txn->mt_txnid, freecount, count + MDBX_NUM_METAS, freecount + count + MDBX_NUM_METAS,
              txn->mt_next_pgno);
    return MDBX_SIGN;
  }
  return MDBX_SUCCESS;
}
