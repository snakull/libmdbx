/*
 * Copyright 2017-2018 Leonid Yuriev <leo@yuriev.ru>
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

#include "test.h"

bool testcase_try::setup() {
  log_trace(">> setup");
  if (!inherited::setup())
    return false;

  log_trace("<< setup");
  return true;
}

bool testcase_try::run() {
  db_open();
  assert(!txn_guard);

  const MDBX_txn_result_t tr1 = mdbx_begin(db_guard.get(), nullptr, MDBX_RDWR);
  if (unlikely(tr1.err != MDBX_SUCCESS))
    failure_perror("mdbx_txn_begin(MDBX_RDWR)", tr1.err);
  else {
    const MDBX_txn_result_t tr2 = mdbx_begin(db_guard.get(), nullptr, MDBX_RDWR | MDBX_NONBLOCK);
    if (unlikely(tr2.err != MDBX_EBUSY))
      failure_perror("mdbx_txn_begin(MDBX_RDWR|MDBX_NONBLOCK)", tr2.err);
  }

  txn_guard.reset(tr1.txn);
  return true;
}

bool testcase_try::teardown() {
  log_trace(">> teardown");
  cursor_guard.release();
  txn_guard.release();
  db_guard.release();
  return inherited::teardown();
}
