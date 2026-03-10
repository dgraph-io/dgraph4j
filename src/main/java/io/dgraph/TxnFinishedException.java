/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public class TxnFinishedException extends TxnException {
  private static final long serialVersionUID = 1L;

  TxnFinishedException() {
    super("Transaction has already been committed or discarded");
  }
}
