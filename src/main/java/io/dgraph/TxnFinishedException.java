/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public class TxnFinishedException extends TxnException {
  private static final long serialVersionUID = 1L;

  TxnFinishedException() {
    super("Transaction has already been committed or discarded");
  }
}
