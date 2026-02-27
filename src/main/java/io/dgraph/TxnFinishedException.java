/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public class TxnFinishedException extends TxnException {
  TxnFinishedException() {
    super("Transaction has already been committed or discarded");
  }
}
