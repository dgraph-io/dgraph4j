/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public class TxnReadOnlyException extends TxnException {
  private static final long serialVersionUID = 1L;

  TxnReadOnlyException() {
    super("Transaction is read only. No mutate or commit operation is allowed.");
  }
}
