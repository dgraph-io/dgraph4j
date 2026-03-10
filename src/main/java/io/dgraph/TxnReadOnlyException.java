/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public class TxnReadOnlyException extends TxnException {
  private static final long serialVersionUID = 1L;

  TxnReadOnlyException() {
    super("Transaction is read only. No mutate or commit operation is allowed.");
  }
}
