/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public class TxnReadOnlyException extends TxnException {
  TxnReadOnlyException() {
    super("Transaction is read only. No mutate or commit operation is allowed.");
  }
}
