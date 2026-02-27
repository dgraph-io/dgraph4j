/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public class TxnConflictException extends TxnException {
  public TxnConflictException(String msg) {
    super(msg);
  }
}
