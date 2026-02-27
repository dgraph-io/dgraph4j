/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public abstract class TxnException extends RuntimeException {
  TxnException(String message) {
    super(message);
  }
}
