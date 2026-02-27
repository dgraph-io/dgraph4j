/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public abstract class TxnException extends RuntimeException {
  TxnException(String message) {
    super(message);
  }
}
