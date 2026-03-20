/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a transaction conflicts with another concurrent transaction. This is a retryable error
 * — the operation can be retried with a new transaction.
 */
public class TxnConflictException extends TxnException {
  private static final long serialVersionUID = 1L;

  public TxnConflictException(String msg) {
    super(Status.ABORTED.withDescription(msg), null);
  }

  TxnConflictException(Status status, Metadata trailers) {
    super(status, trailers);
  }

  @Override
  public boolean isRetryable() {
    return true;
  }
}
