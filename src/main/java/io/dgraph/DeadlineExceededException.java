/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a Dgraph operation exceeds its deadline or timeout. This is typically a transient
 * error that can be resolved by retrying, possibly with a longer deadline.
 */
public class DeadlineExceededException extends DgraphException {
  private static final long serialVersionUID = 1L;

  DeadlineExceededException(Status status, Metadata trailers) {
    super(status, trailers);
  }

  @Override
  public boolean isRetryable() {
    return true;
  }
}
