/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when the Dgraph server is overloaded with pending proposals. This is a transient error that
 * can be resolved by retrying after a backoff period.
 */
public class DgraphOverloadedException extends DgraphException {
  private static final long serialVersionUID = 1L;

  DgraphOverloadedException(Status status, Metadata trailers) {
    super(status, trailers);
  }

  @Override
  public boolean isRetryable() {
    return true;
  }
}
