/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a connection to the Dgraph server is lost or unavailable. This is typically a
 * transient error that can be resolved by retrying.
 */
public class DgraphConnectionException extends DgraphException {
  private static final long serialVersionUID = 1L;

  DgraphConnectionException(Status status, Metadata trailers) {
    super(status, trailers);
  }

  @Override
  public boolean isRetryable() {
    return true;
  }
}
