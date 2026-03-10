/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a Dgraph Alpha is reachable but temporarily unable to serve requests. This covers
 * transient Alpha-side conditions such as startup, draining, and overload. All subclasses are
 * retryable.
 */
public class AlphaException extends DgraphException {
  private static final long serialVersionUID = 1L;

  AlphaException(Status status, Metadata trailers) {
    super(status, trailers);
  }

  @Override
  public boolean isRetryable() {
    return true;
  }
}
