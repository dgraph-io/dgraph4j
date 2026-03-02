/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a resource limit is exceeded, such as the maximum gRPC message size. This is not
 * retryable without changing the request (e.g., reducing payload size).
 */
public class DgraphResourceExhaustedException extends DgraphException {
  private static final long serialVersionUID = 1L;

  DgraphResourceExhaustedException(Status status, Metadata trailers) {
    super(status, trailers);
  }
}
