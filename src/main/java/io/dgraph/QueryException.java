/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a query or mutation contains invalid syntax or input. This is not retryable — the
 * query must be corrected before retrying.
 */
public class QueryException extends DgraphException {
  private static final long serialVersionUID = 1L;

  QueryException(Status status, Metadata trailers) {
    super(status, trailers);
  }
}
