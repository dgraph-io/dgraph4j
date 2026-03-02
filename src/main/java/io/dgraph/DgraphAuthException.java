/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when authentication or authorization fails. This includes expired tokens, invalid
 * credentials, and permission denied errors.
 */
public class DgraphAuthException extends DgraphException {
  private static final long serialVersionUID = 1L;

  DgraphAuthException(Status status, Metadata trailers) {
    super(status, trailers);
  }

  DgraphAuthException(String message, Throwable cause) {
    super(message, cause);
  }
}
