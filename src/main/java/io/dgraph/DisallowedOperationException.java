/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when the Dgraph Alpha refuses to execute a well-formed operation. This can occur due to
 * server configuration (e.g., mutations disabled, drop operations blocked) or other conditions that
 * prevent the operation from being carried out.
 */
public class DisallowedOperationException extends DgraphException {
  private static final long serialVersionUID = 1L;

  DisallowedOperationException(Status status, Metadata trailers) {
    super(status, trailers);
  }
}
