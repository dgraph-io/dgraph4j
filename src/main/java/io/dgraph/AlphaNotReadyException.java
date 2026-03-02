/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a Dgraph Alpha is reachable but not yet ready to accept requests. This typically
 * occurs during startup, rolling deploys, or background indexing and will resolve on its own.
 * Retrying after a short backoff is appropriate.
 */
public class AlphaNotReadyException extends AlphaException {
  private static final long serialVersionUID = 1L;

  AlphaNotReadyException(Status status, Metadata trailers) {
    super(status, trailers);
  }
}
