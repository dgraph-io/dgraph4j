/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a Dgraph Alpha is in draining mode and shutting down. It will not accept new
 * transactions. The client should retry against a different Alpha in the cluster.
 */
public class AlphaShutdownException extends AlphaException {
  private static final long serialVersionUID = 1L;

  AlphaShutdownException(Status status, Metadata trailers) {
    super(status, trailers);
  }
}
