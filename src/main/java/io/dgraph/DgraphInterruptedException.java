/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Status;

/** Thrown when a Dgraph operation is interrupted. */
public class DgraphInterruptedException extends DgraphException {
  private static final long serialVersionUID = 1L;

  DgraphInterruptedException(String message, InterruptedException cause) {
    super(Status.CANCELLED.withDescription(message).withCause(cause), null);
    // Preserve the interrupt flag so callers further up the stack can detect it.
    Thread.currentThread().interrupt();
  }
}
