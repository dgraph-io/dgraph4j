/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Base exception for all errors thrown by the Dgraph client. Extends {@link
 * StatusRuntimeException} so that existing code catching gRPC exceptions continues to work.
 *
 * <p>For errors originating from gRPC calls, the {@link Status} carries the real gRPC status code
 * and description. For client-side errors (e.g., transaction state violations), the status is
 * synthesized as {@link Status#INTERNAL}.
 */
public class DgraphException extends StatusRuntimeException {
  private static final long serialVersionUID = 1L;

  /** For gRPC-origin errors with full Status and optional trailers. */
  DgraphException(Status status, Metadata trailers) {
    super(status, trailers);
  }

  /** For client-side errors with a descriptive message. Uses Status.INTERNAL. */
  DgraphException(String message) {
    super(Status.INTERNAL.withDescription(message));
  }

  /** For wrapping non-gRPC causes. Uses Status.INTERNAL. */
  DgraphException(String message, Throwable cause) {
    super(Status.INTERNAL.withDescription(message).withCause(cause));
  }

  /**
   * Returns whether this error is typically safe to retry. Subclasses override this to indicate
   * retryable conditions such as transient connectivity issues or transaction conflicts.
   */
  public boolean isRetryable() {
    return false;
  }
}
