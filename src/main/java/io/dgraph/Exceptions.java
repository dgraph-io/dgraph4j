/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/** Utility methods for exception handling in the Dgraph client. */
public class Exceptions {

  private Exceptions() {} // prevent instantiation

  public static RuntimeException unwrapException(CompletionException ex) {
    if (ex.getCause() instanceof RuntimeException) {
      return (RuntimeException) ex.getCause();
    }
    return ex;
  }

  public static <R> R withExceptionUnwrapped(Supplier<R> s) {
    try {
      return s.get();
    } catch (CompletionException ex) {
      throw unwrapException(ex);
    }
  }

  public static void withExceptionUnwrapped(Runnable r) {
    try {
      r.run();
    } catch (CompletionException ex) {
      throw unwrapException(ex);
    }
  }

  /**
   * Translates a Throwable into a typed DgraphException. If the throwable is already a
   * DgraphException, returns it as-is. If it is a StatusRuntimeException, maps it by status code.
   * Otherwise wraps it in a generic DgraphException.
   */
  public static DgraphException translate(Throwable t) {
    // Unwrap CompletionException to get at the real cause
    if (t instanceof CompletionException && t.getCause() != null) {
      return translate(t.getCause());
    }

    if (t instanceof DgraphException) {
      return (DgraphException) t;
    }

    if (t instanceof StatusRuntimeException) {
      return fromStatusRuntimeException((StatusRuntimeException) t);
    }

    return new DgraphException(t.getMessage(), t);
  }

  /**
   * Maps a StatusRuntimeException to a typed DgraphException. Status codes are the primary
   * discriminator. Message-based matching is a fallback only for UNKNOWN status, which the Dgraph
   * server uses for many semantically distinct errors.
   */
  private static DgraphException fromStatusRuntimeException(StatusRuntimeException sre) {
    Status status = sre.getStatus();
    Metadata trailers = sre.getTrailers();

    // Primary dispatch: gRPC status code
    switch (status.getCode()) {
        // Transport-level errors (set by gRPC, not Dgraph)
      case DEADLINE_EXCEEDED:
        return new DeadlineExceededException(status, trailers);
      case UNAVAILABLE:
        return new ConnectionException(status, trailers);
      case RESOURCE_EXHAUSTED:
        return new ResourceExhaustedException(status, trailers);

        // Dgraph server sets these explicitly
      case UNAUTHENTICATED:
      case PERMISSION_DENIED:
        return new AuthException(status, trailers);
      case ABORTED:
      case FAILED_PRECONDITION:
        return new TxnConflictException(status, trailers);

        // Dgraph sends many semantically distinct errors as UNKNOWN — fall through to message
        // matching
      case UNKNOWN:
        return translateUnknown(status, trailers);

      default:
        return new DgraphException(status, trailers);
    }
  }

  /**
   * Fallback translation for UNKNOWN status errors. The Dgraph server returns UNKNOWN for any Go
   * error not explicitly wrapped with a gRPC status code. We match on message content to provide
   * more specific exception types where possible.
   */
  private static DgraphException translateUnknown(Status status, Metadata trailers) {
    String desc = status.getDescription();
    if (desc == null) {
      return new DgraphException(status, trailers);
    }
    String lower = desc.toLowerCase();

    // Server shutting down — will not become ready, try a different node
    if (lower.contains("draining mode")) {
      return new AlphaShutdownException(status, trailers);
    }

    // Server not yet ready or temporarily blocked — will resolve on its own
    if (lower.contains("not ready")
        || lower.contains("errindexinginprogress")
        || lower.contains("raft isn't initialized yet")) {
      return new AlphaNotReadyException(status, trailers);
    }

    // Server overloaded — back off and retry
    if (lower.contains("overloaded") || lower.contains("too many requests")) {
      return new AlphaOverloadedException(status, trailers);
    }

    // Network/connection issues surfaced as UNKNOWN
    if (lower.contains("no connection") || lower.contains("unhealthy connection")) {
      return new ConnectionException(status, trailers);
    }

    // Server policy rejects a well-formed request
    if (lower.contains("no mutations allowed")
        || lower.contains("drop all operation is not permitted")) {
      return new DisallowedOperationException(status, trailers);
    }

    // Query/schema parsing errors
    if (lower.contains("invalid syntax")
        || lower.contains("invalid input")
        || lower.contains("while lexing")) {
      return new QueryException(status, trailers);
    }

    // No message match — generic wrapper
    return new DgraphException(status, trailers);
  }

  public static boolean isJwtExpired(Throwable e) {
    if (!(e instanceof StatusRuntimeException)) {
      return false;
    }
    StatusRuntimeException sre = (StatusRuntimeException) e;
    Status.Code code = sre.getStatus().getCode();
    boolean isExpired = sre.getMessage().contains("Token is expired");
    return isExpired
        && (code.equals(Status.Code.UNAUTHENTICATED) || code.equals(Status.Code.UNKNOWN));
  }
}
