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
public class ExceptionUtil {

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
   * Finds a StatusRuntimeException in the cause chain of the given throwable. Returns null if none
   * is found. Guards against circular cause chains.
   */
  public static StatusRuntimeException findStatusRuntimeException(Throwable t) {
    Throwable cause = t;
    int depth = 0;
    while (cause != null && depth < 20) {
      if (cause instanceof StatusRuntimeException) {
        return (StatusRuntimeException) cause;
      }
      Throwable next = cause.getCause();
      if (next == cause) {
        break; // self-referencing cause chain
      }
      cause = next;
      depth++;
    }
    return null;
  }

  /**
   * Translates a Throwable into a typed DgraphException. If the throwable is already a
   * DgraphException, returns it as-is. If it contains a StatusRuntimeException in its cause chain,
   * maps it to the appropriate typed exception. Otherwise wraps it in a generic DgraphException.
   */
  public static DgraphException translate(Throwable t) {
    // Unwrap CompletionException to get at the real cause
    if (t instanceof CompletionException && t.getCause() != null) {
      return translate(t.getCause());
    }

    if (t instanceof DgraphException) {
      return (DgraphException) t;
    }

    if (t instanceof InterruptedException) {
      return new DgraphInterruptedException(t.getMessage(), (InterruptedException) t);
    }

    StatusRuntimeException sre = findStatusRuntimeException(t);
    if (sre != null) {
      return fromStatusRuntimeException(sre);
    }

    return new DgraphException(t.getMessage(), t);
  }

  private static DgraphException fromStatusRuntimeException(StatusRuntimeException sre) {
    Status status = sre.getStatus();
    Metadata trailers = sre.getTrailers();

    switch (status.getCode()) {
      case DEADLINE_EXCEEDED:
        return new DgraphDeadlineExceededException(status, trailers);
      case UNAVAILABLE:
        return new DgraphConnectionException(status, trailers);
      case RESOURCE_EXHAUSTED:
        return new DgraphResourceExhaustedException(status, trailers);
      case UNAUTHENTICATED:
      case PERMISSION_DENIED:
        return new DgraphAuthException(status, trailers);
      case ABORTED:
      case FAILED_PRECONDITION:
        return new TxnConflictException(status, trailers);
      case UNKNOWN:
        return translateUnknown(status, trailers);
      default:
        return new DgraphException(status, trailers);
    }
  }

  private static DgraphException translateUnknown(Status status, Metadata trailers) {
    String desc = status.getDescription();
    if (desc == null) {
      return new DgraphException(status, trailers);
    }
    String lower = desc.toLowerCase();

    // Connection/availability patterns
    if (lower.contains("not ready")
        || lower.contains("no connection")
        || lower.contains("unhealthy connection")) {
      return new DgraphConnectionException(status, trailers);
    }

    // Server overload
    if (lower.contains("overloaded")) {
      return new DgraphOverloadedException(status, trailers);
    }

    // Query/input errors
    if (lower.contains("invalid syntax")
        || lower.contains("invalid input")
        || lower.contains("while lexing")) {
      return new DgraphQueryException(status, trailers);
    }

    // Fallback
    return new DgraphException(status, trailers);
  }

  public static boolean isJwtExpired(Throwable e) {
    StatusRuntimeException sre = findStatusRuntimeException(e);
    if (sre != null) {
      Status.Code code = sre.getStatus().getCode();
      boolean isExpired = sre.getMessage().contains("Token is expired");
      return isExpired
          && (code.equals(Status.Code.UNAUTHENTICATED) || code.equals(Status.Code.UNKNOWN));
    }
    return false;
  }
}
