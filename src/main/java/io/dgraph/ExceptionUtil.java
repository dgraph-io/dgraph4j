/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

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
      // here we are trying to fish out any Dgraph-specific exceptions and pass them up
      throw unwrapException(ex);
    }
  }

  public static void withExceptionUnwrapped(Runnable r) {
    try {
      r.run();
    } catch (CompletionException ex) {
      // here we are trying to fish out any Dgraph-specific exceptions and pass them up
      throw unwrapException(ex);
    }
  }

  public static boolean isJwtExpired(Throwable e) {
    // search the cause stack to try to find a StatusRuntimeException
    Throwable cause = e;
    while (cause.getCause() != null && !(cause instanceof StatusRuntimeException)) {
      cause = cause.getCause();
    }

    if (cause instanceof StatusRuntimeException) {
      StatusRuntimeException runtimeException = (StatusRuntimeException) cause;
      Status.Code code = runtimeException.getStatus().getCode();
      boolean isExpired = runtimeException.getMessage().contains("Token is expired");
      // Check for both UNAUTHENTICATED and UNKNOWN status codes, as some RPCs
      // (e.g., namespace operations) return UNKNOWN instead of UNAUTHENTICATED
      // when the JWT token has expired.
      return isExpired
          && (code.equals(Status.Code.UNAUTHENTICATED) || code.equals(Status.Code.UNKNOWN));
    }
    return false;
  }
}
