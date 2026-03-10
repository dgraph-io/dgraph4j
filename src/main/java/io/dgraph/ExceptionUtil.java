/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * @deprecated Use {@link Exceptions} instead. This class will be removed in a future release.
 */
@Deprecated
public class ExceptionUtil {

  private ExceptionUtil() {}

  /** @deprecated Use {@link Exceptions#unwrapException(CompletionException)} instead. */
  @Deprecated
  public static RuntimeException unwrapException(CompletionException ex) {
    return Exceptions.unwrapException(ex);
  }

  /** @deprecated Use {@link Exceptions#withExceptionUnwrapped(Supplier)} instead. */
  @Deprecated
  public static <R> R withExceptionUnwrapped(Supplier<R> s) {
    return Exceptions.withExceptionUnwrapped(s);
  }

  /** @deprecated Use {@link Exceptions#withExceptionUnwrapped(Runnable)} instead. */
  @Deprecated
  public static void withExceptionUnwrapped(Runnable r) {
    Exceptions.withExceptionUnwrapped(r);
  }

  /** @deprecated Use {@link Exceptions#translate(Throwable)} instead. */
  @Deprecated
  public static DgraphException translate(Throwable t) {
    return Exceptions.translate(t);
  }

  /** @deprecated Use {@link Exceptions#isJwtExpired(Throwable)} instead. */
  @Deprecated
  public static boolean isJwtExpired(Throwable e) {
    return Exceptions.isJwtExpired(e);
  }
}
