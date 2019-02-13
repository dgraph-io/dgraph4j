package io.dgraph;

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
}
