/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Context;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static utility methods for CompletableFuture orchestration patterns used across the Dgraph
 * client.
 */
final class CompletableFutures {
  private static final Logger LOG = LoggerFactory.getLogger(CompletableFutures.class);

  private CompletableFutures() {} // utility class

  /**
   * Executes a CompletableFuture-producing callable with a single JWT-refresh retry. If the first
   * attempt fails with an expired JWT error, {@code retryLogin} is invoked and the callable is
   * retried once.
   *
   * @param <T> the result type
   * @param operation human-readable name used in log messages
   * @param callable the operation to execute (will be wrapped with the current gRPC Context)
   * @param retryLogin supplier that performs a JWT refresh and returns a future that completes when
   *     the refresh is done
   * @param executor the executor on which to run the async work
   * @return a CompletableFuture that completes with the result or fails with a translated exception
   */
  static <T> CompletableFuture<T> runWithRetries(
      String operation,
      Callable<CompletableFuture<T>> callable,
      Supplier<CompletableFuture<Void>> retryLogin,
      Executor executor) {
    final Callable<CompletableFuture<T>> ctxCallable = Context.current().wrap(callable);

    // Composing on the gRPC future rather than blocking on it keeps every stage off the
    // critical path: no thread is parked for the round trip.
    return invoke(operation, ctxCallable)
        .handleAsync(
            (value, error) ->
                classify(operation, value, error, ctxCallable, retryLogin, executor),
            executor)
        .thenCompose(Function.identity())
        .handle(CompletableFutures::translateTerminal);
  }

  /**
   * Upholds the contract that every failure is a {@code CompletionException} wrapping a {@link
   * DgraphException}. Runs synchronously so it still fires when {@code executor} rejects a stage.
   */
  private static <T> T translateTerminal(T value, Throwable error) {
    if (error == null) {
      return value;
    }
    throw new CompletionException(Exceptions.translate(unwrap(error)));
  }

  /** Runs the callable, turning a synchronous throw or a null result into a failed future. */
  private static <T> CompletableFuture<T> invoke(
      String operation, Callable<CompletableFuture<T>> callable) {
    try {
      CompletableFuture<T> future = callable.call();
      if (future != null) {
        return future;
      }
      return CompletableFuture.failedFuture(
          new DgraphException("The " + operation + " returned a null future"));
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  /** Strips one CompletionException wrapper so error classification sees the real cause. */
  private static Throwable unwrap(Throwable t) {
    if (t instanceof CompletionException && t.getCause() != null) {
      return t.getCause();
    }
    return t;
  }

  /**
   * Passes a success through, retries once after a JWT refresh on an expired-token failure, and
   * translates every other failure to {@code CompletionException(DgraphException)}. All paths
   * complete on {@code executor}.
   */
  private static <T> CompletableFuture<T> classify(
      String operation,
      T value,
      Throwable error,
      Callable<CompletableFuture<T>> ctxCallable,
      Supplier<CompletableFuture<Void>> retryLogin,
      Executor executor) {
    if (error == null) {
      return CompletableFuture.completedFuture(value);
    }

    Throwable cause = unwrap(error);
    if (Exceptions.isJwtExpired(cause)) {
      return retryLogin
          .get()
          .thenComposeAsync(ignored -> invoke(operation, ctxCallable), executor)
          .handleAsync(
              (retryValue, retryError) -> {
                if (retryError != null) {
                  LOG.error("The retried {} failed", operation, unwrap(retryError));
                  throw new CompletionException(Exceptions.translate(unwrap(retryError)));
                }
                return retryValue;
              },
              executor);
    }

    return CompletableFuture.failedFuture(new CompletionException(Exceptions.translate(cause)));
  }

  /**
   * Executes an async operation inside a managed transaction with automatic retry on retryable
   * failures, using exponential backoff.
   *
   * @param <T> the result type
   * @param policy the retry policy controlling max retries, delays, and transaction mode
   * @param op the operation to execute within a fresh transaction on each attempt
   * @param attempt the current attempt number (0-based)
   * @param txnFactory creates a new read-write or read-only transaction per the policy
   * @param executor the callback executor; the backoff delay and the returned future's completion
   *     run on it, and no thread is held for the duration of a delay
   * @return a CompletableFuture that completes with the result or fails after exhausting retries
   */
  static <T> CompletableFuture<T> attemptAsync(
      RetryPolicy policy,
      AsyncTransactionOp<T> op,
      int attempt,
      Supplier<AsyncTransaction> txnFactory,
      Executor executor) {

    AsyncTransaction txn = txnFactory.get();
    if (policy.isBestEffort()) {
      txn.setBestEffort(true);
    }

    CompletableFuture<T> result = new CompletableFuture<>();

    // handleAsync rather than whenCompleteAsync: the callback consumes the attempt's outcome, so
    // the stage below carries only a rejection or a bug in the callback, never a retryable failure.
    op.execute(txn)
        .handleAsync(
            (value, throwable) -> {
              try {
                txn.discard();
              } catch (Exception ignored) {
                // discard is best-effort cleanup
              }

              if (throwable == null) {
                result.complete(value);
                return null;
              }

              DgraphException ex = Exceptions.translate(throwable);
              if (!ex.isRetryable() || attempt >= policy.getMaxRetries()) {
                result.completeExceptionally(ex);
                return null;
              }

              long delayMs = policy.calculateDelay(attempt);
              // The timer takes no executor on purpose: delayedExecutor submits from the internal
              // Delayer thread, which swallows a rejection and leaves this future incomplete.
              Executor delayed = CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS);
              CompletableFuture.runAsync(() -> {}, delayed)
                  .thenComposeAsync(
                      ignored -> attemptAsync(policy, op, attempt + 1, txnFactory, executor),
                      executor)
                  .whenComplete(
                      (retryValue, retryThrowable) -> {
                        if (retryThrowable != null) {
                          result.completeExceptionally(Exceptions.translate(retryThrowable));
                        } else {
                          result.complete(retryValue);
                        }
                      });
              return null;
            },
            executor)
        // A rejection by executor completes this stage exceptionally but leaves result untouched,
        // so relay it or the caller waits forever.
        .whenComplete(
            (ignored, t) -> {
              if (t != null) {
                result.completeExceptionally(Exceptions.translate(t));
              }
            });

    return result;
  }
}
