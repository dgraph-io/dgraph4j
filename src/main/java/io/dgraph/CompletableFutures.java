/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Context;
import java.util.concurrent.*;
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

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return ctxCallable.call().get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("The " + operation + " got interrupted:", e);
            throw new DgraphException("The " + operation + " got interrupted", e);
          } catch (ExecutionException e) {
            if (Exceptions.isJwtExpired(e.getCause())) {
              try {
                retryLogin.get().get();
                return ctxCallable.call().get();
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("The retried " + operation + " got interrupted:", ie);
                throw new DgraphException(
                    "The retried " + operation + " got interrupted", ie);
              } catch (ExecutionException ie) {
                LOG.error(
                    "The retried " + operation + " encounters an execution exception:", ie);
                throw new CompletionException(Exceptions.translate(ie.getCause()));
              } catch (Exception ie) {
                LOG.error(
                    "The retried " + operation + " encounters a completion exception:", ie);
                throw new CompletionException(Exceptions.translate(ie));
              }
            }
            throw new CompletionException(Exceptions.translate(e.getCause()));
          } catch (Exception e) {
            throw new CompletionException(Exceptions.translate(e));
          }
        },
        executor);
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
   * @return a CompletableFuture that completes with the result or fails after exhausting retries
   */
  static <T> CompletableFuture<T> attemptAsync(
      RetryPolicy policy,
      AsyncTransactionOp<T> op,
      int attempt,
      Supplier<AsyncTransaction> txnFactory) {

    AsyncTransaction txn = txnFactory.get();
    if (policy.isBestEffort()) {
      txn.setBestEffort(true);
    }

    CompletableFuture<T> result = new CompletableFuture<>();

    op.execute(txn)
        .whenComplete(
            (value, throwable) -> {
              try {
                txn.discard();
              } catch (Exception ignored) {
                // discard is best-effort cleanup
              }

              if (throwable == null) {
                result.complete(value);
                return;
              }

              DgraphException ex = Exceptions.translate(throwable);
              if (!ex.isRetryable() || attempt >= policy.getMaxRetries()) {
                result.completeExceptionally(ex);
                return;
              }

              // Schedule retry after backoff delay
              long delayMs = policy.calculateDelay(attempt);
              Executor delayed =
                  CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS);
              CompletableFuture.supplyAsync(() -> null, delayed)
                  .thenCompose(ignored -> attemptAsync(policy, op, attempt + 1, txnFactory))
                  .whenComplete(
                      (retryValue, retryThrowable) -> {
                        if (retryThrowable != null) {
                          result.completeExceptionally(retryThrowable);
                        } else {
                          result.complete(retryValue);
                        }
                      });
            });

    return result;
  }
}
