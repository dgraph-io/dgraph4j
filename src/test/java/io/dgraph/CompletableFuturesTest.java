/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.testng.annotations.Test;

public class CompletableFuturesTest {

  private static <T> CompletableFuture<T> failed(Throwable t) {
    CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(t);
    return f;
  }

  private static StatusRuntimeException jwtExpired() {
    return Status.UNAUTHENTICATED.withDescription("Token is expired").asRuntimeException();
  }

  private static StatusRuntimeException unavailable() {
    return Status.UNAVAILABLE.withDescription("connection refused").asRuntimeException();
  }

  private static final Supplier<CompletableFuture<Void>> NO_RETRY_NEEDED =
      () -> CompletableFuture.completedFuture(null);

  // Regression test for #293: an in-flight call must not hold an executor thread hostage.
  // On the old blocking implementation the lone executor thread parks and the marker never runs.
  @Test
  public void inFlightCallDoesNotHoldExecutorThread() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      for (int i = 0; i < 3; i++) {
        CompletableFuture<String> hung = new CompletableFuture<>();
        CompletableFutures.runWithRetries("op", () -> hung, NO_RETRY_NEEDED, executor);
      }
      CountDownLatch marker = new CountDownLatch(1);
      executor.execute(marker::countDown);
      assertTrue(
          marker.await(2, TimeUnit.SECONDS),
          "executor thread was starved by an in-flight gRPC call");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void successPassesThrough() throws Exception {
    Executor executor = ForkJoinPool.commonPool();
    Callable<CompletableFuture<String>> callable =
        () -> CompletableFuture.completedFuture("ok");

    CompletableFuture<String> result =
        CompletableFutures.runWithRetries("op", callable, NO_RETRY_NEEDED, executor);

    assertEquals(result.get(2, TimeUnit.SECONDS), "ok");
  }

  @Test
  public void nullFutureFromCallableIsTranslated() throws Exception {
    Executor executor = ForkJoinPool.commonPool();
    Callable<CompletableFuture<String>> callable = () -> null;

    CompletableFuture<String> result =
        CompletableFutures.runWithRetries("op", callable, NO_RETRY_NEEDED, executor);

    try {
      result.get(2, TimeUnit.SECONDS);
      fail("expected failure");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof DgraphException, "cause was " + e.getCause());
    }
  }

  @Test
  public void nonJwtErrorIsTranslatedAndNotRetried() throws Exception {
    Executor executor = ForkJoinPool.commonPool();
    AtomicInteger logins = new AtomicInteger();
    Supplier<CompletableFuture<Void>> retryLogin =
        () -> {
          logins.incrementAndGet();
          return CompletableFuture.completedFuture(null);
        };
    Callable<CompletableFuture<String>> callable = () -> failed(unavailable());

    CompletableFuture<String> result =
        CompletableFutures.runWithRetries("op", callable, retryLogin, executor);

    try {
      result.get(2, TimeUnit.SECONDS);
      fail("expected failure");
    } catch (ExecutionException e) {
      assertTrue(
          e.getCause() instanceof ConnectionException, "cause was " + e.getCause());
    }
    assertEquals(logins.get(), 0, "retryLogin must not run for a non-JWT error");
  }

  @Test
  public void jwtExpiryTriggersRetryThenSucceeds() throws Exception {
    Executor executor = ForkJoinPool.commonPool();
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger logins = new AtomicInteger();
    Callable<CompletableFuture<String>> callable =
        () -> {
          if (calls.incrementAndGet() == 1) {
            return failed(jwtExpired());
          }
          return CompletableFuture.completedFuture("ok");
        };
    Supplier<CompletableFuture<Void>> retryLogin =
        () -> {
          logins.incrementAndGet();
          return CompletableFuture.completedFuture(null);
        };

    CompletableFuture<String> result =
        CompletableFutures.runWithRetries("op", callable, retryLogin, executor);

    assertEquals(result.get(2, TimeUnit.SECONDS), "ok");
    assertEquals(calls.get(), 2, "callable should be invoked twice (original + retry)");
    assertEquals(logins.get(), 1, "retryLogin should run exactly once");
  }

  @Test
  public void jwtExpiryRetryFailureIsTranslated() throws Exception {
    Executor executor = ForkJoinPool.commonPool();
    AtomicInteger calls = new AtomicInteger();
    Callable<CompletableFuture<String>> callable =
        () -> {
          if (calls.incrementAndGet() == 1) {
            return failed(jwtExpired());
          }
          return failed(unavailable());
        };

    CompletableFuture<String> result =
        CompletableFutures.runWithRetries("op", callable, NO_RETRY_NEEDED, executor);

    try {
      result.get(2, TimeUnit.SECONDS);
      fail("expected failure");
    } catch (ExecutionException e) {
      assertTrue(
          e.getCause() instanceof ConnectionException, "cause was " + e.getCause());
    }
    assertEquals(calls.get(), 2);
  }

  // The callback executor is the documented completion thread for every path, including the
  // JWT-retry path, whose gRPC future completes on a channel thread we do not control.
  @Test
  public void retryPathCompletesOnCallbackExecutor() throws Exception {
    ExecutorService executor =
        Executors.newSingleThreadExecutor(r -> new Thread(r, "callback-executor"));
    try {
      CompletableFuture<String> first = new CompletableFuture<>();
      CompletableFuture<String> retry = new CompletableFuture<>();
      CountDownLatch retryInvoked = new CountDownLatch(1);
      AtomicInteger calls = new AtomicInteger();
      Callable<CompletableFuture<String>> callable =
          () -> {
            if (calls.incrementAndGet() == 1) {
              return first;
            }
            retryInvoked.countDown();
            return retry;
          };

      CompletableFuture<String> completedOn =
          CompletableFutures.runWithRetries("op", callable, NO_RETRY_NEEDED, executor)
              .thenApply(ignored -> Thread.currentThread().getName());

      first.completeExceptionally(jwtExpired());
      assertTrue(retryInvoked.await(2, TimeUnit.SECONDS), "retry was never attempted");
      // FIFO on a single-thread executor: by the time this task runs, runWithRetries has
      // composed on the retry future, so completing it off-executor exercises the hop back.
      executor.execute(() -> new Thread(() -> retry.complete("ok"), "grpc-thread").start());

      assertEquals(completedOn.get(2, TimeUnit.SECONDS), "callback-executor");
    } finally {
      executor.shutdownNow();
    }
  }

  // attemptAsync's backoff used delayedExecutor(delay, unit), which targets the common pool and
  // ignored the executor the client was built with. Nothing here contacts a server.
  @Test
  public void retryBackoffAndCompletionRunOnSuppliedExecutor() throws Exception {
    ExecutorService executor =
        Executors.newSingleThreadExecutor(r -> new Thread(r, "retry-executor"));
    ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:1").usePlaintext().build();
    try {
      DgraphAsyncClient client = new DgraphAsyncClient(executor, DgraphGrpc.newStub(channel));
      RetryPolicy policy =
          RetryPolicy.builder().maxRetries(2).baseDelay(Duration.ofMillis(10)).jitter(0).build();

      AtomicInteger attempts = new AtomicInteger();
      List<String> attemptThreads = Collections.synchronizedList(new ArrayList<>());
      CompletableFuture<String> secondAttempt = new CompletableFuture<>();
      CountDownLatch retryStarted = new CountDownLatch(1);
      AsyncTransactionOp<String> op =
          txn -> {
            attemptThreads.add(Thread.currentThread().getName());
            if (attempts.incrementAndGet() == 1) {
              return failed(unavailable());
            }
            retryStarted.countDown();
            return secondAttempt;
          };

      CompletableFuture<String> result =
          CompletableFutures.attemptAsync(policy, op, 0, client::newTransaction, executor);
      CompletableFuture<String> completedOn =
          result.thenApply(ignored -> Thread.currentThread().getName());

      assertTrue(retryStarted.await(5, TimeUnit.SECONDS), "the retry never ran");
      // FIFO on the single-thread executor: this task runs after attemptAsync composed on
      // secondAttempt, so completing it off-executor exercises the hop back.
      executor.execute(
          () -> new Thread(() -> secondAttempt.complete("ok"), "grpc-thread").start());

      assertEquals(result.get(5, TimeUnit.SECONDS), "ok");
      assertEquals(attempts.get(), 2, "the retryable failure should be retried once");
      assertEquals(attemptThreads.get(1), "retry-executor", "backoff ran off the given executor");
      assertEquals(completedOn.get(5, TimeUnit.SECONDS), "retry-executor");
    } finally {
      channel.shutdownNow();
      executor.shutdownNow();
    }
  }

  @Test
  public void retryLoginFailureIsTranslated() throws Exception {
    Executor executor = ForkJoinPool.commonPool();
    AtomicInteger calls = new AtomicInteger();
    Callable<CompletableFuture<String>> callable =
        () -> {
          calls.incrementAndGet();
          return failed(jwtExpired());
        };
    Supplier<CompletableFuture<Void>> retryLogin =
        () -> failed(new RuntimeException("refresh failed"));

    CompletableFuture<String> result =
        CompletableFutures.runWithRetries("op", callable, retryLogin, executor);

    try {
      result.get(2, TimeUnit.SECONDS);
      fail("expected failure");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof DgraphException, "cause was " + e.getCause());
    }
    assertEquals(calls.get(), 1, "callable must not be retried when login refresh fails");
  }
}
