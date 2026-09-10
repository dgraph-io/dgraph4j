/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A JWT refresh attempted without a refresh token must fail as an auth error. No RPC is issued, so
 * these need no server: the missing-token check short-circuits before the login call.
 */
public class RetryLoginTest {

  private ManagedChannel channel;
  private ExecutorService executor;
  private DgraphAsyncClient client;

  @BeforeMethod
  public void setUp() {
    channel = ManagedChannelBuilder.forTarget("localhost:1").usePlaintext().build();
    executor = Executors.newSingleThreadExecutor();
    client = new DgraphAsyncClient(executor, DgraphGrpc.newStub(channel));
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    channel.shutdownNow();
    executor.shutdownNow();
  }

  @Test
  public void refreshWithoutATokenFailsWithAuthException() {
    CompletableFuture<Void> refreshed = client.retryLogin();

    assertTrue(refreshed.isCompletedExceptionally(), "the refresh should fail immediately");
    try {
      refreshed.join();
      fail("expected the refresh to fail");
    } catch (RuntimeException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      assertTrue(cause instanceof AuthException, "cause was " + cause);
      assertEquals(
          Status.fromThrowable(cause).getCode(),
          Status.Code.UNAUTHENTICATED,
          "a missing refresh token is an authentication failure, not an internal error");
    }
  }

  // The user-visible path: an expired token with no way to refresh it must surface as AuthException
  // rather than a generic DgraphException.
  @Test
  public void expiredTokenWithNoRefreshSurfacesAuthException() throws Exception {
    Callable<CompletableFuture<String>> expired =
        () ->
            CompletableFuture.failedFuture(
                Status.UNAUTHENTICATED
                    .withDescription("Token is expired")
                    .asRuntimeException());

    CompletableFuture<String> result =
        CompletableFutures.runWithRetries("op", expired, client::retryLogin, executor);

    try {
      result.get(5, TimeUnit.SECONDS);
      fail("expected the operation to fail");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof AuthException, "cause was " + e.getCause());
    }
  }
}
