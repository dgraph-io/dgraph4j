/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;

public class ExceptionMappingTest {

  private StatusRuntimeException sre(Status.Code code, String description) {
    return code.toStatus().withDescription(description).asRuntimeException();
  }

  // --- Status code based mapping ---

  @Test
  public void testDeadlineExceeded() {
    StatusRuntimeException cause = sre(Status.Code.DEADLINE_EXCEEDED, "context deadline exceeded");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof DeadlineExceededException);
    assertEquals(ex.getStatus().getCode(), Status.Code.DEADLINE_EXCEEDED);
    assertEquals(ex.getStatus().getDescription(), "context deadline exceeded");
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testCallOptionsDeadlineExceeded() {
    StatusRuntimeException cause =
        sre(Status.Code.DEADLINE_EXCEEDED, "CallOptions deadline exceeded after 299.99999s");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof DeadlineExceededException);
  }

  @Test
  public void testUnavailableGoaway() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNAVAILABLE,
            "Abrupt GOAWAY closed sent stream. HTTP/2 error code: NO_ERROR");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof ConnectionException);
    assertEquals(ex.getStatus().getCode(), Status.Code.UNAVAILABLE);
  }

  @Test
  public void testUnavailableChannelShutdown() {
    StatusRuntimeException cause = sre(Status.Code.UNAVAILABLE, "Channel shutdown invoked");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof ConnectionException);
  }

  @Test
  public void testUnavailableRstStream() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNAVAILABLE,
            "RST_STREAM closed stream. HTTP/2 error code: REFUSED_STREAM");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof ConnectionException);
  }

  @Test
  public void testResourceExhausted() {
    StatusRuntimeException cause =
        sre(Status.Code.RESOURCE_EXHAUSTED, "gRPC message exceeds maximum size 4194304");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof ResourceExhaustedException);
    assertFalse(ex.isRetryable());
  }

  @Test
  public void testUnauthenticated() {
    StatusRuntimeException cause = sre(Status.Code.UNAUTHENTICATED, "Token is expired");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof AuthException);
  }

  @Test
  public void testPermissionDenied() {
    StatusRuntimeException cause = sre(Status.Code.PERMISSION_DENIED, "not authorized");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof AuthException);
  }

  @Test
  public void testAborted() {
    StatusRuntimeException cause = sre(Status.Code.ABORTED, "Transaction conflict");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof TxnConflictException);
    assertTrue(ex.isRetryable());
    assertEquals(ex.getStatus().getCode(), Status.Code.ABORTED);
  }

  @Test
  public void testFailedPrecondition() {
    StatusRuntimeException cause = sre(Status.Code.FAILED_PRECONDITION, "Transaction conflict");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof TxnConflictException);
  }

  // --- UNKNOWN status with message matching ---

  @Test
  public void testUnknownServerNotReady() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNKNOWN,
            "Please retry again, server is not ready to accept requests.");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof AlphaNotReadyException);
    assertTrue(ex instanceof AlphaException);
    assertEquals(ex.getStatus().getCode(), Status.Code.UNKNOWN);
  }

  @Test
  public void testUnknownDrainingMode() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNKNOWN,
            "the server is in draining mode, no new transaction is accepted");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof AlphaShutdownException);
    assertTrue(ex instanceof AlphaException);
    // Not a AlphaNotReadyException — server won't become ready
    assertFalse(ex instanceof AlphaNotReadyException);
  }

  @Test
  public void testUnknownIndexingInProgress() {
    StatusRuntimeException cause =
        sre(Status.Code.UNKNOWN, "errIndexingInProgress. Please retry");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof AlphaNotReadyException);
  }

  @Test
  public void testUnknownNoConnection() {
    StatusRuntimeException cause = sre(Status.Code.UNKNOWN, "No connection exists");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof ConnectionException);
  }

  @Test
  public void testUnknownPredicateNoConnection() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNKNOWN,
            "cannot retrieve predicate information: No connection exists");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof ConnectionException);
  }

  @Test
  public void testUnknownUnhealthyConnection() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNKNOWN,
            "dispatchTaskOverNetwork: while retrieving connection.: Unhealthy connection");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof ConnectionException);
  }

  @Test
  public void testUnknownRaftNotInitialized() {
    StatusRuntimeException cause =
        sre(Status.Code.UNKNOWN, "Raft isn't initialized yet");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof AlphaNotReadyException);
  }

  @Test
  public void testUnknownOverloaded() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNKNOWN,
            "Server overloaded with pending proposals. Please retry later");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof AlphaOverloadedException);
    assertTrue(ex instanceof AlphaException);
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testUnknownTooManyRequests() {
    StatusRuntimeException cause =
        sre(Status.Code.UNKNOWN, "429 Too Many Requests. Please throttle your requests");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof AlphaOverloadedException);
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testUnknownNoMutationsAllowed() {
    StatusRuntimeException cause =
        sre(Status.Code.UNKNOWN, "No mutations allowed by server.");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof DisallowedOperationException);
    assertFalse(ex.isRetryable());
  }

  @Test
  public void testUnknownDropNotPermitted() {
    StatusRuntimeException cause =
        sre(Status.Code.UNKNOWN, "Drop all operation is not permitted.");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof DisallowedOperationException);
    assertFalse(ex.isRetryable());
  }

  @Test
  public void testUnknownInvalidSyntax() {
    StatusRuntimeException cause =
        sre(Status.Code.UNKNOWN, "while unquoting: invalid syntax");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof QueryException);
  }

  @Test
  public void testUnknownInvalidInput() {
    StatusRuntimeException cause = sre(Status.Code.UNKNOWN, "Invalid input");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof QueryException);
  }

  @Test
  public void testUnknownLexingError() {
    StatusRuntimeException cause =
        sre(Status.Code.UNKNOWN, "while lexing query: unexpected character");
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof QueryException);
  }

  @Test
  public void testUnknownFallsBackToGeneric() {
    StatusRuntimeException cause = sre(Status.Code.UNKNOWN, "something completely unexpected");
    DgraphException ex = Exceptions.translate(cause);
    assertEquals(ex.getClass(), DgraphException.class);
    assertEquals(ex.getStatus().getCode(), Status.Code.UNKNOWN);
  }

  // --- Passthrough and wrapping ---

  @Test
  public void testAlreadyDgraphExceptionPassedThrough() {
    DgraphException original = new DgraphException("already typed");
    DgraphException result = Exceptions.translate(original);
    assertSame(result, original);
  }

  @Test
  public void testNestedStatusRuntimeExceptionNotUnwrapped() {
    // translate() does not walk cause chains — only direct StatusRuntimeException is mapped
    StatusRuntimeException sre = sre(Status.Code.DEADLINE_EXCEEDED, "timeout");
    RuntimeException wrapper = new RuntimeException("outer", sre);
    DgraphException ex = Exceptions.translate(wrapper);
    assertEquals(ex.getClass(), DgraphException.class);
    assertSame(ex.getCause(), wrapper);
  }

  @Test
  public void testNonGrpcExceptionWrapped() {
    RuntimeException cause = new RuntimeException("something else");
    DgraphException ex = Exceptions.translate(cause);
    assertEquals(ex.getClass(), DgraphException.class);
    assertSame(ex.getCause(), cause);
    assertEquals(ex.getStatus().getCode(), Status.Code.INTERNAL);
  }

  @Test
  public void testInterruptedExceptionWrapped() {
    InterruptedException cause = new InterruptedException("interrupted");
    DgraphException ex = Exceptions.translate(cause);
    assertEquals(ex.getClass(), DgraphException.class);
    assertSame(ex.getCause(), cause);
  }

  @Test
  public void testTrailersPreserved() {
    Metadata trailers = new Metadata();
    Metadata.Key<String> key =
        Metadata.Key.of("custom-key", Metadata.ASCII_STRING_MARSHALLER);
    trailers.put(key, "custom-value");
    StatusRuntimeException cause =
        Status.UNAVAILABLE.withDescription("down").asRuntimeException(trailers);
    DgraphException ex = Exceptions.translate(cause);
    assertTrue(ex instanceof ConnectionException);
    assertNotNull(ex.getTrailers());
  }

  @Test
  public void testUnknownWithNullDescription() {
    StatusRuntimeException cause = Status.UNKNOWN.asRuntimeException();
    DgraphException ex = Exceptions.translate(cause);
    assertEquals(ex.getClass(), DgraphException.class);
    assertEquals(ex.getStatus().getCode(), Status.Code.UNKNOWN);
  }

  @Test
  public void testUnrecognizedStatusCode() {
    StatusRuntimeException cause =
        sre(Status.Code.DATA_LOSS, "some data loss");
    DgraphException ex = Exceptions.translate(cause);
    assertEquals(ex.getClass(), DgraphException.class);
    assertEquals(ex.getStatus().getCode(), Status.Code.DATA_LOSS);
  }

  // --- CompletionException unwrapping ---

  @Test
  public void testCompletionExceptionUnwrapsDgraphException() {
    TxnFinishedException original = new TxnFinishedException();
    CompletionException ce = new CompletionException(original);
    DgraphException ex = Exceptions.translate(ce);
    assertSame(ex, original);
  }

  @Test
  public void testCompletionExceptionUnwrapsStatusRuntimeException() {
    StatusRuntimeException cause = sre(Status.Code.DEADLINE_EXCEEDED, "timeout");
    CompletionException ce = new CompletionException(cause);
    DgraphException ex = Exceptions.translate(ce);
    assertTrue(ex instanceof DeadlineExceededException);
  }

  @Test
  public void testCompletionExceptionWithNullCauseFallsThrough() {
    CompletionException ce = new CompletionException(null);
    DgraphException ex = Exceptions.translate(ce);
    // CompletionException with null cause is treated as non-gRPC exception
    assertEquals(ex.getClass(), DgraphException.class);
  }

}
