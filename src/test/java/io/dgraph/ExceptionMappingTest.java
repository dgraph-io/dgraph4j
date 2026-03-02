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
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphDeadlineExceededException);
    assertEquals(ex.getStatus().getCode(), Status.Code.DEADLINE_EXCEEDED);
    assertEquals(ex.getStatus().getDescription(), "context deadline exceeded");
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testCallOptionsDeadlineExceeded() {
    StatusRuntimeException cause =
        sre(Status.Code.DEADLINE_EXCEEDED, "CallOptions deadline exceeded after 299.99999s");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphDeadlineExceededException);
  }

  @Test
  public void testUnavailableGoaway() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNAVAILABLE,
            "Abrupt GOAWAY closed sent stream. HTTP/2 error code: NO_ERROR");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphConnectionException);
    assertEquals(ex.getStatus().getCode(), Status.Code.UNAVAILABLE);
  }

  @Test
  public void testUnavailableChannelShutdown() {
    StatusRuntimeException cause = sre(Status.Code.UNAVAILABLE, "Channel shutdown invoked");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphConnectionException);
  }

  @Test
  public void testUnavailableRstStream() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNAVAILABLE,
            "RST_STREAM closed stream. HTTP/2 error code: REFUSED_STREAM");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphConnectionException);
  }

  @Test
  public void testResourceExhausted() {
    StatusRuntimeException cause =
        sre(Status.Code.RESOURCE_EXHAUSTED, "gRPC message exceeds maximum size 4194304");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphResourceExhaustedException);
    assertFalse(ex.isRetryable());
  }

  @Test
  public void testUnauthenticated() {
    StatusRuntimeException cause = sre(Status.Code.UNAUTHENTICATED, "Token is expired");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphAuthException);
  }

  @Test
  public void testPermissionDenied() {
    StatusRuntimeException cause = sre(Status.Code.PERMISSION_DENIED, "not authorized");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphAuthException);
  }

  @Test
  public void testAborted() {
    StatusRuntimeException cause = sre(Status.Code.ABORTED, "Transaction conflict");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof TxnConflictException);
    assertTrue(ex.isRetryable());
    assertEquals(ex.getStatus().getCode(), Status.Code.ABORTED);
  }

  @Test
  public void testFailedPrecondition() {
    StatusRuntimeException cause = sre(Status.Code.FAILED_PRECONDITION, "Transaction conflict");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof TxnConflictException);
  }

  // --- UNKNOWN status with message matching ---

  @Test
  public void testUnknownServerNotReady() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNKNOWN,
            "Please retry again, server is not ready to accept requests.");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphConnectionException);
    assertEquals(ex.getStatus().getCode(), Status.Code.UNKNOWN);
  }

  @Test
  public void testUnknownNoConnection() {
    StatusRuntimeException cause = sre(Status.Code.UNKNOWN, "No connection exists");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphConnectionException);
  }

  @Test
  public void testUnknownPredicateNoConnection() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNKNOWN,
            "cannot retrieve predicate information: No connection exists");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphConnectionException);
  }

  @Test
  public void testUnknownUnhealthyConnection() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNKNOWN,
            "dispatchTaskOverNetwork: while retrieving connection.: Unhealthy connection");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphConnectionException);
  }

  @Test
  public void testUnknownOverloaded() {
    StatusRuntimeException cause =
        sre(
            Status.Code.UNKNOWN,
            "Server overloaded with pending proposals. Please retry later");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphOverloadedException);
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testUnknownInvalidSyntax() {
    StatusRuntimeException cause =
        sre(Status.Code.UNKNOWN, "while unquoting: invalid syntax");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphQueryException);
  }

  @Test
  public void testUnknownInvalidInput() {
    StatusRuntimeException cause = sre(Status.Code.UNKNOWN, "Invalid input");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphQueryException);
  }

  @Test
  public void testUnknownLexingError() {
    StatusRuntimeException cause =
        sre(Status.Code.UNKNOWN, "while lexing query: unexpected character");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphQueryException);
  }

  @Test
  public void testUnknownFallsBackToGeneric() {
    StatusRuntimeException cause = sre(Status.Code.UNKNOWN, "something completely unexpected");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertEquals(ex.getClass(), DgraphException.class);
    assertEquals(ex.getStatus().getCode(), Status.Code.UNKNOWN);
  }

  // --- Passthrough and wrapping ---

  @Test
  public void testAlreadyDgraphExceptionPassedThrough() {
    DgraphException original = new DgraphException("already typed");
    DgraphException result = ExceptionUtil.translate(original);
    assertSame(result, original);
  }

  @Test
  public void testNestedStatusRuntimeException() {
    StatusRuntimeException sre = sre(Status.Code.DEADLINE_EXCEEDED, "timeout");
    RuntimeException wrapper = new RuntimeException("outer", sre);
    DgraphException ex = ExceptionUtil.translate(wrapper);
    assertTrue(ex instanceof DgraphDeadlineExceededException);
    assertEquals(ex.getStatus().getCode(), Status.Code.DEADLINE_EXCEEDED);
  }

  @Test
  public void testNonGrpcExceptionWrapped() {
    RuntimeException cause = new RuntimeException("something else");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertEquals(ex.getClass(), DgraphException.class);
    assertSame(ex.getCause(), cause);
    assertEquals(ex.getStatus().getCode(), Status.Code.INTERNAL);
  }

  @Test
  public void testInterruptedExceptionMapped() {
    InterruptedException cause = new InterruptedException("interrupted");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphInterruptedException);
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
    DgraphException ex = ExceptionUtil.translate(cause);
    assertTrue(ex instanceof DgraphConnectionException);
    assertNotNull(ex.getTrailers());
  }

  @Test
  public void testUnknownWithNullDescription() {
    StatusRuntimeException cause = Status.UNKNOWN.asRuntimeException();
    DgraphException ex = ExceptionUtil.translate(cause);
    assertEquals(ex.getClass(), DgraphException.class);
    assertEquals(ex.getStatus().getCode(), Status.Code.UNKNOWN);
  }

  @Test
  public void testUnrecognizedStatusCode() {
    StatusRuntimeException cause =
        sre(Status.Code.DATA_LOSS, "some data loss");
    DgraphException ex = ExceptionUtil.translate(cause);
    assertEquals(ex.getClass(), DgraphException.class);
    assertEquals(ex.getStatus().getCode(), Status.Code.DATA_LOSS);
  }

  // --- CompletionException unwrapping ---

  @Test
  public void testCompletionExceptionUnwrapsDgraphException() {
    TxnFinishedException original = new TxnFinishedException();
    CompletionException ce = new CompletionException(original);
    DgraphException ex = ExceptionUtil.translate(ce);
    assertSame(ex, original);
  }

  @Test
  public void testCompletionExceptionUnwrapsStatusRuntimeException() {
    StatusRuntimeException cause = sre(Status.Code.DEADLINE_EXCEEDED, "timeout");
    CompletionException ce = new CompletionException(cause);
    DgraphException ex = ExceptionUtil.translate(ce);
    assertTrue(ex instanceof DgraphDeadlineExceededException);
  }

  @Test
  public void testCompletionExceptionWithNullCauseFallsThrough() {
    CompletionException ce = new CompletionException(null);
    DgraphException ex = ExceptionUtil.translate(ce);
    // CompletionException with null cause is treated as non-gRPC exception
    assertEquals(ex.getClass(), DgraphException.class);
  }

  // --- Cause chain safety ---

  @Test
  public void testFindStatusRuntimeExceptionWithNullChain() {
    RuntimeException cause = new RuntimeException("no sre in chain");
    assertNull(ExceptionUtil.findStatusRuntimeException(cause));
  }

  @Test
  public void testFindStatusRuntimeExceptionNull() {
    assertNull(ExceptionUtil.findStatusRuntimeException(null));
  }
}
