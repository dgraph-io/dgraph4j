/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.testng.annotations.Test;

public class ExceptionHierarchyTest {

  // --- DgraphException base class ---

  @Test
  public void testDgraphExceptionExtendsStatusRuntimeException() {
    DgraphException ex = new DgraphException("test message");
    assertTrue(ex instanceof StatusRuntimeException);
    assertTrue(ex instanceof RuntimeException);
  }

  @Test
  public void testDgraphExceptionMessageOnly() {
    DgraphException ex = new DgraphException("something failed");
    assertEquals(ex.getStatus().getCode(), Status.Code.INTERNAL);
    assertEquals(ex.getStatus().getDescription(), "something failed");
    assertNull(ex.getCause());
    assertFalse(ex.isRetryable());
  }

  @Test
  public void testDgraphExceptionWithCause() {
    RuntimeException cause = new RuntimeException("original");
    DgraphException ex = new DgraphException("wrapped", cause);
    assertEquals(ex.getStatus().getCode(), Status.Code.INTERNAL);
    assertEquals(ex.getStatus().getDescription(), "wrapped");
    assertSame(ex.getCause(), cause);
    assertFalse(ex.isRetryable());
  }

  @Test
  public void testDgraphExceptionWithStatusAndTrailers() {
    Metadata trailers = new Metadata();
    Status status = Status.UNAVAILABLE.withDescription("connection lost");
    DgraphException ex = new DgraphException(status, trailers);
    assertEquals(ex.getStatus().getCode(), Status.Code.UNAVAILABLE);
    assertEquals(ex.getStatus().getDescription(), "connection lost");
    assertSame(ex.getTrailers(), trailers);
  }

  @Test
  public void testDgraphExceptionWithStatusCauseAndTrailers() {
    StatusRuntimeException cause = Status.UNKNOWN.withDescription("err").asRuntimeException();
    DgraphException ex =
        new DgraphException(Status.UNKNOWN.withDescription("err").withCause(cause), null);
    assertSame(ex.getCause(), cause);
  }

  // --- TxnException hierarchy ---

  @Test
  public void testTxnExceptionExtendsDgraphException() {
    TxnConflictException ex = new TxnConflictException("conflict");
    assertTrue(ex instanceof DgraphException);
    assertTrue(ex instanceof TxnException);
    assertTrue(ex instanceof StatusRuntimeException);
    assertTrue(ex instanceof RuntimeException);
  }

  @Test
  public void testTxnConflictIsRetryable() {
    TxnConflictException ex = new TxnConflictException("conflict");
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testTxnConflictUsesAbortedStatus() {
    TxnConflictException ex = new TxnConflictException("conflict");
    assertEquals(ex.getStatus().getCode(), Status.Code.ABORTED);
    assertEquals(ex.getStatus().getDescription(), "conflict");
  }

  @Test
  public void testTxnFinishedNotRetryable() {
    try {
      throw new TxnFinishedException();
    } catch (DgraphException e) {
      assertFalse(e.isRetryable());
      assertTrue(e instanceof TxnFinishedException);
      assertEquals(e.getStatus().getCode(), Status.Code.INTERNAL);
    }
  }

  @Test
  public void testTxnReadOnlyNotRetryable() {
    try {
      throw new TxnReadOnlyException();
    } catch (DgraphException e) {
      assertFalse(e.isRetryable());
      assertTrue(e instanceof TxnReadOnlyException);
      assertEquals(e.getStatus().getCode(), Status.Code.INTERNAL);
    }
  }

  // --- New exception classes ---

  @Test
  public void testConnectionExceptionIsRetryable() {
    DgraphConnectionException ex =
        new DgraphConnectionException(
            Status.UNAVAILABLE.withDescription("no connection"), null);
    assertTrue(ex instanceof DgraphException);
    assertTrue(ex instanceof StatusRuntimeException);
    assertTrue(ex.isRetryable());
    assertEquals(ex.getStatus().getCode(), Status.Code.UNAVAILABLE);
  }

  @Test
  public void testDeadlineExceededIsRetryable() {
    DgraphDeadlineExceededException ex =
        new DgraphDeadlineExceededException(
            Status.DEADLINE_EXCEEDED.withDescription("timeout"), null);
    assertTrue(ex.isRetryable());
    assertEquals(ex.getStatus().getCode(), Status.Code.DEADLINE_EXCEEDED);
  }

  @Test
  public void testOverloadedIsRetryable() {
    DgraphOverloadedException ex =
        new DgraphOverloadedException(
            Status.UNKNOWN.withDescription("overloaded"), null);
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testResourceExhaustedNotRetryable() {
    DgraphResourceExhaustedException ex =
        new DgraphResourceExhaustedException(
            Status.RESOURCE_EXHAUSTED.withDescription("too big"), null);
    assertFalse(ex.isRetryable());
    assertEquals(ex.getStatus().getCode(), Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  public void testQueryExceptionNotRetryable() {
    DgraphQueryException ex =
        new DgraphQueryException(
            Status.UNKNOWN.withDescription("bad syntax"), null);
    assertFalse(ex.isRetryable());
  }

  @Test
  public void testAuthExceptionNotRetryable() {
    DgraphAuthException ex =
        new DgraphAuthException(
            Status.UNAUTHENTICATED.withDescription("denied"), null);
    assertFalse(ex.isRetryable());
  }

  @Test
  public void testAuthExceptionWithCause() {
    RuntimeException cause = new RuntimeException("parse error");
    DgraphAuthException ex = new DgraphAuthException("jwt failed", cause);
    assertFalse(ex.isRetryable());
    assertSame(ex.getCause(), cause);
    assertEquals(ex.getStatus().getCode(), Status.Code.INTERNAL);
  }

  @Test
  public void testInterruptedExceptionNotRetryable() {
    InterruptedException cause = new InterruptedException("interrupted");
    DgraphInterruptedException ex = new DgraphInterruptedException("op interrupted", cause);
    assertFalse(ex.isRetryable());
    assertSame(ex.getCause(), cause);
    assertEquals(ex.getStatus().getCode(), Status.Code.CANCELLED);
  }

  // --- getMessage() format ---

  @Test
  public void testGetMessageFormatIncludesStatusCode() {
    DgraphException ex = new DgraphException("something failed");
    assertEquals(ex.getMessage(), "INTERNAL: something failed");
  }

  @Test
  public void testGetMessageFormatForTxnFinished() {
    TxnFinishedException ex = new TxnFinishedException();
    assertEquals(
        ex.getMessage(), "INTERNAL: Transaction has already been committed or discarded");
  }

  @Test
  public void testGetMessageFormatForGrpcOrigin() {
    DgraphConnectionException ex =
        new DgraphConnectionException(
            Status.UNAVAILABLE.withDescription("connection lost"), null);
    assertEquals(ex.getMessage(), "UNAVAILABLE: connection lost");
  }
}
