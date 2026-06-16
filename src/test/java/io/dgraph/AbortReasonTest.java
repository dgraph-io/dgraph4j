/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import io.dgraph.TxnConflictException.AbortReason;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.testng.annotations.Test;

/**
 * Unit tests for surfacing the transaction-abort reason to the client. The Dgraph server encodes the
 * abort category as a {@code "<code>: <detail>"} prefix on the gRPC ABORTED status; these tests
 * verify that {@link TxnConflictException#getReason()} parses it, that the mapping from a raw gRPC
 * error goes through {@link Exceptions#translate}, and that behavior degrades gracefully against a
 * server that reports no reason.
 */
public class AbortReasonTest {

  private StatusRuntimeException aborted(String description) {
    return Status.Code.ABORTED.toStatus().withDescription(description).asRuntimeException();
  }

  private TxnConflictException conflictExceptionFor(String description) {
    DgraphException ex = Exceptions.translate(aborted(description));
    assertTrue(
        ex instanceof TxnConflictException,
        "ABORTED status should map to TxnConflictException, got " + ex.getClass());
    return (TxnConflictException) ex;
  }

  // --- Reason categorization (the three server-reported categories) ---

  @Test
  public void testConflictReason() {
    TxnConflictException ex =
        conflictExceptionFor("conflict: Transaction has been aborted. Please retry");
    assertEquals(ex.getReason(), AbortReason.CONFLICT);
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testPredicateMoveReason() {
    TxnConflictException ex =
        conflictExceptionFor(
            "predicate-move: Commits on predicate name are blocked due to predicate move");
    assertEquals(ex.getReason(), AbortReason.PREDICATE_MOVE);
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testStaleStartTsReason() {
    TxnConflictException ex =
        conflictExceptionFor(
            "stale-startts: Transaction has been aborted due to a leader change. Please retry");
    assertEquals(ex.getReason(), AbortReason.STALE_STARTTS);
    assertTrue(ex.isRetryable());
  }

  // --- Full message preserved alongside the parsed reason (backward compatibility) ---

  @Test
  public void testFullMessageIsPreserved() {
    String desc = "conflict: Transaction has been aborted. Please retry";
    TxnConflictException ex = conflictExceptionFor(desc);
    // getMessage() still exposes the complete human-readable description.
    assertTrue(ex.getMessage().contains(desc));
    assertEquals(ex.getStatus().getDescription(), desc);
  }

  // --- Graceful degradation against an older server (no reason prefix) ---

  @Test
  public void testLegacyMessageDegradesToUnknown() {
    // Pre-feature servers emit the bare static string with no category prefix.
    TxnConflictException ex = conflictExceptionFor("Transaction has been aborted. Please retry");
    assertEquals(ex.getReason(), AbortReason.UNKNOWN);
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testUnrecognizedPrefixDegradesToUnknown() {
    TxnConflictException ex = conflictExceptionFor("something-else: not a known category");
    assertEquals(ex.getReason(), AbortReason.UNKNOWN);
  }

  @Test
  public void testNullDescriptionIsUnknown() {
    DgraphException ex = Exceptions.translate(Status.ABORTED.asRuntimeException());
    assertTrue(ex instanceof TxnConflictException);
    assertEquals(((TxnConflictException) ex).getReason(), AbortReason.UNKNOWN);
  }

  // --- Parsing robustness ---

  @Test
  public void testReasonIsCaseInsensitiveAndTrimmed() {
    assertEquals(conflictExceptionFor("CONFLICT: x").getReason(), AbortReason.CONFLICT);
    assertEquals(conflictExceptionFor("  predicate-move : y").getReason(), AbortReason.PREDICATE_MOVE);
  }

  @Test
  public void testReasonWithoutDetailStillParses() {
    // A bare code with no ": detail" suffix should still categorize.
    assertEquals(conflictExceptionFor("conflict").getReason(), AbortReason.CONFLICT);
  }

  // --- The constructor used elsewhere in the client still works ---

  @Test
  public void testStringConstructorReason() {
    TxnConflictException ex = new TxnConflictException("conflict: manual");
    assertEquals(ex.getReason(), AbortReason.CONFLICT);
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testFailedPreconditionAlsoCarriesReason() {
    // FAILED_PRECONDITION also maps to TxnConflictException; reason parsing applies there too.
    DgraphException ex =
        Exceptions.translate(
            Status.Code.FAILED_PRECONDITION
                .toStatus()
                .withDescription("conflict: Transaction conflict")
                .asRuntimeException());
    assertTrue(ex instanceof TxnConflictException);
    assertEquals(((TxnConflictException) ex).getReason(), AbortReason.CONFLICT);
  }
}
