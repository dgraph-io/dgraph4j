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

  // Verbatim descriptions the server sends, from the abort-detail constants in
  // dgraph/cmd/zero/oracle.go. Kept literal rather than abbreviated so these fixtures stay a
  // faithful record of the wire format: several now contain colons of their own after the category
  // prefix, which is exactly the case the prefix parser has to get right.
  private static final String SERVER_CONFLICT =
      "conflict: Transaction has been aborted. Please retry. Another transaction committed to one "
          + "of the same keys. The conflicting key cannot be identified: it may be the data key "
          + "written directly, or an index or count key derived from it. On an @upsert predicate "
          + "the uid is excluded, so any two transactions writing the same value conflict";
  private static final String SERVER_STALE_STARTTS =
      "stale-startts: Transaction start timestamp is older than the oldest timestamp Zero can "
          + "still validate (Zero leader change, or its conflict map was trimmed at a snapshot). "
          + "Please retry";
  private static final String SERVER_MOVE_IN_FLIGHT =
      "predicate-move: Commits on predicate name are blocked due to predicate move";
  private static final String SERVER_MOVE_COMPLETED =
      "predicate-move: Mutation done in group: 1. Predicate name assigned to 2";

  // Causes the server deliberately leaves uncategorized: it says what happened, but no published
  // category fits, so it sends the detail with no prefix rather than implying a wrong remedy.
  private static final String SERVER_PRE_ABORTED =
      "Transaction has been aborted. Please retry. It was already aborted before this commit was "
          + "decided, which happens when a schema update or a drop-predicate cancels pending "
          + "transactions on a predicate it touched, or when the server ages out transactions idle "
          + "for longer than --limit \"txn-abort-after\"";
  private static final String SERVER_TABLET_NIL = "Tablet for name is nil";
  private static final String SERVER_MALFORMED_KEY = "Unable to find group id in 1name";
  private static final String SERVER_BAD_GROUP_ID =
      "unable to parse group id from xname: strconv.Atoi: parsing \"x\": invalid syntax";
  private static final String SERVER_CTX_CANCELLED = "context canceled";

  // --- Reason categorization (the three server-reported categories) ---

  @Test
  public void testConflictReason() {
    TxnConflictException ex = conflictExceptionFor(SERVER_CONFLICT);
    assertEquals(ex.getReason(), AbortReason.CONFLICT);
    assertTrue(ex.isRetryable());
  }

  @Test
  public void testPredicateMoveReason() {
    TxnConflictException ex = conflictExceptionFor(SERVER_MOVE_IN_FLIGHT);
    assertEquals(ex.getReason(), AbortReason.PREDICATE_MOVE);
    assertTrue(ex.isRetryable());
  }

  /**
   * The completed-move message contains a colon of its own ("group: 1"). Only the first colon
   * delimits the category, so this must still parse as PREDICATE_MOVE rather than being confused by
   * the second one.
   */
  @Test
  public void testPredicateMoveCompletedReasonWithEmbeddedColon() {
    TxnConflictException ex = conflictExceptionFor(SERVER_MOVE_COMPLETED);
    assertEquals(ex.getReason(), AbortReason.PREDICATE_MOVE);
  }

  @Test
  public void testStaleStartTsReason() {
    TxnConflictException ex = conflictExceptionFor(SERVER_STALE_STARTTS);
    assertEquals(ex.getReason(), AbortReason.STALE_STARTTS);
    assertTrue(ex.isRetryable());
  }

  /**
   * The conflict detail also contains its own colon ("cannot be identified: it may be"). Same
   * requirement as the completed-move case: the category comes from the first colon only.
   */
  @Test
  public void testConflictReasonWithEmbeddedColon() {
    assertTrue(
        SERVER_CONFLICT.indexOf(':') != SERVER_CONFLICT.lastIndexOf(':'),
        "fixture should contain more than one colon, otherwise this test proves nothing");
    assertEquals(conflictExceptionFor(SERVER_CONFLICT).getReason(), AbortReason.CONFLICT);
  }

  // --- Full message preserved alongside the parsed reason (backward compatibility) ---

  @Test
  public void testFullMessageIsPreserved() {
    TxnConflictException ex = conflictExceptionFor(SERVER_CONFLICT);
    // getMessage() still exposes the complete human-readable description, including the detail
    // explaining which kinds of key could have collided.
    assertTrue(ex.getMessage().contains(SERVER_CONFLICT));
    assertEquals(ex.getStatus().getDescription(), SERVER_CONFLICT);
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

  /**
   * A current server also sends aborts with no category, for causes no published category fits. It
   * still explains what happened in the description — only the machine-readable code is absent —
   * and the client must report UNKNOWN rather than misreading the detail as a category. These are
   * the real messages, not invented ones.
   */
  @Test
  public void testUncategorizedServerCausesDegradeToUnknown() {
    String[] uncategorized = {
      SERVER_PRE_ABORTED, SERVER_TABLET_NIL, SERVER_MALFORMED_KEY,
      SERVER_BAD_GROUP_ID, SERVER_CTX_CANCELLED,
    };
    for (String desc : uncategorized) {
      TxnConflictException ex = conflictExceptionFor(desc);
      assertEquals(
          ex.getReason(),
          AbortReason.UNKNOWN,
          "uncategorized server message should not parse as a category: " + desc);
      assertEquals(
          ex.getStatus().getDescription(), desc, "the explanation must survive intact: " + desc);
    }
  }

  /**
   * The out-of-band abort opens with the same sentence a pre-feature server sent for every abort,
   * and the malformed-group-id message carries a colon of its own. Neither may be mistaken for a
   * category — a false CONFLICT here would tell a caller to retry something that cannot succeed.
   */
  @Test
  public void testUncategorizedLookalikesAreNotMisparsed() {
    assertTrue(
        SERVER_PRE_ABORTED.startsWith("Transaction has been aborted. Please retry"),
        "fixture should start with the legacy sentence, otherwise this test proves nothing");
    assertEquals(conflictExceptionFor(SERVER_PRE_ABORTED).getReason(), AbortReason.UNKNOWN);

    assertTrue(SERVER_BAD_GROUP_ID.contains(":"), "fixture should contain a colon");
    assertEquals(conflictExceptionFor(SERVER_BAD_GROUP_ID).getReason(), AbortReason.UNKNOWN);
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
