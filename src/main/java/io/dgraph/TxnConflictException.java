/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a transaction conflicts with another concurrent transaction. This is a retryable error
 * — the operation can be retried with a new transaction.
 */
public class TxnConflictException extends TxnException {
  private static final long serialVersionUID = 1L;

  /**
   * The category of a transaction abort, as reported by the Dgraph server.
   *
   * <ul>
   *   <li>{@link #CONFLICT} — a write-write conflict with another concurrent transaction; retrying
   *       with a fresh transaction is the expected response. The server cannot say <em>which</em>
   *       key collided: conflict keys are one-way fingerprints by the time they are compared, so
   *       the culprit may be the data key written directly, or an index or count key derived from
   *       it. On an {@code @upsert} predicate the uid is excluded from the comparison, so any two
   *       transactions writing the same value conflict.
   *   <li>{@link #PREDICATE_MOVE} — a predicate is being moved between groups and commits on it are
   *       temporarily blocked, or it finished moving while the transaction was open; back off and
   *       retry once the move completes.
   *   <li>{@link #STALE_STARTTS} — the transaction's start timestamp is older than the oldest
   *       timestamp the server can still validate against. That happens on a Zero leader change,
   *       and also when Zero trims its conflict map at a snapshot, which is not a leader change at
   *       all. Retry with a fresh transaction.
   *   <li>{@link #UNKNOWN} — no category was reported. This covers aborts from older servers that
   *       do not categorize at all, and aborts a current server declines to categorize because no
   *       published category fits — for example a transaction already aborted out of band by a
   *       schema change or the idle-transaction reaper, a cancelled context, or a predicate no
   *       group currently serves. The description still explains what happened; only the machine
   *       readable category is absent, so callers degrade gracefully.
   * </ul>
   *
   * <p>Categories are matched on the description prefix, and an unrecognized prefix degrades to
   * {@link #UNKNOWN}. A newer server may therefore introduce categories this enum does not name
   * without breaking this client.
   */
  public enum AbortReason {
    CONFLICT,
    PREDICATE_MOVE,
    STALE_STARTTS,
    UNKNOWN
  }

  public TxnConflictException(String msg) {
    super(Status.ABORTED.withDescription(msg), null);
  }

  TxnConflictException(Status status, Metadata trailers) {
    super(status, trailers);
  }

  /**
   * Returns the category of this abort. The server encodes the reason as a {@code "<code>: <detail>"}
   * prefix on the gRPC status description; this method parses that prefix. Against a server that does
   * not report a reason (older versions), this returns {@link AbortReason#UNKNOWN}. The full
   * human-readable description remains available via {@link #getMessage()}.
   */
  public AbortReason getReason() {
    String desc = getStatus().getDescription();
    if (desc == null) {
      return AbortReason.UNKNOWN;
    }
    int colon = desc.indexOf(':');
    String code = (colon >= 0 ? desc.substring(0, colon) : desc).trim().toLowerCase();
    switch (code) {
      case "conflict":
        return AbortReason.CONFLICT;
      case "predicate-move":
        return AbortReason.PREDICATE_MOVE;
      case "stale-startts":
        return AbortReason.STALE_STARTTS;
      default:
        return AbortReason.UNKNOWN;
    }
  }

  @Override
  public boolean isRetryable() {
    return true;
  }
}
