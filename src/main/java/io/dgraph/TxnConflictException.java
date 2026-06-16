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
   *       with a fresh transaction is the expected response.
   *   <li>{@link #PREDICATE_MOVE} — a predicate is being moved between groups and commits on it are
   *       temporarily blocked; back off and retry once the move completes.
   *   <li>{@link #STALE_STARTTS} — the transaction's start timestamp predates the current Zero
   *       leader (a leader change); retry with a fresh transaction.
   *   <li>{@link #UNKNOWN} — no reason was reported. Returned for aborts from older servers that do
   *       not yet categorize the reason, so callers degrade gracefully.
   * </ul>
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
