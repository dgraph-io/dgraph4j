/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Abstract base for transaction-related exceptions. These are client-side errors indicating invalid
 * transaction state.
 */
public abstract class TxnException extends DgraphException {
  private static final long serialVersionUID = 1L;

  TxnException(String message) {
    super(message);
  }

  TxnException(Status status, Metadata trailers) {
    super(status, trailers);
  }

  /**
   * Returns the plain description without the gRPC status code prefix, preserving backward
   * compatibility with code that previously caught these as RuntimeException.
   */
  @Override
  public String getMessage() {
    String desc = getStatus().getDescription();
    return desc != null ? desc : super.getMessage();
  }
}
