/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.Metadata;
import io.grpc.Status;

/**
 * Thrown when a Dgraph Alpha is overloaded with pending proposals or too many concurrent requests.
 * This is a transient error that can be resolved by retrying after a backoff period.
 */
public class AlphaOverloadedException extends AlphaException {
  private static final long serialVersionUID = 1L;

  AlphaOverloadedException(Status status, Metadata trailers) {
    super(status, trailers);
  }
}
