/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

public class DgraphException extends RuntimeException {
  DgraphException(String message) {
    super(message);
  }
}
