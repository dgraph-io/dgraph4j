/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

/**
 * A synchronous operation to execute within a managed transaction. Used with {@link
 * DgraphClient#withRetry}.
 *
 * @param <T> the return type of the operation
 */
@FunctionalInterface
public interface TransactionOp<T> {
  /**
   * Executes the operation using the provided transaction.
   *
   * @param txn the transaction to use — do not discard it, the caller manages the lifecycle
   * @return the result of the operation
   * @throws Exception if the operation fails
   */
  T execute(Transaction txn) throws Exception;
}
