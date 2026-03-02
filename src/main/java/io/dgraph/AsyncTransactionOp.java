/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import java.util.concurrent.CompletableFuture;

/**
 * An asynchronous operation to execute within a managed transaction. Used with {@link
 * DgraphAsyncClient#withRetry}.
 *
 * @param <T> the return type of the operation
 */
@FunctionalInterface
public interface AsyncTransactionOp<T> {
  /**
   * Executes the operation using the provided transaction.
   *
   * @param txn the transaction to use — do not discard it, the caller manages the lifecycle
   * @return a future that completes with the result of the operation
   */
  CompletableFuture<T> execute(AsyncTransaction txn);
}
