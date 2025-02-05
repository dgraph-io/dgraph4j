/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;

public class StreamObserverBridge<T> implements StreamObserver<T> {

  private final CompletableFuture<T> delegate = new CompletableFuture<>();

  @Override
  public void onNext(T value) {
    delegate.complete(value);
  }

  @Override
  public void onError(Throwable t) {
    delegate.completeExceptionally(t);
  }

  @Override
  public void onCompleted() {
    // TODO is this right? what if it's a void operation and there is no onNext()?
    // do nothing
  }

  public CompletableFuture<T> getDelegate() {
    return delegate;
  }
}
