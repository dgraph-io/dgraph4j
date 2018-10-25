/*
 * Copyright (C) 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
