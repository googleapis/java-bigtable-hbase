/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc.scanner;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Adapts a {@link io.grpc.stub.StreamObserver} to a {@link io.grpc.ClientCall.Listener}.
 * {@link io.grpc.ClientCall#request(int)} will be called onNext.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class StreamObserverAdapter<T> extends ClientCall.Listener<T> {

  private final ClientCall<?, T> call;
  private final StreamObserver<T> observer;

  /**
   * <p>Constructor for StreamObserverAdapter.</p>
   *
   * @param call a {@link io.grpc.ClientCall} object.
   * @param observer a {@link io.grpc.stub.StreamObserver} object.
   */
  public StreamObserverAdapter(ClientCall<?, T> call, StreamObserver<T> observer) {
    this.call = call;
    this.observer = observer;
  }

  /** {@inheritDoc} */
  @Override
  public void onMessage(T message) {
    call.request(1);
    observer.onNext(message);
  }

  /** {@inheritDoc} */
  @Override
  public void onClose(Status status, Metadata trailers) {
    if (status.isOk()) {
      observer.onCompleted();
    } else {
      observer.onError(status.asRuntimeException());
    }
  }
}
