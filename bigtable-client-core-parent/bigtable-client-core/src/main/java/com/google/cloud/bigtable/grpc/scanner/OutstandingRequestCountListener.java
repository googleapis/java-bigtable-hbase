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

import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * A {@link io.grpc.ClientCall.Listener} that wraps a {@link io.grpc.stub.StreamObserver} and decrements
 * outstandingRequestCount when a message is received.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class OutstandingRequestCountListener<ResponseT> extends ClientCall.Listener<ResponseT> {
  private StreamObserver<ResponseT> observer;
  private AtomicInteger outstandingRequestCount;

  /**
   * <p>Constructor for OutstandingRequestCountListener.</p>
   *
   * @param observer a {@link io.grpc.stub.StreamObserver} object.
   * @param outstandingRequestCount a {@link java.util.concurrent.atomic.AtomicInteger} object.
   */
  public OutstandingRequestCountListener(StreamObserver<ResponseT> observer,
      AtomicInteger outstandingRequestCount) {
    this.outstandingRequestCount = outstandingRequestCount;
    this.observer = observer;
  }

  /** {@inheritDoc} */
  @Override
  public void onMessage(ResponseT response) {
    outstandingRequestCount.decrementAndGet();
    observer.onNext(response);
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
