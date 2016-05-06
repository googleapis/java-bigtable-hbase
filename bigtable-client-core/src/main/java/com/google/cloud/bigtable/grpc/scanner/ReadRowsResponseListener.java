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

import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * A {@link io.grpc.ClientCall.Listener} that translates between a {@link ReadRowsResponse} and a
 * {@link Row} {@link StreamObserver}.
 */
public class ReadRowsResponseListener extends ClientCall.Listener<ReadRowsResponse> {
  private RowMerger builder;
  private AtomicInteger outstandingRequestCount;

  public ReadRowsResponseListener(StreamObserver<Row> observer, AtomicInteger outstandingRequestCount) {
    this.outstandingRequestCount = outstandingRequestCount;
    this.builder = new RowMerger(observer);
  }

  @Override
  public void onMessage(ReadRowsResponse response) {
    builder.onNext(response);
    outstandingRequestCount.decrementAndGet();
  }

  @Override
  public void onClose(Status status, Metadata trailers) {
    if (status.isOk()) {
      builder.onCompleted();
    } else {
      builder.onError(status.asRuntimeException());
    }
  }
}
