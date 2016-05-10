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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.ClientCall;

/**
 * An extension of {@link ResponseQueueReader} that will request more rows in batch.
 */
public class RequestCountManagingResponseQueueReader<ResponseT>
    extends ResponseQueueReader<ResponseT> {
  private final int capacityCap;
  private final int batchRequestSize;
  private AtomicInteger outstandingRequestCount;
  private final ClientCall<?, ?> call;

  public RequestCountManagingResponseQueueReader(
      int readPartialRowTimeoutMillis,
      int capacityCap,
      AtomicInteger outstandingRequestCount,
      int batchRequestSize,
      ClientCall<?, ?> call) {
    super(readPartialRowTimeoutMillis, capacityCap);
    this.capacityCap = capacityCap;
    this.outstandingRequestCount = outstandingRequestCount;
    this.batchRequestSize = batchRequestSize;
    this.call = call;
  }

  @Override
  protected ResultQueueEntry<ResponseT> getNext() throws IOException {
    // If there are currently less than or equal to the batch request size, then ask gRPC to
    // request more results in a batch. Batch requests are more efficient that reading one at
    // a time.
    if (!completionMarkerFound.get() && moreCanBeRequested()) {
      call.request(batchRequestSize);
      outstandingRequestCount.addAndGet(batchRequestSize);
    }
    return super.getNext();
  }

  /**
   * Calculates whether or not a new batch should be requested.
   * @return true if a new batch should be requested.
   */
  private boolean moreCanBeRequested() {
    return outstandingRequestCount.get() + resultQueue.size() <= capacityCap - batchRequestSize;
  }
}