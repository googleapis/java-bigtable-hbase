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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.common.base.Preconditions;

import io.grpc.ClientCall;
import io.grpc.stub.StreamObserver;

/**
 * Helper to read a queue of ResultQueueEntries and use the RowMergers to reconstruct
 * complete Row objects from the partial ReadRowsResponse objects.
 */
public class ResponseQueueReader implements StreamObserver<Row> {
  private final BlockingQueue<ResultQueueEntry<Row>> resultQueue;
  private final int readPartialRowTimeoutMillis;
  private boolean lastResponseProcessed = false;
  private AtomicBoolean completionMarkerFound = new AtomicBoolean(false);
  private final int capacityCap;
  private final int batchRequestSize;
  private AtomicInteger outstandingRequestCount;
  private final ClientCall<?, ReadRowsResponse> call;

  public ResponseQueueReader(int readPartialRowTimeoutMillis, int capacityCap,
      AtomicInteger outstandingRequestCount, int batchRequestSize,
      ClientCall<?, ReadRowsResponse> call) {
    this.resultQueue = new LinkedBlockingQueue<>();
    this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
    this.capacityCap = capacityCap;
    this.outstandingRequestCount = outstandingRequestCount;
    this.batchRequestSize = batchRequestSize;
    this.call = call;
  }

  /**
   * Get the next complete Row object from the response queue.
   * @return null if end-of-stream, otherwise a complete Row.
   * @throws IOException On errors.
   */
  public synchronized Row getNextMergedRow() throws IOException {
    if (!lastResponseProcessed) {
      ResultQueueEntry<Row> queueEntry = getNext();

      if (queueEntry.isCompletionMarker()) {
        lastResponseProcessed = true;
      } else {
        return queueEntry.getResponseOrThrow();
      }
    }

    Preconditions.checkState(lastResponseProcessed,
      "Should only exit merge loop with by returning a complete Row or hitting end of stream.");
    return null;
  }

  private ResultQueueEntry<Row> getNext() throws IOException {
    // If there are currently less than or equal to the batch request size, then ask gRPC to
    // request more results in a batch. Batch requests are more efficient that reading one at
    // a time.
    if (!completionMarkerFound.get() && moreCanBeRequested()) {
      call.request(batchRequestSize);
      outstandingRequestCount.addAndGet(batchRequestSize);
    }
    ResultQueueEntry<Row> queueEntry;
    try {
      queueEntry = resultQueue.poll(readPartialRowTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for next result", e);
    }
    if (queueEntry == null) {
      throw new ScanTimeoutException("Timeout while merging responses.");
    }

    return queueEntry;
  }

  /**
   * Calculates whether or not a new batch should be requested.
   * @return true if a new batch should be requested.
   */
  private boolean moreCanBeRequested() {
    return outstandingRequestCount.get() + resultQueue.size() <= capacityCap - batchRequestSize;
  }

  public int available() {
    return resultQueue.size();
  }

  @Override
  public void onNext(Row row) {
    try {
      resultQueue.put(ResultQueueEntry.fromResponse(row));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }

  @Override
  public void onError(Throwable t) {
    try {
      resultQueue.put(ResultQueueEntry.<Row> fromThrowable(t));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }

  @Override
  public void onCompleted() {
    try {
      completionMarkerFound.set(true);
      resultQueue.put(ResultQueueEntry.<Row> completionMarker());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }
}