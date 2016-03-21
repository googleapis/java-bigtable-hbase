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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.common.base.Preconditions;

/**
 * Helper to read a queue of ResultQueueEntries and use the RowMergers to reconstruct
 * complete Row objects from the partial ReadRowsResponse objects.
 */
public class ResponseQueueReader {
  private final BlockingQueue<ResultQueueEntry<Row>> resultQueue;
  private final int thresholdToRequestMore;
  private final int batchRequestSize;
  private final int readPartialRowTimeoutMillis;
  private final ClientCall<?, ?> call;

  private boolean lastResponseProcessed = false;
  private AtomicBoolean completionMarkerFound = new AtomicBoolean(false);
  private AtomicInteger outstandingRequestCount;
  private RowMerger rowMerger;

  public ResponseQueueReader(int readPartialRowTimeoutMillis, int capacityCap,
      int outstandingRequestCount, int batchRequestSize, ClientCall<?, ?> call) {
    this.resultQueue = new LinkedBlockingQueue<>();
    this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
    this.outstandingRequestCount = new AtomicInteger(outstandingRequestCount);
    this.batchRequestSize = batchRequestSize;
    this.call = call;
    this.thresholdToRequestMore = capacityCap - batchRequestSize;
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
    fetchMore();
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
    fetchMore();

    return queueEntry;
  }

  private void fetchMore() {
    // If there are currently less than or equal to the batch request size, then ask gRPC to
    // request more results in a batch. Batch requests are more efficient that reading one at
    // a time.
    if (!completionMarkerFound.get() && moreCanBeRequested()) {
      call.request(batchRequestSize);
      outstandingRequestCount.addAndGet(batchRequestSize);
    }
  }

  /**
   * Calculates whether or not a new batch should be requested.
   * @return true if a new batch should be requested.
   */
  private boolean moreCanBeRequested() {
    return outstandingRequestCount.get() + resultQueue.size() <= thresholdToRequestMore;
  }

  public int available() {
    return resultQueue.size();
  }

  //////////////////   All of the methods below are assumed to be called serially.
  public void addResult(ReadRowsResponse response) throws InterruptedException {
    outstandingRequestCount.decrementAndGet();
    if (rowMerger == null) {
      rowMerger = new RowMerger();
    }

    rowMerger.addPartialRow(response);

    if (rowMerger.isRowCommitted()) {
      Row row = rowMerger.buildRow();
      rowMerger = null;
      if (row != null) {
        resultQueue.put(ResultQueueEntry.newResult(row));
      }
    }
  }

  public void setError(Throwable error) throws InterruptedException {
    resultQueue.put(ResultQueueEntry.<Row> newThrowable(error));
  }

  public void complete() throws InterruptedException {
    completionMarkerFound.set(true);
    if (rowMerger != null) {
      setError(new IllegalStateException("End of stream marker encountered while merging a row."));
    } else {
      resultQueue.put(ResultQueueEntry.<Row> newCompletionMarker());
    }
  }
}