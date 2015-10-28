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

import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.common.base.Preconditions;

/**
 * Helper to read a queue of ResultQueueEntries and use the RowMergers to reconstruct
 * complete Row objects from the partial ReadRowsResponse objects.
 */
class ResponseQueueReader {
  private static final long OFFER_WAIT_SECONDS = 5;
  private final BlockingQueue<ResultQueueEntry<ReadRowsResponse>> resultQueue;
  private final int readPartialRowTimeoutMillis;
  private boolean lastResponseProcessed = false;

  public ResponseQueueReader(int capacity, int readPartialRowTimeoutMillis) {
    this.resultQueue = new LinkedBlockingQueue<>(capacity);
    this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
  }

  /**
   * Get the next complete Row object from the response queue.
   * @return Optional.absent() if end-of-stream, otherwise a complete Row.
   * @throws IOException On errors.
   */
  public synchronized Row getNextMergedRow() throws IOException {
    RowMerger builder = null;

    while (!lastResponseProcessed) {
      ResultQueueEntry<ReadRowsResponse> queueEntry = getNext();

      if (queueEntry.isCompletionMarker()) {
        lastResponseProcessed = true;
        break;
      }

      ReadRowsResponse partialRow = queueEntry.getResponseOrThrow();
      if (builder == null) {
        builder = new RowMerger();
      }

      builder.addPartialRow(partialRow);

      if (builder.isRowCommitted()) {
        return builder.buildRow();
      }
    }

    Preconditions.checkState(
        builder == null,
        "End of stream marker encountered while merging a row.");
    Preconditions.checkState(
        lastResponseProcessed,
        "Should only exit merge loop with by returning a complete Row or hitting end of stream.");
    return null;
  }

  private ResultQueueEntry<ReadRowsResponse> getNext() throws IOException {
    ResultQueueEntry<ReadRowsResponse> queueEntry;
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
  
  public int available() {
    return resultQueue.size();
  }

  public void add(ResultQueueEntry<ReadRowsResponse> entry) throws InterruptedException {
    resultQueue.offer(entry, OFFER_WAIT_SECONDS, TimeUnit.SECONDS);
  }
}