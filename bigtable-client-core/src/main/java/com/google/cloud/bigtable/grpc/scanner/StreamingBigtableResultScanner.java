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

import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ResultScanner} implementation against the v1 bigtable API.
 */
public class StreamingBigtableResultScanner extends AbstractBigtableResultScanner {

  /**
   * Helper to read a queue of ResultQueueEntries and use the RowMergers to reconstruct
   * complete Row objects from the partial ReadRowsResponse objects.
   */
  protected static class ResponseQueueReader {
    private final BlockingQueue<ResultQueueEntry<ReadRowsResponse>> resultQueue;
    private final int readPartialRowTimeoutMillis;
    private boolean lastResponseProcessed = false;

    public ResponseQueueReader(
        BlockingQueue<ResultQueueEntry<ReadRowsResponse>> resultQueue,
        int readPartialRowTimeoutMillis) {
      this.resultQueue = resultQueue;
      this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
    }

    /**
     * Get the next complete Row object from the response queue.
     * @return Optional.absent() if end-of-stream, otherwise a complete Row.
     * @throws IOException On errors.
     */
    public Optional<Row> getNextMergedRow() throws IOException {
      ResultQueueEntry<ReadRowsResponse> queueEntry;
      RowMerger builder = null;

      while (!lastResponseProcessed) {
        try {
          queueEntry = resultQueue.poll(readPartialRowTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for next result", e);
        }
        if (queueEntry == null) {
          throw new ScanTimeoutException("Timeout while merging responses.");
        }

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
          return Optional.of(builder.buildRow());
        }
      }

      Preconditions.checkState(
          builder == null,
          "End of stream marker encountered while merging a row.");
      Preconditions.checkState(
          lastResponseProcessed,
          "Should only exit merge loop with by returning a complete Row or hitting end of stream.");
      return Optional.absent();
    }
  }

  private final CancellationToken cancellationToken;
  private final BlockingQueue<ResultQueueEntry<ReadRowsResponse>> resultQueue;
  private final ResponseQueueReader responseQueueReader;

  public StreamingBigtableResultScanner(
      int capacity,
      int readPartialRowTimeoutMillis,
      CancellationToken cancellationToken) {
    Preconditions.checkArgument(cancellationToken != null, "cancellationToken cannot be null");
    Preconditions.checkArgument(capacity > 0, "capacity must be a positive integer");
    this.cancellationToken = cancellationToken;
    this.resultQueue = new LinkedBlockingQueue<>(capacity);
    this.responseQueueReader = new ResponseQueueReader(
        resultQueue, readPartialRowTimeoutMillis);
  }

  private void add(ResultQueueEntry<ReadRowsResponse> entry) {
    try {
      resultQueue.put(entry);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }

  public void addResult(ReadRowsResponse response) {
    add(ResultQueueEntry.newResult(response));
  }

  public void setError(Throwable error) {
    add(ResultQueueEntry.<ReadRowsResponse>newThrowable(error));
  }

  public void complete() {
    add(ResultQueueEntry.<ReadRowsResponse>newCompletionMarker());
  }

  @Override
  public Row next() throws IOException {
    Optional<Row> next = responseQueueReader.getNextMergedRow();
    if (next.isPresent()) {
      return next.get();
    } else {
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    cancellationToken.cancel();
  }
}
