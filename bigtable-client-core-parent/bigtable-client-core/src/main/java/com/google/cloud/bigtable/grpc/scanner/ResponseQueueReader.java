/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.base.Preconditions;

import io.grpc.stub.StreamObserver;

/**
 * Helper to read a queue of ResultQueueEntries and use the RowMergers to reconstruct
 * complete Row objects from the partial ReadRowsResponse objects.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ResponseQueueReader implements StreamObserver<Row> {

  private static Timer firstResponseTimer;

  private synchronized static Timer getFirstResponseTimer() {
    if (firstResponseTimer == null) {
      firstResponseTimer = BigtableClientMetrics
        .timer(MetricLevel.Info, "grpc.method.ReadRows.firstResponse.latency");
    }
    return firstResponseTimer;
  }

  protected final BlockingQueue<ResultQueueEntry<Row>> resultQueue;
  protected AtomicBoolean completionMarkerFound = new AtomicBoolean(false);
  private final int readPartialRowTimeoutMillis;
  private boolean lastResponseProcessed = false;
  private Long startTime;

  /**
   * <p>Constructor for ResponseQueueReader.</p>
   *
   * @param readPartialRowTimeoutMillis a int.
   * @param capacityCap a int.
   */
  public ResponseQueueReader(int readPartialRowTimeoutMillis, int capacityCap) {
    this.resultQueue = new LinkedBlockingQueue<>(capacityCap);
    this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
    if (BigtableClientMetrics.isEnabled(MetricLevel.Info)) {
      startTime = System.nanoTime();
    }
  }

  /**
   * Get the next complete Row object from the response queue.
   *
   * @return null if end-of-stream, otherwise a complete Row.
   * @throws java.io.IOException On errors.
   */
  public synchronized Row getNextMergedRow() throws IOException {
    if (lastResponseProcessed) {
      return null;
    }

    ResultQueueEntry<Row> queueEntry = getNext();

    switch (queueEntry.getType()) {
    case CompletionMarker:
      lastResponseProcessed = true;
      break;
    case Data:
      if (startTime != null) {
        getFirstResponseTimer().update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        startTime = null;
      }
      return queueEntry.getResponseOrThrow();
    case Exception:
      return queueEntry.getResponseOrThrow();
    default:
      throw new IllegalStateException("Cannot process type: " + queueEntry.getType());
    }

    Preconditions.checkState(lastResponseProcessed,
      "Should only exit merge loop with by returning a complete Row or hitting end of stream.");
    return null;
  }

  /**
   * <p>getNext.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.scanner.ResultQueueEntry} object.
   * @throws java.io.IOException if any.
   */
  protected ResultQueueEntry<Row> getNext() throws IOException {
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
   * <p>available.</p>
   *
   * @return a int.
   */
  public int available() {
    return resultQueue.size();
  }

  /** {@inheritDoc} */
  @Override
  public void onNext(Row row) {
    try {
      resultQueue.put(ResultQueueEntry.fromResponse(row));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onError(Throwable t) {
    try {
      resultQueue.put(ResultQueueEntry.<Row> fromThrowable(t));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }

  /** {@inheritDoc} */
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
