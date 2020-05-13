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

import com.google.api.core.InternalApi;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.base.Preconditions;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a queue of {@link ResultQueueEntry}s of {@link FlatRow}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ResponseQueueReader
    implements StreamObserver<FlatRow>, ClientResponseObserver<ReadRowsRequest, FlatRow> {

  private static Timer firstResponseTimer;
  private final long waitForRowsTimeoutMillis;

  private static synchronized Timer getFirstResponseTimer() {
    if (firstResponseTimer == null) {
      firstResponseTimer =
          BigtableClientMetrics.timer(
              MetricLevel.Info, "grpc.method.ReadRows.firstResponse.latency");
    }
    return firstResponseTimer;
  }

  private final BlockingQueue<ResultQueueEntry<FlatRow>> resultQueue = new LinkedBlockingQueue<>();
  private final AtomicBoolean completionMarkerFound = new AtomicBoolean(false);
  private boolean lastResponseProcessed = false;
  private Long startTime;
  private ClientCallStreamObserver<ReadRowsRequest> requestStream;
  private AtomicInteger markerCounter = new AtomicInteger();

  /** Constructor for ResponseQueueReader. */
  public ResponseQueueReader(long waitForRowsTimeoutMillis) {
    if (BigtableClientMetrics.isEnabled(MetricLevel.Info)) {
      startTime = System.nanoTime();
    }

    this.waitForRowsTimeoutMillis = waitForRowsTimeoutMillis;
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<ReadRowsRequest> requestStream) {
    requestStream.disableAutoInboundFlowControl();
    this.requestStream = requestStream;
  }

  public void close() {
    requestStream.cancel("Closing ResponseQueueReader", null);
  }

  /**
   * Get the next complete {@link FlatRow} object from the response queue.
   *
   * @return null if end-of-stream, otherwise a complete {@link FlatRow}.
   * @throws java.io.IOException On errors.
   */
  public synchronized FlatRow getNextMergedRow() throws IOException {
    ResultQueueEntry<FlatRow> queueEntry = getNext();

    switch (queueEntry.getType()) {
      case CompletionMarker:
        lastResponseProcessed = true;
        markerCounter.decrementAndGet();
        break;
      case Data:
        if (startTime != null) {
          getFirstResponseTimer().update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
          startTime = null;
        }
        return queueEntry.getResponseOrThrow();
      case Exception:
        markerCounter.decrementAndGet();
        return queueEntry.getResponseOrThrow();
      case RequestResultMarker:
        markerCounter.decrementAndGet();
        if (!completionMarkerFound.get()) {
          requestStream.request(1);
        }
        return getNextMergedRow();
      default:
        throw new IllegalStateException("Cannot process type: " + queueEntry.getType());
    }

    Preconditions.checkState(
        lastResponseProcessed,
        "Should only exit merge loop with by returning a complete FlatRow or hitting end of stream.");
    return null;
  }

  /**
   * getNext.
   *
   * @return a {@link com.google.cloud.bigtable.grpc.scanner.ResultQueueEntry} object.
   * @throws java.io.IOException if any.
   */
  protected ResultQueueEntry<FlatRow> getNext() throws IOException {
    ResultQueueEntry<FlatRow> queueEntry;
    try {
      queueEntry = resultQueue.poll(waitForRowsTimeoutMillis, TimeUnit.MILLISECONDS);
      // Should never happen
      if (queueEntry == null) {
        throw new IllegalStateException("Timed out awaiting next sync rows");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for next result", e);
    }

    return queueEntry;
  }

  /**
   * available.
   *
   * @return a int.
   */
  public int available() {
    return resultQueue.size() - markerCounter.get();
  }

  /** {@inheritDoc} */
  @Override
  public void onNext(FlatRow row) {
    addEntry("adding a data ResultQueueEntry", ResultQueueEntry.fromResponse(row));
  }

  /** {@inheritDoc} */
  @Override
  public void onError(Throwable t) {
    addEntry("adding an error ResultQueueEntry", ResultQueueEntry.<FlatRow>fromThrowable(t));
    markerCounter.incrementAndGet();
  }

  /** {@inheritDoc} */
  @Override
  public void onCompleted() {
    completionMarkerFound.set(true);
    addEntry("setting completion", ResultQueueEntry.<FlatRow>completionMarker());
    markerCounter.incrementAndGet();
  }

  /**
   * This marker notifies {@link #getNextMergedRow()} to request more results and allows for flow
   * control based on the state of the {@link #resultQueue}. If rows are removed from the queue
   * quickly, {@link #getNextMergedRow()} will request more results. If rows are not read fast
   * enough, then gRPC will stop fetching rows, and will wait until more rows are requested. This
   * marker tells {@link #getNextMergedRow()} to read more rows.
   */
  public void addRequestResultMarker() {
    addEntry("setting request result marker", ResultQueueEntry.<FlatRow>requestResultMarker());
    markerCounter.incrementAndGet();
  }

  private void addEntry(String message, ResultQueueEntry<FlatRow> entry) {
    try {
      resultQueue.put(entry);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while " + message, e);
    }
  }
}
