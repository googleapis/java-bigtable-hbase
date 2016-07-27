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

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.base.Preconditions;

import io.grpc.stub.StreamObserver;

/**
 * Helper to read a queue of ResultQueueEntries and use the RowMergers to reconstruct
 * complete Row objects from the partial ReadRowsResponse objects.
 */
public class ResponseQueueReader implements StreamObserver<Row> {
  protected final BlockingQueue<ResultQueueEntry<Row>> resultQueue;
  protected AtomicBoolean completionMarkerFound = new AtomicBoolean(false);
  private final int readPartialRowTimeoutMillis;
  private boolean lastResponseProcessed = false;

  private static Timer first = BigtableSession.metrics.timer("ResponseQueueReader.first");
  private static Timer getNextMergedRow = BigtableSession.metrics.timer("ResponseQueueReader.getNextMergedRow");
  private static Timer betweenComplete = BigtableSession.metrics.timer("ResponseQueueReader.betweenComplete");
  private static Timer poll = BigtableSession.metrics.timer("ResponseQueueReader.poll");
  private boolean isFirst = true;
  private Context betweenCompleteContext;

  public ResponseQueueReader(int readPartialRowTimeoutMillis, int capacityCap) {
    this.resultQueue = new LinkedBlockingQueue<>(capacityCap);
    this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
  }

  /**
   * Get the next complete Row object from the response queue.
   * @return null if end-of-stream, otherwise a complete Row.
   * @throws IOException On errors.
   */
  public synchronized Row getNextMergedRow() throws IOException {
    Context context = getNextMergedRow.time();
    try {
      if (isFirst) {
        Timer.Context firstContet = first.time();
        try {
          return getNextRow();
        } finally {
          firstContet.close();
        }
      } else {
        return getNextRow();
      }
    } finally {
      context.stop();
    }
  }

  protected Row getNextRow() throws IOException {
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

  protected ResultQueueEntry<Row> getNext() throws IOException {
    final Context context = poll.time();
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
    context.stop();
    return queueEntry;
  }

  public int available() {
    return resultQueue.size();
  }

  @Override
  public void onNext(Row row) {
    try {
      if (betweenCompleteContext != null) {
        betweenCompleteContext.close();
      }
      resultQueue.put(ResultQueueEntry.fromResponse(row));
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    } finally {
      betweenCompleteContext = betweenComplete.time();
    }
  }

  @Override
  public void onError(Throwable t) {
    try {
      resultQueue.put(ResultQueueEntry.<Row> fromThrowable(t));
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }

  @Override
  public void onCompleted() {
    try {
      completionMarkerFound.set(true);
      resultQueue.put(ResultQueueEntry.<Row> completionMarker());
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }
}