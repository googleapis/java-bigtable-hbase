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

import com.google.common.base.Preconditions;

import io.grpc.stub.StreamObserver;

/**
 * Helper to read a queue of ResultQueueEntries and use the RowMergers to reconstruct
 * complete Row objects from the partial ReadRowsResponse objects.
 */
public class ResponseQueueReader<ResponseT> implements StreamObserver<ResponseT> {
  protected final BlockingQueue<ResultQueueEntry<ResponseT>> resultQueue;
  protected AtomicBoolean completionMarkerFound = new AtomicBoolean(false);
  private final int readPartialRowTimeoutMillis;
  private boolean lastResponseProcessed = false;

  public ResponseQueueReader(int readPartialRowTimeoutMillis, int capacityCap) {
    this.resultQueue = new LinkedBlockingQueue<>(capacityCap);
    this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
  }

  /**
   * Get the next complete Row object from the response queue.
   * @return null if end-of-stream, otherwise a complete Row.
   * @throws IOException On errors.
   */
  public synchronized ResponseT getNextMergedRow() throws IOException {
    if (!lastResponseProcessed) {
      ResultQueueEntry<ResponseT> queueEntry = getNext();

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

  protected ResultQueueEntry<ResponseT> getNext() throws IOException {
    ResultQueueEntry<ResponseT> queueEntry;
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

  @Override
  public void onNext(ResponseT row) {
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
      resultQueue.put(ResultQueueEntry.<ResponseT> fromThrowable(t));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }

  @Override
  public void onCompleted() {
    try {
      completionMarkerFound.set(true);
      resultQueue.put(ResultQueueEntry.<ResponseT> completionMarker());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }
}