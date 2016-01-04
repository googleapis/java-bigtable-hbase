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
package com.google.cloud.bigtable.grpc.async;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.cloud.bigtable.config.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * This class ensures that operations meet heap size and max RPC counts.  A wait will occur
 * if RPCs are requested after heap and RPC count thresholds are exceeded.
 */
public class HeapSizeManager {
  protected static final Logger LOG = new Logger(HeapSizeManager.class);

  // Flush is not properly synchronized with respect to waiting. It will never exit
  // improperly, but it might wait more than it has to. Setting this to a low value ensures
  // that improper waiting is minimal.
  private static final long FINISH_WAIT_MILLIS = 100;
  private static final long REGISTER_WAIT_MILLIS = 10;

  // In flush, wait up to this number of milliseconds without any operations completing.  If
  // this amount of time goes by without any updates, flush will log a warning.  Flush()
  // will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING_MS = 60000;

  private final long maxHeapSize;
  private final int maxInFlightRpcs;

  private AtomicLong operationSequenceGenerator = new AtomicLong();

  private final Map<Long, Long> pendingOperationsWithSize = new HashMap<>();
  private final AtomicInteger pendingOperationCount = new AtomicInteger();

  private final LinkedBlockingQueue<Long> completedOperationIds = new LinkedBlockingQueue<>();
  private long currentWriteBufferSize = 0;
  private long lastOperationChange = System.currentTimeMillis();

  private AtomicBoolean isFlushing = new AtomicBoolean(false);
  private AtomicBoolean isFull = new AtomicBoolean(false);

  public HeapSizeManager(long maxHeapSize, int maxInflightRpcs) {
    this.maxHeapSize = maxHeapSize;
    this.maxInFlightRpcs = maxInflightRpcs;
  }

  public long getMaxHeapSize() {
    return maxHeapSize;
  }

  public void flush() throws InterruptedException {
    isFlushing.set(true);
    boolean performedWarning = false;
    while (true) {
      synchronized (pendingOperationsWithSize) {
        cleanupFinishedOperations();
        if (pendingOperationsWithSize.isEmpty()) {
          break;
        }
      }
      if (!performedWarning && requiresWarning()) {
        warn();
        performedWarning = true;
      }
      synchronized (this) {
        wait(FINISH_WAIT_MILLIS);
      }
    }
    if (performedWarning) {
      LOG.info("flush() completed. See the log for previous warnings.");
    }
    isFlushing.set(false);
  }

  /**
   * @return {@code true} if the last successful completion is awhile ago (longer than {@code
   *         INTERVAL_NO_SUCCESS_WARNING_MS} ms ago.
   */
  private boolean requiresWarning() {
    return lastOperationChange + INTERVAL_NO_SUCCESS_WARNING_MS < System.currentTimeMillis();
  }

  private void warn() {
    long lastUpdated = (System.currentTimeMillis() - lastOperationChange) / 1000;
    LOG.warn(
        "No operations completed within the last %d seconds. "
        + "There are still %d operations in progress. ",
        lastUpdated, pendingOperationsWithSize.size());
  }

  private void cleanupFinishedOperations(Long... extraOperationsComplete) {
    List<Long> toClean = new ArrayList<>();
    completedOperationIds.drainTo(toClean);
    toClean.addAll(Arrays.asList(extraOperationsComplete));
    if (!toClean.isEmpty()) {
      for (Long operationSequenceId : toClean) {
        markOperationComplete(operationSequenceId);
      }
      lastOperationChange = System.currentTimeMillis();
      isFull.set(checkIsFull());
    }
  }

  private void markOperationComplete(Long operationSequenceId) {
    Long heapSize = pendingOperationsWithSize.remove(operationSequenceId);
    if (heapSize != null) {
      currentWriteBufferSize -= heapSize;
    } else {
      LOG.warn("An operation was reported to be complete multiple times. It's likely due to a "
          + "communication glitch and a retry.");
    }
  }

  public long registerOperationWithHeapSize(long heapSize) throws InterruptedException {
    long operationId = operationSequenceGenerator.incrementAndGet();
    synchronized (pendingOperationsWithSize) {
      while (isFull.get()) {
        Long completedOperationId =
            completedOperationIds.poll(REGISTER_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        if (completedOperationId != null) {
          cleanupFinishedOperations(completedOperationId);
        }
      }
      register(heapSize, operationId);
      return operationId;
    }
  }

  /**
   * @param heapSize
   * @param operationId
   */
  private void register(long heapSize, long operationId) {
    pendingOperationsWithSize.put(operationId, heapSize);
    currentWriteBufferSize += heapSize;
    pendingOperationCount.incrementAndGet();
    lastOperationChange = System.currentTimeMillis();
    if (checkIsFull()) {
      isFull.set(true);
    }
  }

  private boolean checkIsFull() {
    return currentWriteBufferSize >= maxHeapSize
        || pendingOperationsWithSize.size() >= maxInFlightRpcs;
  }

  @VisibleForTesting
  public boolean isFull() {
    synchronized (pendingOperationsWithSize) {
      cleanupFinishedOperations();
      return isFull.get();
    }
  }

  public boolean hasInflightRequests() {
    synchronized (pendingOperationsWithSize) {
      cleanupFinishedOperations();
      return !pendingOperationsWithSize.isEmpty();
    }
  }

  @VisibleForTesting
  long getHeapSize() {
    synchronized (pendingOperationsWithSize) {
      return currentWriteBufferSize;
    }
  }

  public <T> FutureCallback<T> addCallback(ListenableFuture<T> future, final Long id) {
    FutureCallback<T> callback = new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        markCanBeCompleted(id);
      }

      @Override
      public void onFailure(Throwable t) {
        markCanBeCompleted(id);
      }
    };
    Futures.addCallback(future, callback);
    return callback;
  }

  public void markCanBeCompleted(Long id) {
    completedOperationIds.offer(id);
    int inflightOperationCount = pendingOperationCount.decrementAndGet();

    // We usually don't want to synchronize in the callback because that holds up the gRPC transport
    // thread. The only case we care about is when flush() is waiting for all of the operations to
    // complete. Only synchornize and notify() when flush() can complete.
    if (this.isFlushing.get() && inflightOperationCount <= 0) {
      int count = 0;
      synchronized (pendingOperationsWithSize) {
        cleanupFinishedOperations();
        count = pendingOperationsWithSize.size();
      }
      if (count == 0) {
        synchronized (this) {
          notifyAll();
        }
      }
    }
  }
}
