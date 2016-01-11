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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.cloud.bigtable.config.Logger;
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
  private static final long FINISH_WAIT_MILLIS = 250;
  private static final long REGISTER_WAIT_MILLIS = 5;

  // In flush, wait up to this number of milliseconds without any operations completing.  If
  // this amount of time goes by without any updates, flush will log a warning.  Flush()
  // will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING = 300000;
  private final long maxHeapSize;
  private final int maxInFlightRpcs;
  private final Map<Long, Long> pendingOperationsWithSize = new HashMap<>();
  private final LinkedBlockingDeque<Long> completedOperationIds = new LinkedBlockingDeque<>();

  private long currentWriteBufferSize = 0;
  private AtomicLong operationSequenceGenerator = new AtomicLong();
  private long lastOperationChange = System.currentTimeMillis();

  public HeapSizeManager(long maxHeapSize, int maxInflightRpcs) {
    this.maxHeapSize = maxHeapSize;
    this.maxInFlightRpcs = maxInflightRpcs;
  }

  public long getMaxHeapSize() {
    return maxHeapSize;
  }
  
  public int getMaxInFlightRpcs() {
    return maxInFlightRpcs;
  }

  public synchronized void flush() throws InterruptedException {
    boolean performedWarning = false;
    cleanupFinishedOperations();
    while(!pendingOperationsWithSize.isEmpty()) {
      if (!performedWarning
          && lastOperationChange + INTERVAL_NO_SUCCESS_WARNING < System.currentTimeMillis()) {
        long lastUpdated = (System.currentTimeMillis() - lastOperationChange) / 1000;
        LOG.warn("No operations completed within the last %d seconds. "
            + "There are still %d operations in progress.", lastUpdated,
          pendingOperationsWithSize.size());
        performedWarning = true;
      }
      waitForCompletions(FINISH_WAIT_MILLIS);
      cleanupFinishedOperations();
    }
    if (performedWarning) {
      LOG.info("flush() completed");
    }
  }


  private void cleanupFinishedOperations() {
    List<Long> toClean = new ArrayList<>();
    completedOperationIds.drainTo(toClean);
    if (!toClean.isEmpty()) {
      markOperationsCompleted(toClean);
    }
  }
  public synchronized long registerOperationWithHeapSize(long heapSize)
      throws InterruptedException {
    long operationId = operationSequenceGenerator.incrementAndGet();
    while (unsynchronizedIsFull()) {
      waitForCompletions(REGISTER_WAIT_MILLIS);
    }

    lastOperationChange = System.currentTimeMillis();
    pendingOperationsWithSize.put(operationId, heapSize);
    currentWriteBufferSize += heapSize;
    return operationId;
  }

  /**
   * Waits for a completion and then marks it as complete.
   * @throws InterruptedException
   */
  private void waitForCompletions(long timeoutMs) throws InterruptedException {
    Long completedOperation =
        this.completedOperationIds.pollFirst(timeoutMs, TimeUnit.MILLISECONDS);
    if (completedOperation != null) {
      markOperationComplete(completedOperation);
    }
  }

  public synchronized boolean isFull() {
    return unsynchronizedIsFull();
  }

  private boolean unsynchronizedIsFull() {
    if (!isFullInternal()) {
      return false;
    }
    // If we're not full, don't worry about cleaning up just yet.
    cleanupFinishedOperations();
    return isFullInternal();
  }

  private boolean isFullInternal() {
    return currentWriteBufferSize >= maxHeapSize
        || pendingOperationsWithSize.size() >= maxInFlightRpcs;
  }

  private synchronized void markOperationsCompleted(List<Long> operationSequenceIds) {
    for (Long operationSequenceId : operationSequenceIds) {
      markOperationComplete(operationSequenceId);
    }
    lastOperationChange = System.currentTimeMillis();
    notifyAll();
  }

  private void markOperationComplete(Long operationSequenceId) {
    Long heapSize = pendingOperationsWithSize.remove(operationSequenceId);
    if (heapSize != null) {
      currentWriteBufferSize -= heapSize;
    } else {
      LOG.warn("An operation completion was recieved multiple times. Your operations completed."
          + " Please notify Google that this occurred.");
    }
  }

  public synchronized boolean hasInflightRequests() {
    cleanupFinishedOperations();
    return !pendingOperationsWithSize.isEmpty();
  }

  synchronized long getHeapSize() {
    return currentWriteBufferSize;
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
  }
}