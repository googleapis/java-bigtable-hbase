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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

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
  private static final long WAIT_MILLIS = 250;

  // In flush, wait up to this number of milliseconds without any operations completing.  If
  // this amount of time goes by without any updates, flush will log a warning.  Flush()
  // will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING = 300000;
  private final long maxHeapSize;
  private final int maxInFlightRpcs;
  private final ExecutorService heapSizeExecutor;
  private final Map<Long, Long> pendingOperationsWithSize = new HashMap<>();

  private long currentWriteBufferSize = 0;
  private long operationSequenceGenerator = 0;
  private long lastOperationChange = System.currentTimeMillis();

  public HeapSizeManager(long maxHeapSize, int maxInflightRpcs, ExecutorService heapSizeExecutor) {
    this.maxHeapSize = maxHeapSize;
    this.maxInFlightRpcs = maxInflightRpcs;
    this.heapSizeExecutor = heapSizeExecutor;
  }

  public long getMaxHeapSize() {
    return maxHeapSize;
  }

  public synchronized void waitUntilAllOperationsAreDone() throws InterruptedException {
    boolean performedWarning = false;
    while(!pendingOperationsWithSize.isEmpty()) {
      if (!performedWarning
          && lastOperationChange + INTERVAL_NO_SUCCESS_WARNING < System.currentTimeMillis()) {
        long lastUpdated = (System.currentTimeMillis() - lastOperationChange) / 1000;
        LOG.warn("No operations completed within the last %d seconds."
            + "There are still %d operations in progress.", lastUpdated,
          pendingOperationsWithSize.size());
        performedWarning = true;
      }
      wait(WAIT_MILLIS);
    }
    if (performedWarning) {
      LOG.info("flush() completed");
    }
  }

  public synchronized long registerOperationWithHeapSize(long heapSize)
      throws InterruptedException {
    long operationId = ++operationSequenceGenerator;
    while (isFull()) {
      wait(WAIT_MILLIS);
    }

    lastOperationChange = System.currentTimeMillis();
    pendingOperationsWithSize.put(operationId, heapSize);
    currentWriteBufferSize += heapSize;
    return operationId;
  }

  public boolean isFull() {
    return currentWriteBufferSize >= maxHeapSize
        || pendingOperationsWithSize.size() >= maxInFlightRpcs;
  }

  public synchronized void markOperationCompleted(long operationSequenceId) {
    lastOperationChange = System.currentTimeMillis();
    Long heapSize = pendingOperationsWithSize.remove(operationSequenceId);
    if (heapSize != null) {
      currentWriteBufferSize -= heapSize;
      notifyAll();
    } else {
      LOG.warn("An operation completion was recieved multiple times. Your operations completed."
          + " Please notify Google that this occurred.");
    }
  }

  public synchronized boolean hasInflightRequests() {
    return !pendingOperationsWithSize.isEmpty();
  }

  synchronized long getHeapSize() {
    return currentWriteBufferSize;
  }

  public <T> void addCallback(ListenableFuture<T> future, final long id) {
    FutureCallback<T> callback = new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        markOperationCompleted(id);
      }

      @Override
      public void onFailure(Throwable t) {
        markOperationCompleted(id);
      }
    };
    Futures.addCallback(future, callback, heapSizeExecutor);
  }
}