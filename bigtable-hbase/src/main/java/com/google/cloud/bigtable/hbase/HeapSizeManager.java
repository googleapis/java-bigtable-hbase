package com.google.cloud.bigtable.hbase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * This class ensures that operations meet heap size and max RPC counts.  A wait will occur
 * if RPCs are requested after heap and RPC count thresholds are exceeded.
 */
@VisibleForTesting class HeapSizeManager {
  private final long maxHeapSize;
  private final int maxInFlightRpcs;
  private long currentWriteBufferSize = 0;
  private long operationSequenceGenerator = 0;

  // Flush is not properly synchronized with respect to waiting. It will never exit
  // improperly, but it might wait more than it has to. Setting this to a low value ensures
  // that improper waiting is minimal.
  static final long WAIT_MILLIS = 250;

  private final ExecutorService heapSizeExecutor;

  @VisibleForTesting
  final Map<Long, Long> pendingOperationsWithSize = new HashMap<>();
  private long lastOperationChange = System.currentTimeMillis();

  HeapSizeManager(long maxHeapSize, int maxInflightRpcs, ExecutorService executorService) {
    this.maxHeapSize = maxHeapSize;
    this.maxInFlightRpcs = maxInflightRpcs;
    this.heapSizeExecutor = executorService;
  }

  long getMaxHeapSize() {
    return maxHeapSize;
  }

  synchronized long registerOperationWithHeapSize(long heapSize)
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

  boolean isFull() {
    return currentWriteBufferSize >= maxHeapSize
        || pendingOperationsWithSize.size() >= maxInFlightRpcs;
  }

  @VisibleForTesting
  synchronized void operationComplete(long operationSequenceId) {
    lastOperationChange = System.currentTimeMillis();
    Long heapSize = pendingOperationsWithSize.remove(operationSequenceId);
    if (heapSize != null) {
      currentWriteBufferSize -= heapSize;
      notifyAll();
    } else {
      BigtableBufferedMutator.LOG.warn("An operation completion was recieved multiple times. Your operations completed."
          + " Please notify Google that this occurred.");
    }
  }

  synchronized boolean hasInflightRequests() {
    return !pendingOperationsWithSize.isEmpty();
  }

  @VisibleForTesting
  synchronized long getHeapSize() {
    return currentWriteBufferSize;
  }

  public <T> void addCallback(ListenableFuture<T> future, final Long sequenceId) {
    FutureCallback<T> callback = new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        operationComplete(sequenceId);
      }

      @Override
      public void onFailure(Throwable t) {
        operationComplete(sequenceId);
      }
    };
    Futures.addCallback(future, callback, heapSizeExecutor);
  }

  synchronized void waitUntilAllOperationsAreDone() throws InterruptedException {
    boolean performedWarning = false;
    while(!pendingOperationsWithSize.isEmpty()) {
      if (!performedWarning
          && lastOperationChange + BigtableBufferedMutator.INTERVAL_NO_SUCCESS_WARNING < System.currentTimeMillis()) {
        long lastUpdated = (System.currentTimeMillis() - lastOperationChange) / 1000;
        BigtableBufferedMutator.LOG.warn("No operations completed within the last %d seconds."
            + "There are still %d operations in progress.", lastUpdated,
          pendingOperationsWithSize.size());
        performedWarning = true;
      }
      wait(WAIT_MILLIS);
    }
    if (performedWarning) {
      BigtableBufferedMutator.LOG.info("flush() completed");
    }
  }
}
