package com.google.cloud.bigtable.grpc.async;

import com.google.cloud.bigtable.config.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class limits access by RPCs to system resources
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ResourceLimiter {
  private static final Logger LOG = new Logger(ResourceLimiter.class);

  private static final long REGISTER_WAIT_MILLIS = 5;

  private final long maxHeapSize;
  private final int maxInFlightRpcs;
  private final AtomicLong operationSequenceGenerator = new AtomicLong();
  private final Map<Long, Long> pendingOperationsWithSize = new HashMap<>();
  private final LinkedBlockingDeque<Long> completedOperationIds = new LinkedBlockingDeque<>();
  private long currentWriteBufferSize;
  private final ResourceLimiterStats resourceLimiterStats = new ResourceLimiterStats();

  /**
   * <p>Constructor for ResourceLimiter.</p>
   *
   * @param maxHeapSize a long.
   * @param maxInFlightRpcs a int.
   */
  public ResourceLimiter(long maxHeapSize, int maxInFlightRpcs) {
    this.maxHeapSize = maxHeapSize;
    this.maxInFlightRpcs = maxInFlightRpcs;
  }

  /**
   * Register an operation with the given size before sending.
   * This call WILL BLOCK until resources are available. This method must
   * be paired with a call to {@code markCanBeCompleted} in order to make sure
   * resources are properly released.
   *
   * @param heapSize The serialized size of the RPC to be sent
   * @return A unique operation id
   * @throws java.lang.InterruptedException if any.
   */
  public synchronized long registerOperationWithHeapSize(long heapSize)
      throws InterruptedException {
    long operationId = operationSequenceGenerator.incrementAndGet();
    long start = System.nanoTime();
    while (unsynchronizedIsFull()) {
      waitForCompletions(REGISTER_WAIT_MILLIS);
    }
    resourceLimiterStats.markThrottling(System.nanoTime() - start);

    pendingOperationsWithSize.put(operationId, heapSize);
    currentWriteBufferSize += heapSize;
    return operationId;
  }


  public ResourceLimiterStats getResourceLimiterStats() {
	return resourceLimiterStats;
  }

  /**
   * Mark an operation id, as returned by {@code registerOperationWithHeapSize}, as complete
   *
   * @param id a long.
   */
  public void markCanBeCompleted(long id) {
    completedOperationIds.offerLast(id);
  }

  /**
   * <p>Getter for the field <code>maxHeapSize</code>.</p>
   *
   * @return The maximum allowed number of bytes across all across all outstanding RPCs
   */
  public long getMaxHeapSize() {
    return maxHeapSize;
  }

  /**
   * <p>Getter for the field <code>maxInFlightRpcs</code>.</p>
   *
   * @return The maximum allowed number of in-flight RPCs
   */
  public int getMaxInFlightRpcs() {
    return maxInFlightRpcs;
  }

  /**
   * <p>getHeapSize.</p>
   *
   * @return The total size of all currently outstanding RPCs
   */
  public long getHeapSize() {
    return currentWriteBufferSize;
  }

  /**
   * <p>isFull.</p>
   *
   * @return true if no more RPCs can be started, false otherwise
   */
  public synchronized boolean isFull() {
    return unsynchronizedIsFull();
  }

  private boolean isFullInternal() {
    return currentWriteBufferSize >= maxHeapSize
        || pendingOperationsWithSize.size() >= maxInFlightRpcs;
  }

  private boolean unsynchronizedIsFull() {
    if (!isFullInternal()) {
      return false;
    }
    // If we're not full, don't worry about cleaning up just yet.
    cleanupFinishedOperations();
    return isFullInternal();
  }

  /**
   * <p>hasInflightRequests.</p>
   *
   * @return true if there are currently in-flight RPCs
   */
  public synchronized boolean hasInflightRequests() {
    cleanupFinishedOperations();
    return !pendingOperationsWithSize.isEmpty();
  }

  private void cleanupFinishedOperations() {
    List<Long> toClean = new ArrayList<>();
    completedOperationIds.drainTo(toClean);
    if (!toClean.isEmpty()) {
      markOperationsCompleted(toClean);
    }
  }

  private synchronized void markOperationsCompleted(List<Long> operationSequenceIds) {
    for (Long operationSequenceId : operationSequenceIds) {
      markOperationComplete(operationSequenceId);
    }
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

  private void markOperationComplete(Long operationSequenceId) {
    Long heapSize = pendingOperationsWithSize.remove(operationSequenceId);
    if (heapSize != null) {
      currentWriteBufferSize -= heapSize;
    } else {
      LOG.warn("An operation completed successfully but provided multiple completion notifications."
          + " Please notify Google that this occurred.");
    }
  }
}
