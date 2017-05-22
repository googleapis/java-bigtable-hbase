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


import com.google.api.client.util.NanoClock;
import com.google.cloud.bigtable.config.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

/**
 * Throttles the number of operations that are outstanding at any point in time.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class OperationAccountant {
  /** Constant <code>LOG</code> */
  @VisibleForTesting
  static Logger LOG = new Logger(OperationAccountant.class);

  @VisibleForTesting
  static final long DEFAULT_FINISH_WAIT_MILLIS = 250;

  public static interface ComplexOperationStalenessHandler {
    void performRetryIfStale();
  }

  // In awaitCompletion, wait up to this number of nanoseconds without any operations completing.  If
  // this amount of time goes by without any updates, awaitCompletion will log a warning.  Flush()
  // will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING_NANOS = TimeUnit.SECONDS.toNanos(30);

  private final ResourceLimiter resourceLimiter;
  private final NanoClock clock;
  private final long finishWaitMillis;
  private final AtomicLong complexOperationIdGenerator = new AtomicLong();

  private ReentrantLock lock = new ReentrantLock();
  private Condition flushedCondition = lock.newCondition();

  @GuardedBy("lock")
  private Set<Long> operations = new HashSet<>();
  @GuardedBy("lock")
  private Map<Long, ComplexOperationStalenessHandler> complexOperations = new HashMap<>();

  private long noSuccessCheckDeadlineNanos;
  private int noSuccessWarningCount;

  /**
   * <p>Constructor for {@link OperationAccountant}.</p>
   *
   * @param resourceLimiter a {@link com.google.cloud.bigtable.grpc.async.ResourceLimiter} object.
   */
  public OperationAccountant(ResourceLimiter resourceLimiter) {
    this(resourceLimiter, NanoClock.SYSTEM, DEFAULT_FINISH_WAIT_MILLIS);
  }

  @VisibleForTesting
  OperationAccountant(ResourceLimiter resourceLimiter, NanoClock clock, long finishWaitMillis) {
    this.resourceLimiter = resourceLimiter;
    this.clock = clock;
    this.finishWaitMillis = finishWaitMillis;
    resetNoSuccessWarningDeadline();
  }

  /**
   * Register a new RPC operation. Blocks until the requested resources are available. This method
   * must be paired with a call to {@link #onOperationCompletion(long)}.
   * @param heapSize The serialized size of the RPC
   * @return An operation id
   * @throws java.lang.InterruptedException if any.
   */
  long registerOperationWithHeapSize(long heapSize)
      throws InterruptedException {
    long id = resourceLimiter.registerOperationWithHeapSize(heapSize);

    lock.lock();
    try {
      operations.add(id);
    } finally {
      lock.unlock();
    }
    return id;
  }

  /**
   * Registers a complex operation, like bulk mutation operations, that has a more subtle definition
   * of success than a normal operation. Bulk mutation RPCs can have some mutations succeed and some
   * fail; the failed mutations have to be retried in a subsequent RPC.
   * @return a long id of the complex operation.
   * @param handler a ComplexOperationStalenessHandler that will be periodically checked in
   *          {@link #awaitCompletion()}
   */
  // TODO: This functionality should be moved to BulkMutation where the functionality is used. The
  // abstraction.
  <T> long registerComplexOperation(ComplexOperationStalenessHandler handler) {
    final long id = complexOperationIdGenerator.incrementAndGet();

    lock.lock();
    try {
      complexOperations.put(id, handler);
    } finally {
      lock.unlock();
    }
    return id;
  }

  /**
   * Blocks until all outstanding RPCs and retries have completed
   *
   * @throws java.lang.InterruptedException if any.
   */
  public void awaitCompletion() throws InterruptedException {
    boolean performedWarning = false;

    lock.lock();
    try {
      while (!isFlushed()) {
        flushedCondition.await(finishWaitMillis, TimeUnit.MILLISECONDS);

        long now = clock.nanoTime();
        if (now >= noSuccessCheckDeadlineNanos) {
          // There are unusual cases where an RPC could be completed, but we don't clean up
          // the state and the locks.  Try to clean up if there is a timeout.
          ImmutableList<ComplexOperationStalenessHandler> toCheck =
              ImmutableList.copyOf(complexOperations.values());

          // The cleanup process can potentially incur deadlocks, so unlock to avoid deadlocking.
          lock.unlock();
          try {
            for (ComplexOperationStalenessHandler stalenessHandler : toCheck) {
              stalenessHandler.performRetryIfStale();
            }
          } finally {
            lock.lock();
          }
          if (isFlushed()) {
            break;
          }
          logNoSuccessWarning(now);
          resetNoSuccessWarningDeadline();
          performedWarning = true;
        }
      }
      if (performedWarning) {
        LOG.info("awaitCompletion() completed");
      }
    } finally {
      lock.unlock();
    }
  }

  private void logNoSuccessWarning(long now) {
    long lastUpdateNanos = now - noSuccessCheckDeadlineNanos + INTERVAL_NO_SUCCESS_WARNING_NANOS;
    long lastUpdated = TimeUnit.NANOSECONDS.toSeconds(lastUpdateNanos);
    LOG.warn(
      "No operations completed within the last %d seconds. "
          + "There are still %d simple operations and %d complex operations in progress.",
      lastUpdated, operations.size(), complexOperations.size());
    noSuccessWarningCount++;
  }

  /**
   * <p>getMaxHeapSize.</p>
   *
   * @return The maximum allowed number of bytes across all across all outstanding RPCs
   */
  public long getMaxHeapSize() {
    return resourceLimiter.getMaxHeapSize();
  }


  /**
   * <p>
   * hasInflightRequests.
   * </p>
   * @return true if there are any outstanding requests being tracked by this
   *         {@link OperationAccountant}
   */
  public boolean hasInflightOperations() {
    lock.lock();
    try {
      return !isFlushed();
    } finally {
      lock.unlock();
    }
  }

  /**
   * <p>
   * isFlushed.
   * </p>
   * @return true if there are no outstanding requests being tracked by this
   *         {@link OperationAccountant}
   */
  private boolean isFlushed() {
    return operations.isEmpty() && complexOperations.isEmpty();
  }

  private void resetNoSuccessWarningDeadline() {
    noSuccessCheckDeadlineNanos = clock.nanoTime() + INTERVAL_NO_SUCCESS_WARNING_NANOS;
  }

  @VisibleForTesting
  int getNoSuccessWarningCount() {
    return noSuccessWarningCount;
  }

  @VisibleForTesting
  void onOperationCompletion(long id) {
    resourceLimiter.markCanBeCompleted(id);

    lock.lock();
    try {
      operations.remove(id);
      if (isFlushed()) {
        flushedCondition.signal();
      }
    } finally {
      lock.unlock();
    }
    resetNoSuccessWarningDeadline();
  }

  /**
   * <p>onComplexOperationCompletion.</p>
   *
   * @param id a long.
   */
  public void onComplexOperationCompletion(long id) {
    lock.lock();
    try {
      complexOperations.remove(id);
      if (isFlushed()) {
        flushedCondition.signal();
      }
    } finally {
      lock.unlock();
    }
    resetNoSuccessWarningDeadline();
  }

  @VisibleForTesting
  void awaitCompletionPing(){
    lock.lock();
    try {
      flushedCondition.signal();
    } finally {
      lock.unlock();
    }
  }
}
