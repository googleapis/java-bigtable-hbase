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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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


  // In awaitCompletion, wait up to this number of nanoseconds without any operations completing.  If
  // this amount of time goes by without any updates, awaitCompletion will log a warning.  Flush()
  // will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING_NANOS = TimeUnit.SECONDS.toNanos(30);

  private final NanoClock clock;
  private final long finishWaitMillis;

  private ReentrantLock lock = new ReentrantLock();
  private Condition flushedCondition = lock.newCondition();

  @GuardedBy("lock")
  private Set<Long> operations = new HashSet<>();

  private long noSuccessCheckDeadlineNanos;
  private int noSuccessWarningCount;

  /**
   * <p>Constructor for {@link OperationAccountant}.</p>
   *
   * @param resourceLimiter a {@link com.google.cloud.bigtable.grpc.async.ResourceLimiter} object.
   */
  public OperationAccountant() {
    this(NanoClock.SYSTEM, DEFAULT_FINISH_WAIT_MILLIS);
  }

  @VisibleForTesting
  OperationAccountant(NanoClock clock, long finishWaitMillis) {
    this.clock = clock;
    this.finishWaitMillis = finishWaitMillis;
    resetNoSuccessWarningDeadline();
  }

  /**
   * Register a new RPC operation. Blocks until the requested resources are available. This method
   * must be paired with a call to {@link #onOperationCompletion(long)}.
   * @param id The id of the RPC
   * @return An operation id
   * @throws java.lang.InterruptedException if any.
   */
  public void registerOperation(long id) {
    lock.lock();
    try {
      operations.add(id);
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  boolean onOperationCompletion(long id) {
    boolean response = false;
    lock.lock();
    try {
      response = operations.remove(id);
      if (isFlushed()) {
        flushedCondition.signal();
      }
    } finally {
      lock.unlock();
    }
    resetNoSuccessWarningDeadline();
    return response;
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
          + "There are still %d operations in progress.",
      lastUpdated, operations.size());
    noSuccessWarningCount++;
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
    return operations.isEmpty();
  }

  private void resetNoSuccessWarningDeadline() {
    noSuccessCheckDeadlineNanos = clock.nanoTime() + INTERVAL_NO_SUCCESS_WARNING_NANOS;
  }

  @VisibleForTesting
  int getNoSuccessWarningCount() {
    return noSuccessWarningCount;
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
