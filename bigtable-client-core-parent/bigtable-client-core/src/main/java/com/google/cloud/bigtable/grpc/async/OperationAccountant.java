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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

  private final Object signal = new String("");
  private AtomicInteger count = new AtomicInteger();

  private long noSuccessCheckDeadlineNanos;
  private int noSuccessWarningCount;

  /**
   * <p>Constructor for {@link OperationAccountant}.</p>
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
   * must be paired with a call to {@link #onOperationCompletion()}.
   */
  public void registerOperation(final ListenableFuture<?> future) {
    count.incrementAndGet();
    Futures.addCallback(future, new FutureCallback<Object>() {
      @Override
      public void onSuccess(Object result) {
        onOperationCompletion();
      }

      @Override
      public void onFailure(Throwable t) {
        onOperationCompletion();
      }
    });
  }

  /**
   * Blocks until all outstanding RPCs and retries have completed
   *
   * @throws java.lang.InterruptedException if any.
   */
  public void awaitCompletion() throws InterruptedException {
    boolean performedWarning = false;

    while (hasInflightOperations()) {
      synchronized (signal) {
        if (hasInflightOperations()) {
          signal.wait(finishWaitMillis);
        }
      }

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
  }

  // ////// HELPERS

  private boolean onOperationCompletion() {
    resetNoSuccessWarningDeadline();
    if (count.decrementAndGet() == 0) {
      synchronized (signal) {
        signal.notifyAll();
      }
    }
    return true;
  }


  private void logNoSuccessWarning(long now) {
    long lastUpdateNanos = now - noSuccessCheckDeadlineNanos + INTERVAL_NO_SUCCESS_WARNING_NANOS;
    long lastUpdated = TimeUnit.NANOSECONDS.toSeconds(lastUpdateNanos);
    LOG.warn(
      "No operations completed within the last %d seconds. "
          + "There are still %d operations in progress.",
      lastUpdated, count.get());
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
    return count.get() > 0;
  }

  private void resetNoSuccessWarningDeadline() {
    noSuccessCheckDeadlineNanos = clock.nanoTime() + INTERVAL_NO_SUCCESS_WARNING_NANOS;
  }

  @VisibleForTesting
  int getNoSuccessWarningCount() {
    return noSuccessWarningCount;
  }
}
