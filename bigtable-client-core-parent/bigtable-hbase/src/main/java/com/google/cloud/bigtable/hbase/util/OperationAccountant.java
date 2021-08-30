/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.util;

import com.google.api.core.ApiClock;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.api.core.NanoClock;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Throttles the number of operations that are outstanding at any point in time.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class OperationAccountant {
  /** Constant <code>LOG</code> */
  private static Logger LOG = new Logger(OperationAccountant.class);

  private static final long DEFAULT_FINISH_WAIT_MILLIS = 250;

  // In awaitCompletion, wait up to this number of nanoseconds without any operations completing.
  // If this amount of time goes by without any updates, awaitCompletion will log a warning.
  // Flush() will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING_NANOS = TimeUnit.SECONDS.toNanos(30);

  private final ApiClock clock;
  private final long finishWaitMillis;

  private final Object signal = new String("");
  private AtomicInteger count = new AtomicInteger();

  private long noSuccessCheckDeadlineNanos;
  private int noSuccessWarningCount;

  /** Constructor for {@link OperationAccountant}. */
  public OperationAccountant() {
    this(NanoClock.getDefaultClock(), DEFAULT_FINISH_WAIT_MILLIS);
  }

  @VisibleForTesting
  OperationAccountant(ApiClock clock, long finishWaitMillis) {
    this.clock = clock;
    this.finishWaitMillis = finishWaitMillis;
    resetNoSuccessWarningDeadline();
  }

  /**
   * Register a new RPC operation. Blocks until the requested resources are available. This method
   * must be paired with a call to {@link #onOperationCompletion()}.
   */
  public void registerOperation(final ApiFuture<?> future) {
    count.incrementAndGet();
    ApiFutures.addCallback(
        future,
        new ApiFutureCallback<Object>() {
          @Override
          public void onSuccess(Object result) {
            onOperationCompletion();
          }

          @Override
          public void onFailure(Throwable t) {
            onOperationCompletion();
          }
        },
        MoreExecutors.directExecutor());
  }

  /**
   * Blocks until all outstanding RPCs and retries have completed
   *
   * @throws InterruptedException if any.
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
   * hasInflightRequests.
   *
   * @return true if there are any outstanding requests being tracked by this {@link
   *     OperationAccountant}
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
