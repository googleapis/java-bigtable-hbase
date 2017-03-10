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
 * Throttles the number of RPCs that are outstanding at any point in time.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class RpcThrottler {
  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(RpcThrottler.class);

  private static final long DEFAULT_FINISH_WAIT_MILLIS = 250;

  public static interface RetryHandler {
    void performRetryIfStale();
  }

  // In awaitCompletion, wait up to this number of nanoseconds without any operations completing.  If
  // this amount of time goes by without any updates, awaitCompletion will log a warning.  Flush()
  // will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING_NANOS = TimeUnit.SECONDS.toNanos(30);

  private final ResourceLimiter resourceLimiter;
  private final NanoClock clock;
  private final long finishWaitMillis;
  private final AtomicLong retrySequenceGenerator = new AtomicLong();

  private ReentrantLock lock = new ReentrantLock();
  private Condition flushedCondition = lock.newCondition();

  @GuardedBy("lock")
  private Set<Long> outstandingRequests = new HashSet<>();
  @GuardedBy("lock")
  private Map<Long, RetryHandler> outstandingRetries = new HashMap<>();
  @GuardedBy("lock")
  private boolean isFlushed = true;

  private long noSuccessCheckDeadlineNanos;
  private int noSuccessWarningCount;

  /**
   * <p>Constructor for RpcThrottler.</p>
   *
   * @param resourceLimiter a {@link com.google.cloud.bigtable.grpc.async.ResourceLimiter} object.
   */
  public RpcThrottler(ResourceLimiter resourceLimiter) {
    this(resourceLimiter, NanoClock.SYSTEM, DEFAULT_FINISH_WAIT_MILLIS);
  }

  @VisibleForTesting
  RpcThrottler(ResourceLimiter resourceLimiter, NanoClock clock, long finishWaitMillis) {
    this.resourceLimiter = resourceLimiter;
    this.clock = clock;
    this.finishWaitMillis = finishWaitMillis;
    resetNoSuccessWarningDeadline();
  }

  /**
   * Register a new RPC operation. Blocks until the requested resources are available.
   * This method must be paired with a call to {@code addCallback}.
   *
   * @param heapSize The serialized size of the RPC
   * @return An operation id
   * @throws java.lang.InterruptedException if any.
   */
  public long registerOperationWithHeapSize(long heapSize)
      throws InterruptedException {
    long id = resourceLimiter.registerOperationWithHeapSize(heapSize);

    lock.lock();
    try {
      outstandingRequests.add(id);
      isFlushed = false;
    } finally {
      lock.unlock();
    }
    return id;
  }

  /**
   * Add a callback to a Future representing an RPC call with the given
   * operation id that will clean upon completion and reclaim any utilized resources.
   * This method must be paired with every call to {@code registerOperationWithHeapSize}.
   *
   * @param future a {@link com.google.common.util.concurrent.ListenableFuture} object.
   * @param id a long.
   * @return a {@link com.google.common.util.concurrent.FutureCallback} object.
   * @param <T> a T object.
   */
  public <T> FutureCallback<T> addCallback(ListenableFuture<T> future, final long id) {
    FutureCallback<T> callback = new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        onRpcCompletion(id);
      }

      @Override
      public void onFailure(Throwable t) {
        onRpcCompletion(id);
      }
    };
    Futures.addCallback(future, callback);
    return callback;
  }

  /**
   * Registers a retry, if a retry is necessary, it
   * will be complete before a call to {@code awaitCompletion} returns.
   * Retries do not count against any RPC resource limits.
   *
   * @return a long.
   * @param <T> a T object.
   */
  public <T> long registerRetry(RetryHandler handler) {
    final long id = retrySequenceGenerator.incrementAndGet();

    lock.lock();
    try {
      outstandingRetries.put(id, handler);
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
          lock.lock();
          try {
            // There are unusual cases where an RPC could be completed, but we don't clean up
            // the state and the locks.  Try to clean up if there is a timeout.
            for (RetryHandler retryHandler : outstandingRetries.values()) {
              retryHandler.performRetryIfStale();
            }
            if (isFlushed()) {
              break;
            }
          } finally {
            lock.unlock();
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
    LOG.warn("No operations completed within the last %d seconds. "
            + "There are still %d rpcs and %d retries in progress.", lastUpdated,
        outstandingRequests.size(), outstandingRetries.size());
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
   * <p>hasInflightRequests.</p>
   *
   * @return true if there are any outstanding requests being tracked by this throttler
   */
  public boolean hasInflightRequests() {
    return !isFlushed;
  }

  private boolean isFlushed() {
    return outstandingRequests.isEmpty() && outstandingRetries.isEmpty();
  }

  @VisibleForTesting
  void resetNoSuccessWarningDeadline() {
    noSuccessCheckDeadlineNanos = clock.nanoTime() + INTERVAL_NO_SUCCESS_WARNING_NANOS;
  }

  @VisibleForTesting
  int getNoSuccessWarningCount() {
    return noSuccessWarningCount;
  }

  @VisibleForTesting
  void onRpcCompletion(long id) {
    resourceLimiter.markCanBeCompleted(id);

    lock.lock();
    try {
      outstandingRequests.remove(id);
      if (isFlushed()) {
        flushedCondition.signal();
        isFlushed = true;
      }
    } finally {
      lock.unlock();
    }
    resetNoSuccessWarningDeadline();
  }

  /**
   * <p>onRetryCompletion.</p>
   *
   * @param id a long.
   */
  public void onRetryCompletion(long id) {
    lock.lock();
    try {
      outstandingRetries.remove(id);
      if (isFlushed()) {
        flushedCondition.signal();
        isFlushed = true;
      }
    } finally {
      lock.unlock();
    }
    resetNoSuccessWarningDeadline();
  }
}
