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


import com.google.cloud.bigtable.config.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Throttles the number of RPCs that are outstanding at any point in time.
 */
public class RpcThrottler {
  protected static final Logger LOG = new Logger(RpcThrottler.class);

  private static final long FINISH_WAIT_MILLIS = 250;

  // In awaitCompletion, wait up to this number of milliseconds without any operations completing.  If
  // this amount of time goes by without any updates, awaitCompletion will log a warning.  Flush()
  // will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING = 300000;

  private final ResourceLimiter resourceLimiter;

  private ReentrantLock lock = new ReentrantLock();
  private Condition flushedCondition = lock.newCondition();
  private Set<Long> outstandingRequests = new HashSet<>();
  private Set<Long> outstandingRetries = new HashSet<>();
  private AtomicLong retrySequenceGenerator = new AtomicLong();
  private long lastOperationChange = System.currentTimeMillis();

  public RpcThrottler(ResourceLimiter resourceLimiter) {
    this.resourceLimiter = resourceLimiter;
  }

  /**
   * Register a new RPC operation. Blocks until the requested resources are available.
   * This method must be paired with a call to {@code addCallback}.
   * @param heapSize The serialized size of the RPC
   * @return An operation id
   */
  public long registerOperationWithHeapSize(long heapSize)
      throws InterruptedException {
    long id = resourceLimiter.registerOperationWithHeapSize(heapSize);

    lock.lock();
    try {
      outstandingRequests.add(id);
    } finally {
      lock.unlock();
    }
    return id;
  }

  /**
   * Add a callback to a Future representing an RPC call with the given
   * operation id that will clean upon completion and reclaim any utilized resources.
   * This method must be paired with every call to {@code registerOperationWithHeapSize}.
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
   * Registers a retrying Future such that, if a retry is necessary, it
   * will be complete before a call to {@code awaitCompletion} returns.
   * Retries do not count against any RPC resource limits.
   */
  public <T> void registerRetry(ListenableFuture<T> retryFuture) {
    final long id = retrySequenceGenerator.incrementAndGet();

    lock.lock();
    try {
      outstandingRetries.add(id);
    } finally {
      lock.unlock();
    }
    addRetryCallback(retryFuture, id);
  }

  private <T> void addRetryCallback(ListenableFuture<T> retryFuture, final long id) {
    FutureCallback<T> callback = new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        onRetryCompletion(id);
      }

      @Override
      public void onFailure(Throwable t) {
        onRetryCompletion(id);
      }
    };
    Futures.addCallback(retryFuture, callback);
  }

  /**
   * Blocks until all outstanding RPCs and retries have completed
   * @throws InterruptedException
   */
  public void awaitCompletion() throws InterruptedException {
    boolean performedWarning = false;

    lock.lock();
    try {
      while (!isFlushed()) {
        flushedCondition.await(FINISH_WAIT_MILLIS, TimeUnit.MILLISECONDS);

        if (!performedWarning
            && lastOperationChange + INTERVAL_NO_SUCCESS_WARNING < System.currentTimeMillis()) {
          logNoSuccessWarning();
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

  private void logNoSuccessWarning() {
    long lastUpdated = (System.currentTimeMillis() - lastOperationChange) / 1000;
    LOG.warn("No operations completed within the last %d seconds. "
            + "There are still %d operations in progress.", lastUpdated,
        outstandingRequests.size());
  }

  /**
   * @return The maximum allowed number of bytes across all across all outstanding RPCs
   */
  public long getMaxHeapSize() {
    return resourceLimiter.getMaxHeapSize();
  }

  /**
   * @return true if there are any outstanding requests being tracked by this throttler
   */
  public boolean hasInflightRequests() {
    lock.lock();
    try {
      return outstandingRequests.size() > 0;
    } finally {
      lock.unlock();
    }
  }

  private boolean isFlushed() {
    return outstandingRequests.isEmpty() && outstandingRetries.isEmpty();
  }

  @VisibleForTesting
  void onRpcCompletion(long id) {
    resourceLimiter.markCanBeCompleted(id);

    lock.lock();
    try {
      outstandingRequests.remove(id);
      if (isFlushed()) {
        flushedCondition.signal();
      }
    } finally {
      lock.unlock();
    }
    lastOperationChange = System.currentTimeMillis();
  }

  private void onRetryCompletion(long id) {
    lock.lock();
    try {
      outstandingRetries.remove(id);
      if (isFlushed()) {
        flushedCondition.signal();
      }
    } finally {
      lock.unlock();
    }
    lastOperationChange = System.currentTimeMillis();
  }
}