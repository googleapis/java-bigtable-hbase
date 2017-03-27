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
package com.google.cloud.bigtable.grpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.grpc.internal.GrpcUtil;

/**
 * This class contains executors and other thread pool related resources that can be reused across a
 * few {@link com.google.cloud.bigtable.grpc.BigtableSession}s.  All executors are automatically expand if there is higher use.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableSessionSharedThreadPools {
  /** Constant <code>BATCH_POOL_THREAD_NAME="bigtable-batch-pool-%d"</code> */
  private static final String BATCH_POOL_THREAD_NAME_PATTERN = "bigtable-batch-pool-%d";
  /** Constant <code>RETRY_THREADPOOL_NAME="bigtable-rpc-retry-%d"</code> */
  private static final String RETRY_THREADPOOL_NAME_PATTERN = "bigtable-rpc-retry-%d";

  /** Number of threads to use to initiate retry calls */
  public static final int RETRY_THREAD_COUNT = 4;

  private static BigtableSessionSharedThreadPools INSTANCE = new BigtableSessionSharedThreadPools();

  /**
   * Get the shared instance of ThreadPools.
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools} object.
   */
  public static BigtableSessionSharedThreadPools getInstance() {
    return INSTANCE;
  }

  /**
   * This is used to do i/o work.
   */
  protected ExecutorService batchThreadPool;

  /**
   * Used for a few cases that benefit from retries, such as puts, gets and scans.
   */
  protected ScheduledExecutorService retryExecutor;

  /**
   * <p>Getter for the field <code>batchThreadPool</code>.</p>
   *
   * @return a {@link java.util.concurrent.ExecutorService} object.
   */
  public synchronized ExecutorService getBatchThreadPool() {
    if (batchThreadPool == null) {
      batchThreadPool = Executors
          .newCachedThreadPool(GrpcUtil.getThreadFactory(BATCH_POOL_THREAD_NAME_PATTERN, true));
    }
    return batchThreadPool;
  }

  /**
   * <p>Getter for the field <code>retryExecutor</code>.</p>
   *
   * @return a {@link java.util.concurrent.ScheduledExecutorService} object.
   */
  public synchronized ScheduledExecutorService getRetryExecutor() {
    if (retryExecutor == null) {
      retryExecutor = Executors.newScheduledThreadPool(RETRY_THREAD_COUNT,
        GrpcUtil.getThreadFactory(RETRY_THREADPOOL_NAME_PATTERN, true));
    }
    return retryExecutor;
  }
}
