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

import com.google.cloud.bigtable.util.ThreadPoolUtil;

import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * This class contains executors and other thread pool related resources that can be reused across a
 * few {@link BigtableSession}s.  All executors are automatically expand if there is higher use.
 */
public class BigtableSessionSharedThreadPools {
  public static final String BATCH_POOL_THREAD_NAME = "bigtable-batch-pool";
  public static final String RETRY_THREADPOOL_NAME = "bigtable-rpc-retry";
  public static final String GRPC_EVENTLOOP_GROUP_NAME = "bigtable-grpc-elg";

  /** Number of threads to use to initiate retry calls */
  public static final int RETRY_THREAD_COUNT = 4;

  private static BigtableSessionSharedThreadPools INSTANCE = new BigtableSessionSharedThreadPools();

  /**
   * Get the shared instance of ThreadPools.
   */
  public static BigtableSessionSharedThreadPools getInstance() {
    return INSTANCE;
  }

  /**
   * This is used to do pre and post RPC work, and not the i/o itself.
   */
  protected ExecutorService batchThreadPool;

  /**
   * This is needed by nio. We create daemon threads rather than default threads so that if a user
   * shuts down a JVM, the bigtable connection doesn't block the shutdown. By default, the ELG is
   * not a daemon thread pool.
   */
  protected NioEventLoopGroup elg;

  /**
   * Used for a few cases that benefit from retries, such as puts, gets and scans.
   */
  protected ScheduledExecutorService retryExecutor;

  protected BigtableSessionSharedThreadPools() {
    init();
  }

  protected void init() {
    batchThreadPool = Executors.newCachedThreadPool(createThreadFactory(BATCH_POOL_THREAD_NAME));
    elg = new NioEventLoopGroup(0, createThreadFactory(GRPC_EVENTLOOP_GROUP_NAME));
    retryExecutor = Executors.newScheduledThreadPool(RETRY_THREAD_COUNT,
      createThreadFactory(RETRY_THREADPOOL_NAME));
  }

  protected ThreadFactory createThreadFactory(String name) {
    return ThreadPoolUtil.createThreadFactory(name);
  }

  public ExecutorService getBatchThreadPool() {
    return batchThreadPool;
  }

  public NioEventLoopGroup getElg() {
    return elg;
  }

  public ScheduledExecutorService getRetryExecutor() {
    return retryExecutor;
  }
}
