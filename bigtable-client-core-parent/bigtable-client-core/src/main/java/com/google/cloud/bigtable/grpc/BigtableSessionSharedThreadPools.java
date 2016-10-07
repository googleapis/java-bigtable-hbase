/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
 * few {@link com.google.cloud.bigtable.grpc.BigtableSession}s.  All executors are automatically expand if there is higher use.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableSessionSharedThreadPools {
  /** Constant <code>BATCH_POOL_THREAD_NAME="bigtable-batch-pool"</code> */
  public static final String BATCH_POOL_THREAD_NAME = "bigtable-batch-pool";
  /** Constant <code>RETRY_THREADPOOL_NAME="bigtable-rpc-retry"</code> */
  public static final String RETRY_THREADPOOL_NAME = "bigtable-rpc-retry";
  /** Constant <code>GRPC_EVENTLOOP_GROUP_NAME="bigtable-grpc-elg"</code> */
  public static final String GRPC_EVENTLOOP_GROUP_NAME = "bigtable-grpc-elg";

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

  /**
   * <p>Constructor for BigtableSessionSharedThreadPools.</p>
   */
  protected BigtableSessionSharedThreadPools() {
    init();
  }

  /**
   * <p>init.</p>
   */
  protected void init() {
    batchThreadPool = Executors.newCachedThreadPool(createThreadFactory(BATCH_POOL_THREAD_NAME));
    elg = new NioEventLoopGroup(0, createThreadFactory(GRPC_EVENTLOOP_GROUP_NAME));
    retryExecutor = Executors.newScheduledThreadPool(RETRY_THREAD_COUNT,
      createThreadFactory(RETRY_THREADPOOL_NAME));
  }

  /**
   * <p>createThreadFactory.</p>
   *
   * @param name a {@link java.lang.String} object.
   * @return a {@link java.util.concurrent.ThreadFactory} object.
   */
  protected ThreadFactory createThreadFactory(String name) {
    return ThreadPoolUtil.createThreadFactory(name);
  }

  /**
   * <p>Getter for the field <code>batchThreadPool</code>.</p>
   *
   * @return a {@link java.util.concurrent.ExecutorService} object.
   */
  public ExecutorService getBatchThreadPool() {
    return batchThreadPool;
  }

  /**
   * <p>Getter for the field <code>elg</code>.</p>
   *
   * @return a {@link io.netty.channel.nio.NioEventLoopGroup} object.
   */
  public NioEventLoopGroup getElg() {
    return elg;
  }

  /**
   * <p>Getter for the field <code>retryExecutor</code>.</p>
   *
   * @return a {@link java.util.concurrent.ScheduledExecutorService} object.
   */
  public ScheduledExecutorService getRetryExecutor() {
    return retryExecutor;
  }
}
