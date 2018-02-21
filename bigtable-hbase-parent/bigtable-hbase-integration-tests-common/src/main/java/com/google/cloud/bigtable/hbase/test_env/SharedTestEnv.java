/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.test_env;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;

/**
 * A singleton test environment.
 *
 * This can be either be backed by bigtable or an hbase minicluster. In both cases a minihadoop cluster will be running
 * to submit mapreduce jobs.
 */
abstract class SharedTestEnv {

  private static SharedTestEnv instance;
  private ExecutorService executor;
  private long refCount = 0;
  protected Configuration configuration;

  static synchronized SharedTestEnv get() throws Exception {
    if (instance == null) {
      instance = create();
    }
    instance.retain();

    return instance;
  }

  private static SharedTestEnv create() {
    String bigtableConnectionClassName = System.getProperty("google.bigtable.connection.impl", "");

    if (!bigtableConnectionClassName.isEmpty()) {
      return new BigtableEnv();
    } else {
      return new MiniClusterEnv();
    }
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  private synchronized void retain() throws Exception {
    refCount++;
    if (refCount == 1) {
      executor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("shared-test-env-rule").build());
      setup();
    }
  }

  synchronized void release() throws IOException {
    Preconditions.checkArgument(refCount > 0, "Too many releases");
    refCount--;
    if (refCount == 0) {
      teardown();
      executor.shutdownNow();
    }
  }

  protected abstract void setup() throws Exception;

  protected abstract void teardown() throws IOException;

  public Configuration getConfiguration() {
    return configuration;
  }
}
