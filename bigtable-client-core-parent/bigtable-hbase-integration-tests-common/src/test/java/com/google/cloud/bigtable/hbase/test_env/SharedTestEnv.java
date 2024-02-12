/*
 * Copyright 2017 Google LLC
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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;

/**
 * A singleton test environment.
 *
 * <p>This can be either be backed by bigtable or an hbase minicluster. In both cases a minihadoop
 * cluster will be running to submit mapreduce jobs.
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
    String testEnv = System.getProperty("google.bigtable.test_env");
    switch (testEnv) {
      case "minicluster":
        return new MiniClusterEnv();
      case "cloud":
        return new CloudEnv();
      case "emulator":
        return new EmulatorEnv();
      default:
        throw new IllegalStateException("unsupported test environment");
    }
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  private synchronized void retain() throws Exception {
    refCount++;
    if (refCount == 1) {
      AtomicInteger threadIndex = new AtomicInteger();
      executor =
          Executors.newCachedThreadPool(
              (r) -> {
                Thread t = new Thread(r);
                t.setName("shared-test-env-rule-" + threadIndex.getAndIncrement());
                t.setDaemon(true);
                return t;
              });
      setup();
    }
  }

  synchronized void release() throws IOException {
    if (refCount < 1) {
      throw new IllegalStateException("Too many releases");
    }
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
