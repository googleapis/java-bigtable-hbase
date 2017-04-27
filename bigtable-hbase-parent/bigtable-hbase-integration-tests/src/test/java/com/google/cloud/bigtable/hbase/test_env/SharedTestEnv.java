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
import java.io.IOException;
import org.apache.hadoop.hbase.client.Connection;

/**
 * A singleton test environment.
 *
 * This can be either be backed by bigtable or an hbase minicluster. In both cases a minihadoop cluster will be running
 * to submit mapreduce jobs.
 */
abstract class SharedTestEnv {
  private static SharedTestEnv instance;
  private long refCount = 0;


  static synchronized SharedTestEnv get() throws Exception {
    if (instance == null) {
      instance = create();
    }
    instance.retain();

    return instance;
  }

  private static SharedTestEnv create() {
    String testEnvType = System.getProperty("BIGTABLE_TEST_ENV", "minicluster");

    switch (testEnvType) {
      case "bigtable":
        return new BigtableEnv();
      case "minicluster":
        return new MiniClusterEnv();
      default:
        throw new RuntimeException("Invalid BIGTABLE_TEST_ENV: " + testEnvType);
    }
  }

  private synchronized void retain() throws Exception {
    refCount++;
    if (refCount == 1) {
      setup();
    }
  }

  synchronized void release() throws IOException {
    Preconditions.checkArgument(refCount > 0, "Too many releases");
    refCount--;
    if (refCount == 0) {
      teardown();
    }
  }

  protected abstract void setup() throws Exception;

  protected abstract void teardown() throws IOException;

  public abstract Connection createConnection() throws IOException;
}
