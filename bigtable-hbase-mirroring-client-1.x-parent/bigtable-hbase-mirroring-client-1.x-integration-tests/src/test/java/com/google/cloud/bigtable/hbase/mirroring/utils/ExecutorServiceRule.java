/*
 * Copyright 2021 Google LLC
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

package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.junit.rules.ExternalResource;

public class ExecutorServiceRule extends ExternalResource {
  private ConnectionRule connectionRule;
  public ExecutorService executorService;

  public ExecutorServiceRule(ConnectionRule connectionRule) {
    this.connectionRule = connectionRule;
  }

  public void before() {
    executorService = Executors.newCachedThreadPool();
  }

  public void after() {
    executorService.shutdown();
    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public MirroringConnection createConnection() throws IOException {
    return connectionRule.createConnection(executorService);
  }

  public MirroringConnection createConnection(Configuration configuration) throws IOException {
    return connectionRule.createConnection(executorService, configuration);
  }
}
