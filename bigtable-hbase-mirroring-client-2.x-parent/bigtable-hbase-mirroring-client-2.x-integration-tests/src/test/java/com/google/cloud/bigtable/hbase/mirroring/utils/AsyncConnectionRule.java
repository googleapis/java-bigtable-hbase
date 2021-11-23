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

import com.google.cloud.bigtable.mirroring.hbase2_x.MirroringAsyncConnection;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.rules.ExternalResource;

public class AsyncConnectionRule extends ExternalResource {
  private final ConnectionRule connectionRule;

  public AsyncConnectionRule(ConnectionRule connectionRule) {
    this.connectionRule = connectionRule;
  }

  public MirroringAsyncConnection createAsyncConnection(Configuration configuration) {
    this.connectionRule.updateConfigurationWithHbaseMiniClusterProps(configuration);

    try {
      AsyncConnection conn = ConnectionFactory.createAsyncConnection(configuration).get();
      return (MirroringAsyncConnection) conn;
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
