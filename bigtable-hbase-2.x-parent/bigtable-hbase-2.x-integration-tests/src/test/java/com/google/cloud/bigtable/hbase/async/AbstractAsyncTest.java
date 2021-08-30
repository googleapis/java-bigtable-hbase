/*
 * Copyright 2018 Google LLC
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
package com.google.cloud.bigtable.hbase.async;

import com.google.cloud.bigtable.hbase.AbstractTest;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ScanResultConsumer;

public abstract class AbstractAsyncTest extends AbstractTest {

  private static final String CONN_KEY = AbstractAsyncTest.class.getName() + "_asyncCon";

  public static synchronized AsyncConnection getAsyncConnection()
      throws InterruptedException, ExecutionException {
    SharedTestEnvRule sharedEnv = SharedTestEnvRule.getInstance();
    AsyncConnection conn = (AsyncConnection) sharedEnv.getClosable(CONN_KEY);
    if (conn == null) {
      Configuration sharedEnvConfiguration = sharedEnv.getConfiguration();
      conn = ConnectionFactory.createAsyncConnection(sharedEnvConfiguration).get();
      sharedEnv.registerClosable(CONN_KEY, conn);
    }
    return conn;
  }

  protected AsyncTable<ScanResultConsumer> getDefaultAsyncTable()
      throws InterruptedException, ExecutionException {
    return getAsyncConnection()
        .getTable(sharedTestEnv.getDefaultTableName(), sharedTestEnv.getExecutor());
  }
}
