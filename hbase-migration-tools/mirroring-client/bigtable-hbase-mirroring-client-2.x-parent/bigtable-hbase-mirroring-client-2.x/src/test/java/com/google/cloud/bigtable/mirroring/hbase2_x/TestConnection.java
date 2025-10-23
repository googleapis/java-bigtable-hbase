/*
 * Copyright 2025 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.security.User;

public class TestConnection extends com.google.cloud.bigtable.mirroring.core.TestConnection
    implements Connection {

  public TestConnection(Configuration conf, boolean managed, ExecutorService pool, User user) {
    super(conf, managed, pool, user);
  }

  public TestConnection(Configuration conf, ExecutorService pool, User user) {
    super(conf, false, pool, user);
  }

  @Override
  public void clearRegionLocationCache() {}

  @Override
  public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService) {
    return null;
  }
}
