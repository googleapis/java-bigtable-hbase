/*
 * Copyright 2018 Google LLC All Rights Reserved.
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

import com.google.cloud.bigtable.hbase.AbstractTestCreateTable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

@SuppressWarnings("deprecation")
public class TestAsyncCreateTable extends AbstractTestCreateTable {

  @Override
  protected void createTable(TableName tableName) throws Exception {
    try {
      getAsyncAdmin().createTable(createDescriptor(tableName)).get();
    } catch(ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected void createTable(TableName tableName, byte[] start, byte[] end,
      int splitCount)
      throws Exception {
    try {
      getAsyncAdmin().createTable(createDescriptor(tableName), start, end, splitCount).get();
    } catch(ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected void createTable(TableName tableName, byte[][] ranges) throws Exception {
    try {
      getAsyncAdmin().createTable(createDescriptor(tableName), ranges).get();
    } catch(ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  private TableDescriptor createDescriptor(TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build())
        .build();
  }

  private AsyncAdmin getAsyncAdmin() throws InterruptedException, ExecutionException {
    return AbstractAsyncTest.getAsyncConnection().getAdmin();
  }
}
