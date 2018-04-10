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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.hbase.AbstractTestSnapshot;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

@RunWith(JUnit4.class)
public class TestAsyncSnapshots extends AbstractTestSnapshot {

  private AsyncAdmin getAsyncAdmin() throws InterruptedException, ExecutionException {
    return AbstractAsyncTest.getAsyncConnection().getAdmin();
  }
  
  @Override
  protected void createTable(TableName tableName) throws IOException {
    try{
      getAsyncAdmin().createTable(createDescriptor(tableName)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while creating table: "+e.getCause());
    }
  }  
  
  private TableDescriptor createDescriptor(TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(SharedTestEnvRule.COLUMN_FAMILY).build())
        .build();
  }

  @Override
  protected void snapshot(String snapshotName, TableName tableName)
      throws IOException {
    try {
      getAsyncAdmin().snapshot(snapshotName,tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while creating snapshot: "+e.getCause());
    }
    
  }
}
