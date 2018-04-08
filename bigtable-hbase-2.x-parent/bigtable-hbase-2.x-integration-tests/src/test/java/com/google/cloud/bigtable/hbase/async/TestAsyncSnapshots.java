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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.hbase.AbstractTestSnapshot;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

@RunWith(JUnit4.class)
public class TestAsyncSnapshots extends AbstractTestSnapshot {

  @Before
  public void setup() throws Exception {
    descriptor = new HTableDescriptor(anotherTableName);
    descriptor.addFamily(new HColumnDescriptor(SharedTestEnvRule.COLUMN_FAMILY));
    createTable(anotherTableName);
  }

  @After
  public void tearDown() throws IOException, InterruptedException, ExecutionException {
    getAsyncAdmin().disableTable(anotherTableName).get();
    getAsyncAdmin().deleteTable(anotherTableName).get();
  }
  
  private AsyncAdmin getAsyncAdmin() throws InterruptedException, ExecutionException {
    return AbstractAsyncTest.getAsyncConnection().getAdmin();
  }
  
  @Override
  protected void createTable(TableName tableName) throws IOException {
    try{
      getAsyncAdmin().createTable(createDescriptor(tableName)).get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }  
  
  private TableDescriptor createDescriptor(TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(SharedTestEnvRule.COLUMN_FAMILY).build())
        .build();
  }

  @Test
  public void testAsyncSnapshot() throws IOException {
    if (sharedTestEnv.isBigtable() && !Boolean.getBoolean("perform.snapshot.test")) {
      return;
    }
    
    try {
    	AsyncAdmin asyncAdmin = getAsyncAdmin();    
    	asyncAdmin.createTable(
        new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(SharedTestEnvRule.COLUMN_FAMILY)));

      Map<String, Long> values = createAndPopulateTable(tableName);
      checkSnapshotCount(asyncAdmin, 0);
      asyncAdmin.snapshot(snapshotName, tableName);
      checkSnapshotCount(asyncAdmin, 1);
      asyncAdmin.cloneSnapshot(snapshotName, clonedTableName);
      validateClone(values);
      checkSnapshotCount(asyncAdmin, 1);
      asyncAdmin.deleteSnapshot(snapshotName);
      checkSnapshotCount(asyncAdmin, 0);
      asyncAdmin.restoreSnapshot(snapshotName);
      checkSnapshotCount(asyncAdmin, 1);
      
    } catch (Exception e) {
        e.printStackTrace();
    }
  }
  
private void checkSnapshotCount(AsyncAdmin asyncAdmin, int count) {
  asyncAdmin.listSnapshots().thenApply(r->{
     logger.info("Count from CheckSnapshot :: ", count);
     Assert.assertEquals(count,r.size());
     return null;
   });
 }
}
