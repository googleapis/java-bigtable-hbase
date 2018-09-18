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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
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


  @Override
  protected void deleteSnapshot(String snapshotName) throws IOException {
    try {
      getAsyncAdmin().deleteSnapshot(snapshotName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while deleting snapshot: "+e.getCause());
    }
  }

  @Override
  protected boolean tableExists(TableName tableName) throws IOException {
    try {
      return getAsyncAdmin().tableExists(tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while checking table exists: "+e.getCause());
    }
  }

  @Override
  protected void disableTable(TableName tableName) throws IOException {
    try {
      getAsyncAdmin().disableTable(tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while disabling table: "+e.getCause());
    }
  }

  @Override
  protected void cloneSnapshot(String snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    try {
      getAsyncAdmin().cloneSnapshot(snapshotName,tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while creating clone of snapshot: "+e.getCause());
    }
  }

  @Override
  protected void deleteSnapshots(Pattern pattern) throws IOException {
    try {
      getAsyncAdmin().deleteSnapshots(pattern).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while deleting snapshots: "+e.getCause());
    }
  }

  @Override
  protected int listSnapshotsSize(String regEx) throws IOException {
    try {
      Pattern pattern = Pattern.compile(regEx);
      return getAsyncAdmin().listSnapshots(pattern).get().size();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while listing snapshots size: "+e.getCause());
    }
  }

  @Override
  protected int listTableSnapshotsSize(String tableNameRegex,
      String snapshotNameRegex) throws IOException {
    try {
      Pattern tableNamePattern = Pattern.compile(tableNameRegex);
      Pattern snapshotNamePattern = Pattern.compile(snapshotNameRegex);
      return getAsyncAdmin().listTableSnapshots(tableNamePattern, snapshotNamePattern).get().size();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while listing table snapshot size: "+e.getCause());
    }
  }

  @Override
  protected int listSnapshotsSize(Pattern pattern) throws IOException {
    try {
      return getAsyncAdmin().listSnapshots(pattern).get().size();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while listing snapshots size: "+e.getCause());
    }
  }

  @Override
  protected int listTableSnapshotsSize(Pattern tableNamePattern,
      Pattern snapshotNamePattern) throws IOException {
    try {
      return getAsyncAdmin().listTableSnapshots(tableNamePattern, snapshotNamePattern).get().size();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while listing table snapshot size: "+e.getCause());
    }
  }

  @Override
  protected void deleteTable(TableName tableName) throws IOException {
    try {
      getAsyncAdmin().deleteTable(tableName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while deleting table: "+e.getCause());
    }
  }

  @Override
  protected int listTableSnapshotsSize(Pattern tableNamePattern) throws Exception {
	try {
      return getAsyncAdmin().listTableSnapshots(tableNamePattern).get(60, TimeUnit.SECONDS).size();
	} catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error while listing table snapshots: "+e.getCause());
	}
  }
}
