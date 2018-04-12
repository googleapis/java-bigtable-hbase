/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;

public class TestSnapshots extends AbstractTestSnapshot {

  @Override
  protected void createTable(TableName tableName) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      HTableDescriptor descriptor = new HTableDescriptor(tableName);
      descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
      admin.createTable(descriptor);
    }
    
  }

  @Override
  protected void snapshot(String snapshotName, TableName tableName)
      throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      admin.snapshot(snapshotName, tableName);
    }
  }

  @Override
  protected int listSnapshotsSize(String regEx) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      return admin.listSnapshots(regEx).size();
    }
  }

  @Override
  protected void deleteSnapshot(String snapshotName) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      admin.deleteSnapshot(snapshotName);
    }
    
  }

  @Override
  protected boolean tableExists(TableName tableName) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      return admin.tableExists(tableName);
    }
  }

  @Override
  protected void disableTable(TableName tableName) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      admin.disableTable(tableName);
    }
  }

  @Override
  protected void cloneSnapshot(String snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    try(Admin admin = getConnection().getAdmin()) {
      admin.cloneSnapshot(snapshotName, tableName);
    }
  }

  @Override
  protected void deleteSnapshots(Pattern pattern) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      admin.deleteSnapshots(pattern);
    }
  }

  @Override
  protected int listTableSnapshotsSize(String tableNameRegex,
      String snapshotNameRegex) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      return admin.listTableSnapshots(tableNameRegex, snapshotNameRegex).size();
    }
  }

  @Override
  protected int listSnapshotsSize(Pattern pattern) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      return admin.listSnapshots(pattern).size();
    }
  }

  @Override
  protected int listTableSnapshotsSize(Pattern tableNamePattern,
      Pattern snapshotNamePattern) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      return admin.listTableSnapshots(tableNamePattern, snapshotNamePattern).size();
    }
  }

  @Override
  protected void deleteTable(TableName tableName) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      admin.deleteTable(tableName);
    }
  }
}
