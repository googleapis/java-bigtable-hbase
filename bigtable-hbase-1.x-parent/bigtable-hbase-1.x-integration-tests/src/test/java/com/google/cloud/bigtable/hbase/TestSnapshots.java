/*
 * Copyright 2016 Google LLC
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
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(KnownEmulatorGap.class)
public class TestSnapshots extends AbstractTestSnapshot {

  @Before
  public void setUp() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      values = createAndPopulateTable(tableName);
    }
  }

  @Test
  public void listSnapshots() throws IOException, InterruptedException {
    String snapshot1 = newSnapshotId();
    snapshot(snapshot1, tableName);

    String snapshot2 = newSnapshotId();
    snapshot(snapshot2, tableName);

    try (Admin admin = getConnection().getAdmin()) {
      List<SnapshotDescription> snapshotDescriptions = admin.listSnapshots();
      List<String> descStrings = new ArrayList<>();
      for (SnapshotDescription snapshotDescription : snapshotDescriptions) {
        descStrings.add(snapshotDescription.getName());
      }
      assertThat(descStrings, hasItems(snapshot1, snapshot2));
    } finally {
      deleteSnapshot(snapshot1);
      deleteSnapshot(snapshot2);
    }
  }

  @Override
  protected void snapshot(String snapshotName, TableName tableName)
      throws IOException, InterruptedException {
    try (Admin admin = getConnection().getAdmin()) {
      admin.snapshot(snapshotName, tableName);
    }
  }

  @Override
  protected int listSnapshotsSize(String regEx) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      return admin.listSnapshots(regEx).size();
    }
  }

  @Override
  protected void deleteSnapshot(String snapshotName) throws IOException, InterruptedException {
    try (Admin admin = getConnection().getAdmin()) {
      admin.deleteSnapshot(snapshotName);
    }
  }

  @Override
  protected boolean tableExists(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      return admin.tableExists(tableName);
    }
  }

  @Override
  protected void disableTable(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      admin.disableTable(tableName);
    }
  }

  @Override
  protected void cloneSnapshot(String snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    try (Admin admin = getConnection().getAdmin()) {
      admin.cloneSnapshot(snapshotName, tableName);
    }
  }

  @Override
  protected int listSnapshotsSize(Pattern pattern) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      return admin.listSnapshots(pattern).size();
    }
  }

  @Override
  protected void deleteTable(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      admin.deleteTable(tableName);
    }
  }

  protected Map<String, Long> createAndPopulateTable(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      HTableDescriptor descriptor = new HTableDescriptor(tableName);
      descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
      admin.createTable(descriptor);

      Map<String, Long> values = new HashMap<>();
      try (Table table = getConnection().getTable(tableName)) {
        List<Put> puts = new ArrayList<>();
        for (long i = 0; i < 10; i++) {
          final UUID rowKey = UUID.randomUUID();
          byte[] row = Bytes.toBytes(rowKey.toString());
          values.put(rowKey.toString(), i);
          puts.add(new Put(row).addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes(i)));
        }
        table.put(puts);
      }
      return values;
    }
  }
}
