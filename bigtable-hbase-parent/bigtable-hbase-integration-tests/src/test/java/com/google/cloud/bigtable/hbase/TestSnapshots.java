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
 */package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.IntegrationTests.COLUMN_FAMILY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;


public class TestSnapshots extends AbstractTest {

  final byte[] QUALIFIER = dataHelper.randomData("TestSingleColumnValueFilter");

  private final TableName tableName = IntegrationTests.newTestTableName();
  // The maximum size of a table id or snapshot id is 50. newTestTableName().size() can approach 50.
  // Make sure that the snapshot name and cloned table are within the 50 character limit
  private final String snapshotName = tableName.getNameAsString().substring(40) + "_snp";
  private final TableName clonedTableName =
      TableName.valueOf(tableName.getNameAsString().substring(40) + "_clone");

  @After
  public void cleanup() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      delete(admin, tableName);
      delete(admin, clonedTableName);
      admin.deleteSnapshot(snapshotName);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void delete(Admin admin, TableName tableName) throws IOException {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  @Test
  @Category(KnownGap.class)
  public void testSnapshot() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      admin.createTable(
        new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(COLUMN_FAMILY)));

      Map<String, Long> values = createAndPopulateTable();
      checkSnapshotCount(admin, 0);
      admin.snapshot(snapshotName, tableName);
      checkSnapshotCount(admin, 1);
      admin.cloneSnapshot(snapshotName, clonedTableName);
      validateClone(values);
      checkSnapshotCount(admin, 1);
      admin.deleteSnapshot(snapshotName);
      checkSnapshotCount(admin, 0);
    }
  }

  private void checkSnapshotCount(Admin admin, int count) throws IOException {
    Assert.assertEquals(count, admin.listSnapshots(snapshotName).size());
  }

  /**
   * Create a test table, and add a small number of rows to the table.
   *
   * @return A Map of the data that was added to the original table.
   * @throws IOException
   */
  private Map<String, Long> createAndPopulateTable() throws IOException {
    Map<String, Long> values = new HashMap<>();
    try (Table table = getConnection().getTable(tableName)) {
      values.clear();
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

  protected void validateClone(Map<String, Long> values) throws IOException {
    try (Table clonedTable = getConnection().getTable(clonedTableName);
        ResultScanner scanner = clonedTable.getScanner(new Scan())){
      for(Result result : scanner) {
        String row = Bytes.toString(result.getRow());
        Long expected = values.get(row);
        Long found = Bytes.toLong(result.getValue(COLUMN_FAMILY, QUALIFIER));
        Assert.assertEquals("row " + row + " not equal", expected, found);
        values.remove(row);
      }
    }
    Assert.assertTrue("There were missing keys.", values.isEmpty());
  }

}
