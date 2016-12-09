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
  private final String snapshotName = tableName.getNameAsString() + "_snp";
  private final TableName clonedTableName =
      TableName.valueOf(tableName.getNameAsString() + "_clone");

   @After
   public void delete() throws IOException {
     Admin admin = getConnection().getAdmin();
     admin.disableTable(tableName);
     admin.deleteTable(tableName);
     admin.disableTable(clonedTableName);
     admin.deleteTable(clonedTableName);
     admin.close();
   }

  @Test
  @Category(KnownGap.class)
  public void testSnapshot() throws IOException {
    Admin admin = getConnection().getAdmin();
    admin.createTable(
      new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(COLUMN_FAMILY)));

    Map<String, Long> values = createAndPopulateTable();
    Assert.assertEquals(0, admin.listSnapshots(snapshotName).size());
    admin.snapshot(snapshotName, tableName);
    Assert.assertEquals(1, admin.listSnapshots(snapshotName).size());
    admin.cloneSnapshot(snapshotName, clonedTableName);
    validateClone(values);
    Assert.assertEquals(1, admin.listSnapshots(snapshotName).size());
    admin.deleteSnapshot(snapshotName);
    Assert.assertEquals(0, admin.listSnapshots(snapshotName).size());
    admin.close();
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
