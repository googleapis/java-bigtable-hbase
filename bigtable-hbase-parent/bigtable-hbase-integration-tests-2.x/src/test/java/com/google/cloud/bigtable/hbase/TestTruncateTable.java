/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestTruncateTable extends AbstractTest {

  @Test
  public void testTruncate() throws IOException {
    TableName newTestTableName = sharedTestEnv.newTestTableName();
    sharedTestEnv.createTable(newTestTableName);
    try {
      testTruncate(newTestTableName);
    } finally {
      deleteTable(newTestTableName);
    }
  }

  private void testTruncate(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin();
        Table table = getConnection().getTable(tableName)) {
      byte[] rowKey = dataHelper.randomData("testrow-");
      byte[] qual = dataHelper.randomData("qual-");
      byte[] value = dataHelper.randomData("value-");
      Put put = new Put(rowKey);
      put.addColumn(COLUMN_FAMILY, qual, 1L, value);
      put.addColumn(COLUMN_FAMILY, qual, 2L, value);
      table.put(put);
      assertTrue(table.exists(new Get(rowKey)));
      admin.disableTable(tableName);
      admin.truncateTable(tableName, false);
      assertTrue(admin.tableExists(tableName));
      assertFalse(table.exists(new Get(rowKey)));
    }
  }

  @Test
  public void testTruncateWithSplits() throws IOException {
    byte[][] splits = new byte[][] {
        Bytes.toBytes("AAA"),
        Bytes.toBytes("BBB"),
        Bytes.toBytes("CCC"),
    };

    TableName tableName = sharedTestEnv.newTestTableName();
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

    try(Admin admin = getConnection().getAdmin()) {
      admin.createTable(descriptor, splits);
      testSplits(tableName, splits);
      testTruncate(tableName);
      if (sharedTestEnv.isBigtable()) {
        // The splits are only preserved by Cloud Bigtable, and not by HBase.
        testSplits(tableName, splits);
      }
    } finally {
      deleteTable(tableName);
    }
  }

  private void deleteTable(TableName tableName) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      if (admin.tableExists(tableName)) {
        if (admin.isTableEnabled(tableName)) {
          // The table may or may not be enabled, depending on the success of truncate table.
          admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
      }
    }
  }

  private void testSplits(TableName tableName, byte[][] splits) throws IOException {
    // This logic was borrowed from TestCreateTable.testFiveRegionSplit()
    List<HRegionLocation> regions = null;
    try (RegionLocator locator = getConnection().getRegionLocator(tableName)) {
      regions = locator.getAllRegionLocations();
    }

    // The number of regions should be the number of splits + 1.
    Assert.assertEquals(splits.length + 1, regions.size());
    for (int i = 0; i < regions.size(); i++) {
      HRegionLocation region = regions.get(i);
      String start_key = Bytes.toString(region.getRegionInfo().getStartKey());
      String end_key = Bytes.toString(region.getRegionInfo().getEndKey());
      // Check start & end keys vs what was requested.
      if (i == 0) {
        // First split: the end key must be the first element of splits.
        Assert.assertEquals(Bytes.toString(splits[0]), end_key);
      } else if (i == regions.size() - 1) {
        // Last split: the start key must be the last element of splits.
        Assert.assertEquals(Bytes.toString(splits[splits.length - 1]), start_key);
      } else {
        // For all others: start_key = splits[i - i], end_key = splits[i].
        Assert.assertEquals(Bytes.toString(splits[i - 1]), start_key);
        Assert.assertEquals(Bytes.toString(splits[i]), end_key);
      }
    }
  }
}
