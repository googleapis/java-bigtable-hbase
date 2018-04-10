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

import org.apache.hadoop.hbase.HRegionLocation;
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
public abstract class AbstractTestTruncateTable extends AbstractTest {

  @Test
  public void testTruncate() throws Exception {
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
    try(Admin admin = getConnection().getAdmin()) {
      createTable(tableName, splits);
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

  protected abstract void createTable(TableName tableName, byte[][] splits) throws IOException;

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
    // This logic was borrowed from AbstractTestCreateTable.testFiveRegionSplit()
    List<HRegionLocation> regions = null;
    try (RegionLocator locator = getConnection().getRegionLocator(tableName)) {
      regions = locator.getAllRegionLocations();
    }

    // The number of regions should be the number of splits + 1.
    Assert.assertEquals(splits.length + 1, regions.size());
    AbstractTestCreateTable.assertSplitsAndRegionsMatch(splits, regions);
  }
  
  protected abstract void doTruncate(TableName tableName) throws Exception;
}
