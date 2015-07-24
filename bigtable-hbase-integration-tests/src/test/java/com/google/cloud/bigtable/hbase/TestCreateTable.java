/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import static com.google.cloud.bigtable.hbase.IntegrationTests.COLUMN_FAMILY;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class TestCreateTable extends AbstractTest {

  private static final Logger LOG = new Logger(TestCreateTable.class);

  /**
   * Requirement 1.8 - Table names must match [_a-zA-Z0-9][-_.a-zA-Z0-9]*
   */
  @Test
  public void testTableNames() throws IOException {
    String[] goodNames = {
        "a",
        "1",
        "_", // Really?  Yuck.
        "_x",
        "a-._5x",
        "_a-._5x",
        // TODO(sduskis): Join the last 2 strings once the Bigtable backend supports table names
        // longer than 50 characters.
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi",
        "jklmnopqrstuvwxyz1234567890_-."
    };
    String[] badNames = {
        "-x",
        ".x",
        "a!",
        "a@",
        "a#",
        "a$",
        "a%",
        "a^",
        "a&",
        "a*",
        "a(",
        "a+",
        "a=",
        "a~",
        "a`",
        "a{",
        "a[",
        "a|",
        "a\\",
        "a/",
        "a<",
        "a,",
        "a?",
        "a" + RandomStringUtils.random(10, false, false)
    };

    Admin admin = getConnection().getAdmin();

    for (String badName : badNames) {
      boolean failed = false;
      try {
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(badName));
        descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
        admin.createTable(descriptor);
      } catch (IllegalArgumentException e) {
        failed = true;
      }
      Assert.assertTrue("Should fail as table name: '" + badName + "'", failed);
    }

    for(String goodName : goodNames) {
      TableName tableName = TableName.valueOf(goodName);
      HTableDescriptor descriptor = new HTableDescriptor(tableName);
      descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

      try {
        // TODO(kevinsi): Currently the integration test is shared. We need to make a unique cluster
        // per test environment for a cleaner testing scenario.
        if (admin.tableExists(tableName)) {
          LOG.warn("Not creating the table since it exists: %s", tableName);
        } else {
          admin.createTable(descriptor);
        }
      } finally {
        try {
          admin.disableTable(tableName);
          admin.deleteTable(tableName);
        } catch (Throwable t) {
          // Log the error and ignore it.
          LOG.warn("Error cleaning up the table", t);
        }
      }
    }
  }
  
  @Test
  public void testSplitKeys() throws IOException {
    byte[][] splits = new byte[][] {
        Bytes.toBytes("AAA"),
        Bytes.toBytes("BBB"),
        Bytes.toBytes("CCC"),
    };
    
    Admin admin = getConnection().getAdmin();
    
    TableName tableName = TableName.valueOf("TestTable" +
        UUID.randomUUID().toString());
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    
    try {
      admin.createTable(descriptor, splits);
      List<HRegionLocation> regions = null;
      try (RegionLocator locator =
          getConnection().getRegionLocator(tableName)) {
         regions = locator.getAllRegionLocations();
      }
      // The number of regions should be the number of splits + 1.
      Assert.assertEquals(splits.length + 1, regions.size());
      for (int i = 0; i < regions.size(); i++) {
        HRegionLocation region = regions.get(i);
        String start_key = Bytes.toString(
            region.getRegionInfo().getStartKey());
        String end_key = Bytes.toString(region.getRegionInfo().getEndKey());
        // Check start & end keys vs what was requested.
        if (i == 0) {
          // First split: the end key must be the first element of splits.
          Assert.assertEquals(Bytes.toString(splits[0]), end_key);
        } else if (i == regions.size() - 1) {
          // Last split: the start key must be the last element of splits.
          Assert.assertEquals(Bytes.toString(splits[splits.length - 1]),
              start_key);
        } else {
          // For all others: start_key = splits[i-i], end_key = splits[i].
          Assert.assertEquals(Bytes.toString(splits[i-1]), start_key);
          Assert.assertEquals(Bytes.toString(splits[i]), end_key);
        }
      }
    } finally {
      try {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } catch (Throwable t) {
        // Log the error and ignore it.
        LOG.warn("Error cleaning up the table", t);
      }
    }
  }

  @Test
  public void testEvenSplitKeysFailures() throws IOException {
    Admin admin = getConnection().getAdmin();
    TableName tableName = TableName.valueOf("TestTable" +
        UUID.randomUUID().toString());
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    byte[] startKey = Bytes.toBytes("AAA");
    byte[] endKey = Bytes.toBytes("ZZZ");

    try {
      admin.createTable(descriptor, startKey, endKey, 2);
      Assert.fail();
    }
    catch (IllegalArgumentException e) {
    }
    try {
      admin.createTable(descriptor, endKey, startKey, 5);
      Assert.fail();
    }
    catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testThreeRegionSplit() throws IOException {

    Admin admin = getConnection().getAdmin();

    TableName tableName = TableName.valueOf("TestTable" +
        UUID.randomUUID().toString());
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    byte[] startKey = Bytes.toBytes("AAA");
    byte[] endKey = Bytes.toBytes("ZZZ");

    try {
      admin.createTable(descriptor, startKey, endKey, 3);
      List<HRegionLocation> regions = null;
      try (RegionLocator locator =
          getConnection().getRegionLocator(tableName)) {
         regions = locator.getAllRegionLocations();
      }
      Assert.assertEquals(3, regions.size());
      for (int i = 0; i < regions.size(); i++) {
        HRegionLocation region = regions.get(i);
        String start_key = Bytes.toString(
            region.getRegionInfo().getStartKey());
        String end_key = Bytes.toString(region.getRegionInfo().getEndKey());

        // Check start & end keys vs what was requested.
        if (i == 0) {
          Assert.assertEquals(Bytes.toString(startKey), end_key);
        } else if (i == 1) {
          Assert.assertEquals(Bytes.toString(startKey), start_key);
          Assert.assertEquals(Bytes.toString(endKey), end_key);
        } else {
          Assert.assertEquals(Bytes.toString(endKey), start_key);
        }
      }
    } finally {
      try {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } catch (Throwable t) {
        // Log the error and ignore it.
        LOG.warn("Error cleaning up the table", t);
      }
    }
  }
  @Test
  public void testFiveRegionSplit() throws IOException {

    Admin admin = getConnection().getAdmin();

    TableName tableName = TableName.valueOf("TestTable" +
        UUID.randomUUID().toString());
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    byte[] startKey = Bytes.toBytes("AAA");
    byte[] endKey = Bytes.toBytes("ZZZ");
    byte[][] splitKeys = Bytes.split(startKey, endKey, 2);

    try {
      admin.createTable(descriptor, startKey, endKey, 5);
      List<HRegionLocation> regions = null;
      try (RegionLocator locator =
          getConnection().getRegionLocator(tableName)) {
         regions = locator.getAllRegionLocations();
      }
      // The number of regions should be the number of splits + 1.
      Assert.assertEquals(5, regions.size());
      for (int i = 0; i < regions.size(); i++) {
        HRegionLocation region = regions.get(i);
        String start_key = Bytes.toString(
            region.getRegionInfo().getStartKey());
        String end_key = Bytes.toString(region.getRegionInfo().getEndKey());

        // Check start & end keys vs what was requested.
        if (i == 0) {
          // First split: the end key must be the first element of splits.
          Assert.assertEquals(Bytes.toString(startKey), end_key);
        } else if (i == regions.size() - 1) {
          // Last split: the start key must be the last element of splits.
          Assert.assertEquals(Bytes.toString(endKey), start_key);
        } else {
          // For all others: start_key = splits[i-i], end_key = splits[i].
          Assert.assertEquals(Bytes.toString(splitKeys[i-1]), start_key);
          Assert.assertEquals(Bytes.toString(splitKeys[i]), end_key);
        }
      }
    } finally {
      try {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } catch (Throwable t) {
        // Log the error and ignore it.
        LOG.warn("Error cleaning up the table", t);
      }
    }
  }   
}
