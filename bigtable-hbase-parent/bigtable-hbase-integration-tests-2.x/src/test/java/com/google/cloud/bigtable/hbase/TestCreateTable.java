/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestCreateTable extends AbstractTest {


  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Requirement 1.8 - Table names must match [_a-zA-Z0-9][-_.a-zA-Z0-9]*
   */
  @Test(timeout = 1000l * 60 * 4)
  public void testTableNames() throws IOException {
    String shouldTest = System.getProperty("bigtable.test.create.table", "true");
    if (!"true".equals(shouldTest)) {
      return;
    }
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

    final Admin admin = getConnection().getAdmin();

    for (String badName : badNames) {
      boolean failed = false;
      try {
        admin.createTable(new HTableDescriptor(TableName.valueOf(badName))
            .addFamily(new HColumnDescriptor(COLUMN_FAMILY)));
      } catch (Exception ex) {
        //TODO verify - added RuntimeException check as RandomStringUtils seems to be generating a string server side doesn't like 
        failed = true;
      }
      Assert.assertTrue("Should fail as table name: '" + badName + "'", failed);
    }

    final TableName[] tableNames = admin.listTableNames();

    List<ListenableFuture<Void>> futures = new ArrayList<>();
    ListeningExecutorService es = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    for(final String goodName : goodNames) {
      futures.add(es.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          createTable(admin, goodName, tableNames);
          return null;
        }
      }));
    }
    try {
      try {
        Futures.allAsList(futures).get(3, TimeUnit.MINUTES);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } finally {
      es.shutdownNow();
    }
  }

  private void createTable(Admin admin, String goodName, TableName[] tableNames) throws IOException {
    logger.info("Try create table for: %s", goodName);
    TableName tableName = TableName.valueOf(goodName);
    HTableDescriptor descriptor = new HTableDescriptor(tableName)
        .addFamily(new HColumnDescriptor(COLUMN_FAMILY));

    try {
      if (contains(tableNames, tableName)) {
        logger.warn("Not creating the table since it exists: %s", tableName);
      } else {
        logger.info("Do create table for: %s", goodName);
        admin.createTable(descriptor);
      }
    } finally {
      try {
        admin.disableTable(tableName);
        logger.info("Do delete table for: %s", goodName);
        admin.deleteTable(tableName);
      } catch (Throwable t) {
        // logger the error and ignore it.
        logger.warn("Error cleaning up the table", t);
      }
    }
  }

  private boolean contains(TableName[] tableNames, TableName tableNameTocheck) {
    for (TableName tableName : tableNames) {
      if (tableName.equals(tableNameTocheck)) {
        return true;
      }
    }
    return false;
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
        // logger the error and ignore it.
        logger.warn("Error cleaning up the table", t);
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
    } catch (IllegalArgumentException e) {
    }
    try {
      admin.createTable(descriptor, endKey, startKey, 5);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    } finally {
      admin.close();
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
        // logger the error and ignore it.
        logger.warn("Error cleaning up the table", t);
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
        admin.close();
      } catch (Throwable t) {
        // logger the error and ignore it.
        logger.warn("Error cleaning up the table", t);
      }
    }
  }


  @Test
  public void testAlreadyExists() throws IOException {
    thrown.expect(TableExistsException.class);
    Admin admin = getConnection().getAdmin();
    TableName tableName = sharedTestEnv.getDefaultTableName();
    admin.createTable(admin.getTableDescriptor(tableName));
  }
}
