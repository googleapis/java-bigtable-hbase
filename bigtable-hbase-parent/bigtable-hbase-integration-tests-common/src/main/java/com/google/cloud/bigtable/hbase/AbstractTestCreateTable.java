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
package com.google.cloud.bigtable.hbase;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

@SuppressWarnings("deprecation")
public abstract class AbstractTestCreateTable extends AbstractTest {

  /**
   * This variable ensures that <pre>testTableNames()</pre> only runs once even if there are many subclasses
   * of AbstractTestCreateTable.  HBase 2 will likely have 3+ subclasses, but <pre>testTableNames()</pre>
   * should still run only once.  <pre>testTableNames()</pre> is more about a test of Cloud Bigtable
   * than any specific client implementation.
   */
  private static final AtomicInteger testTableNames_Counter = new AtomicInteger();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreate()throws Exception {
    TableName tableName = sharedTestEnv.newTestTableName();
    createTable(tableName);
    Assert.assertTrue(getConnection().getAdmin().tableExists(tableName));
    deleteTable(tableName);
    Assert.assertFalse(getConnection().getAdmin().tableExists(tableName));
  }

  /**
   * Requirement 1.8 - Table names must match [_a-zA-Z0-9][-_.a-zA-Z0-9]*
   */
  @Test(timeout = 1000l * 60 * 4)
  public void testTableNames()throws Exception {
    String shouldTest = System.getProperty("bigtable.test.create.table", "true");
    if (!"true".equals(shouldTest) || testTableNames_Counter.incrementAndGet() > 1) {
      return;
    }
    // more than one subclass can run at the same time.  Ensure that this method is serial.
    String[] goodNames = { "a", "1", "_", // Really?  Yuck.
        "_x", "a-._5x", "_a-._5x",
        // TODO(sduskis): Join the last 2 strings once the Bigtable backend supports table names
        // longer than 50 characters.
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi", "jklmnopqrstuvwxyz1234567890_-." };
    String[] badNames =
        { "-x", ".x", "a!", "a@", "a#", "a$", "a%", "a^", "a&", "a*", "a(", "a+", "a=", "a~", "a`",
            "a{", "a[", "a|", "a\\", "a/", "a<", "a,", "a?",
            // TODO(issue #1785): this always fails in Cloud Bigtable, but fails intermittently in
            // HBase.  We need to find the difference, document it, and create a better test.
            //            "a" + RandomStringUtils.random(10, false, false)
        };

    for (String badName : badNames) {
      boolean failed = false;
      try {
        createTable(TableName.valueOf(badName));
      } catch (Exception ex) {
        //TODO verify - added RuntimeException check as RandomStringUtils seems to be generating a string server side doesn't like
        failed = true;
      }
      Assert.assertTrue("Should fail as table name: '" + badName + "'", failed);
    }

    final TableName[] tableNames = getConnection().getAdmin().listTableNames();

    List<ListenableFuture<Void>> futures = new ArrayList<>();
    ListeningExecutorService es = MoreExecutors.listeningDecorator(sharedTestEnv.getExecutor());
    for (final String goodName : goodNames) {
      futures.add(es.submit(new Callable<Void>() {
        @Override public Void call()throws Exception {
          createTable(goodName, tableNames);
          return null;
        }
      }));
    }
    try {
      Futures.allAsList(futures).get(3, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createTable(String goodName, TableName[] tableNames)throws Exception {
    logger.info("Try create table for: %s", goodName);
    TableName tableName = TableName.valueOf(goodName);

    try {
      if (contains(tableNames, tableName)) {
        logger.warn("Not creating the table since it exists: %s", tableName);
      } else {
        logger.info("Do create table for: %s", goodName);
        createTable(tableName);
      }
    } finally {
      deleteTable(tableName);
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
  public void testSplitKeys()throws Exception {
    byte[][] splits = new byte[][] {
        Bytes.toBytes("AAA"),
        Bytes.toBytes("BBB"),
        Bytes.toBytes("CCC"),
    };

    TableName tableName = sharedTestEnv.newTestTableName();
    try {
      createTable(tableName, splits);
      List<HRegionLocation> regions = null;
      try (RegionLocator locator = getConnection().getRegionLocator(tableName)) {
         regions = locator.getAllRegionLocations();
      }
      // The number of regions should be the number of splits + 1.
      Assert.assertEquals(splits.length + 1, regions.size());
      assertSplitsAndRegionsMatch(splits, regions);
    } finally {
      deleteTable(tableName);
    }
  }

  /**
   * Ensures that the requested splits and the actual list of {@link HRegionLocation} are the same.
   */
  public static void assertSplitsAndRegionsMatch(byte[][] splits, List<HRegionLocation> regions) {
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
  }

  @Test
  public void testEvenSplitKeysFailures()throws Exception {
    TableName tableName = sharedTestEnv.newTestTableName();
    byte[] startKey = Bytes.toBytes("AAA");
    byte[] endKey = Bytes.toBytes("ZZZ");

    try {
      createTable(tableName, startKey, endKey, 2);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
    try {
      createTable(tableName, endKey, startKey, 5);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testThreeRegionSplit()throws Exception {

    TableName tableName = sharedTestEnv.newTestTableName();

    byte[] startKey = Bytes.toBytes("AAA");
    byte[] endKey = Bytes.toBytes("ZZZ");

    try {
      createTable(tableName, startKey, endKey, 3);
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
      deleteTable(tableName);
    }
  }

  @Test
  public void testFiveRegionSplit()throws Exception {
    TableName tableName = sharedTestEnv.newTestTableName();
    byte[] startKey = Bytes.toBytes("AAA");
    byte[] endKey = Bytes.toBytes("ZZZ");
    byte[][] splitKeys = Bytes.split(startKey, endKey, 2);

    try {
      createTable(tableName, startKey, endKey, 5);
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
      deleteTable(tableName);
    }
  }

  @Test
  public void testAlreadyExists()throws Exception {
    thrown.expect(TableExistsException.class);
    createTable(sharedTestEnv.getDefaultTableName());
  }

  protected void deleteTable(TableName tableName) {
    try (Admin admin = getConnection().getAdmin()) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    } catch (Throwable t) {
      // logger the error and ignore it.
      logger.warn("Error cleaning up the table", t);
    }
  }

  protected abstract void createTable(TableName name) throws Exception;
  protected abstract void createTable(TableName name, byte[] start, byte[] end, int splitCount)
      throws Exception;
  protected abstract void createTable(TableName name, byte[][] ranges)
      throws Exception;

}
