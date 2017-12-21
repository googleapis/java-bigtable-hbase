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
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TestIncrement extends AbstractTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Requirement 6.1 - Increment on or more columns in a given row by given amounts.
   * Requirement 6.4 - Return post-increment value(s)
   */
  @Test
  public void testIncrement() throws IOException {
    // Initialize data
    try (Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName())){
      testIncrement(dataHelper, table);
    }
  }

  public static void testIncrement(DataGenerationHelper dataHelper, Table table)
      throws IOException {
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    long value1 = new Random().nextInt();
    long incr1 = new Random().nextInt();
    byte[] qual2 = dataHelper.randomData("qual-");
    long value2 = new Random().nextInt();
    long incr2 = new Random().nextInt();

    // Put and increment
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, qual1, Bytes.toBytes(value1));
    put.addColumn(COLUMN_FAMILY, qual2, Bytes.toBytes(value2));
    table.put(put);
    Increment increment = new Increment(rowKey);
    increment.addColumn(COLUMN_FAMILY, qual1, incr1);
    increment.addColumn(COLUMN_FAMILY, qual2, incr2);
    Result result = table.increment(increment);
    Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1))));
    Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2))));
    Assert.assertEquals(2, result.size());

    // Double-check values with a Get
    Get get = new Get(rowKey);
    get.readVersions(5);
    result = table.get(get);
    Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1))));
    Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2))));

    // TODO: why isn't this awlways the case in CBT?
    // Assert.assertEquals("Expected four results, two for each column", 4, result.size());
  }

  /**
   * Requirement 6.2 - Specify a timerange (min ts, inclusive + max ts, exclusive). This will create
   * a new value that is an increment of the first value within this range, and will otherwise
   * create a new value.
   * Note: This is pretty weird.  Not sure who would use it, or if we need to support it.
   * Here's the test: 1. Create a cell with three explicit versions: 101, 102, and 103. The values
   * are set to the same so we can track the original versions. 2. Call an increment (+1) with a
   * time range of [101,103) 3. It should have created a fourth version with the current timestamp
   * that incremented version 102.  We can ensure this be looking at the new value (103) and the
   * number of versions. 4. Now increment (+1) with a time range outside of all versions [100000,
   * 200000) 5. It should have create a new version with the current timestamp and value of 1. 6.
   * Check all versions for the cell.  You should have, in descending version order: a. A value of 1
   * with a recent timestamp. b. A value of 103 with a recent timestamp. c. A value of 103 with a
   * timestamp of 103. d. A value of 102 with a timestamp of 102. e. A value of 101 with a timestamp
   * of 101.
   */
  @Test
  @Category(KnownGap.class)
  public void testIncrementWithTimerange() throws IOException {
    // Initialize data
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");

    // Put and increment in range.  Test min is inclusive, max is exclusive.
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, 101L, Bytes.toBytes(101L))
      .addColumn(COLUMN_FAMILY, qual, 102L, Bytes.toBytes(102L))
      .addColumn(COLUMN_FAMILY, qual, 103L, Bytes.toBytes(103L));
    table.put(put);

    // Increment doesn't increment in place.
    Increment increment = new Increment(rowKey);
    increment.setTimeRange(101L, 103L);
    increment.addColumn(COLUMN_FAMILY, qual, 1L);
    Result result = table.increment(increment);
    Assert.assertEquals("Should increment only 1 value", 1, result.size());
    Assert.assertEquals("It should have incremented 102", 102L + 1L,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual))));

    Get get = new Get(rowKey).readVersions(10);
    result = table.get(get);
    Assert.assertEquals("Check there's now a fourth", 4, result.size());

    // Test an increment out of range
    increment = new Increment(rowKey).addColumn(COLUMN_FAMILY, qual, 1L)
      .setTimeRange(100000L, 200000L);
    result = table.increment(increment);
    Assert.assertEquals("Expect single value", 1, result.size());
    Assert.assertTrue("Timestamp should be new",
      result.getColumnLatestCell(COLUMN_FAMILY, qual).getTimestamp() > 1000000000000L);

    // Check values
    get = new Get(rowKey);
    get.readVersions(10);
    result = table.get(get);
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertEquals("Expected five results", 5, cells.size());
    Assert.assertTrue("First should be a default timestamp",
      cells.get(0).getTimestamp() > 1000000000000L);
    Assert.assertEquals("Value should be new", 1L,
      Bytes.toLong(CellUtil.cloneValue(cells.get(0))));
    Assert.assertTrue("Second should also be a default timestamp",
      cells.get(1).getTimestamp() > 1000000000000L);
    Assert.assertEquals("Value should be an increment of 102", 102L + 1L,
      Bytes.toLong(CellUtil.cloneValue(cells.get(1))));
    Assert.assertEquals("Third should be 103", 103L, cells.get(2).getTimestamp());
    Assert.assertEquals("103 should still be 103", 103L,
      Bytes.toLong(CellUtil.cloneValue(cells.get(2))));
    Assert.assertEquals("Fourth should be 102", 102L, cells.get(3).getTimestamp());
    Assert.assertEquals("102 should still be 101", 102L,
      Bytes.toLong(CellUtil.cloneValue(cells.get(3))));
    Assert.assertEquals("Last should be 101", 101L, cells.get(4).getTimestamp());
    Assert.assertEquals("101 should still be 101", 101L,
      Bytes.toLong(CellUtil.cloneValue(cells.get(4))));

    table.close();
  }

  /**
   * Requirement 6.3 - Test that increment uses the current time as the default timestamp for new
   * versions.
   */
  @Test
  public void testDefaultTimestamp() throws IOException {
    long now = System.currentTimeMillis();
    long oneMinute = 60 * 1000;
    long fifteenMinutes = 15 * 60 * 1000;

    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, qualifier, 1L, Bytes.toBytes(100L));
    table.put(put);

    Increment increment = new Increment(rowKey).addColumn(COLUMN_FAMILY, qualifier, 1L);
    table.increment(increment);

    Get get = new Get(rowKey).addColumn(COLUMN_FAMILY, qualifier);
    Result result = table.get(get);
    long timestamp1 = result.getColumnLatestCell(COLUMN_FAMILY, qualifier).getTimestamp();
    Assert.assertTrue(Math.abs(timestamp1 - now) < fifteenMinutes);

    try {
      TimeUnit.MILLISECONDS.sleep(10);  // Make sure the clock has a chance to move
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("sleep was interrupted", e);
    }
    table.increment(increment);
    result = table.get(get);
    long timestamp2 = result.getColumnLatestCell(COLUMN_FAMILY, qualifier).getTimestamp();
    Assert.assertTrue("Time increases strictly", timestamp2 > timestamp1);
    Assert.assertTrue("Time doesn't move too fast", (timestamp2 - timestamp1) < oneMinute);
    table.close();
  }

  /**
   * Requirement 6.6 - Increment should fail on non-64-bit values, and succeed on any 64-bit value.
   */
  @Test
  @Category(KnownGap.class)
  public void testFailOnIncrementInt() throws IOException {
    // Initialize
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    int value = new Random().nextInt();
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, Bytes.toBytes(value));
    table.put(put);

    // Increment
    Increment increment = new Increment(rowKey).addColumn(COLUMN_FAMILY, qual, 1L);
    expectedException.expect(DoNotRetryIOException.class);

    expectedException.expectMessage(
        anyOf(
            containsString("Attempted to increment field that isn't 64 bits wide"),
            containsString("Field is not a long")
        )
    );
    table.increment(increment);
  }

  /**
   * Requirement 6.6
   */
  @Test
  @Category(KnownGap.class)
  public void testFailOnIncrementString() throws IOException {
    // Initialize
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
    table.put(put);

    // Increment
    Increment increment = new Increment(rowKey).addColumn(COLUMN_FAMILY, qual, 1L);
    expectedException.expect(DoNotRetryIOException.class);
    expectedException.expectMessage(
        anyOf(
            containsString("Attempted to increment field that isn't 64 bits wide"),
            containsString(" Field is not a long")
        )
    );
    table.increment(increment);
  }

  /**
   * Requirement 6.6
   */
  @Test
  public void testIncrementEightBytes() throws IOException {
    // Initialize
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    byte[] value = new byte[8];
    new Random().nextBytes(value);
    ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
    buffer.put(value);
    buffer.flip();
    long equivalentValue = buffer.getLong();

    // Put the bytes in
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
    table.put(put);

    // Increment
    Increment increment = new Increment(rowKey).addColumn(COLUMN_FAMILY, qual, 1L);
    Result result = table.increment(increment);
    Assert.assertEquals("Should have incremented the bytes like a long", equivalentValue + 1L,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual))));
  }

  @Test
  @Category(KnownGap.class)
  public void testIncrementWithMaxVersions() throws IOException {
    // Initialize data
    byte[] incrementFamily = Bytes.toBytes("i");
    byte[] incrementTable = Bytes.toBytes("increment_table");

    TableName incrementTableName = TableName.valueOf(incrementTable);
    Admin admin = getConnection().getAdmin();
    HTableDescriptor tableDescriptor = new HTableDescriptor(incrementTableName);
    tableDescriptor.addFamily(new HColumnDescriptor(incrementFamily).setMaxVersions(1));
    admin.createTable(tableDescriptor);

    Table table = getConnection().getTable(incrementTableName);

    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    long value1 = new Random().nextInt();
    long incr1 = new Random().nextInt();
    byte[] qual2 = dataHelper.randomData("qual-");
    long value2 = new Random().nextInt();
    long incr2 = new Random().nextInt();

    // Put and increment
    Put put = new Put(rowKey);
    put.addColumn(incrementFamily, qual1, Bytes.toBytes(value1));
    put.addColumn(incrementFamily, qual2, Bytes.toBytes(value2));
    table.put(put);
    Increment increment = new Increment(rowKey);
    increment.addColumn(incrementFamily, qual1, incr1);
    increment.addColumn(incrementFamily, qual2, incr2);
    Result result = table.increment(increment);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
        Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(incrementFamily, qual1))));
    Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
        Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(incrementFamily, qual2))));

    // Double-check values with a Get
    Get get = new Get(rowKey);
    get.readVersions(5);
    result = table.get(get);
    Assert.assertEquals("Expected two results, only the latest version for each column",
        2, result.size());
    Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
        Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(incrementFamily, qual1))));
    Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
        Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(incrementFamily, qual2))));

    table.close();
  }

}
