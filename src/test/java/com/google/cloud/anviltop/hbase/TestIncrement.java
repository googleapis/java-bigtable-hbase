/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class TestIncrement extends AbstractTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Requirement 6.1 - Increment on or more columns in a given row by given amounts.
   */
  @Test
  public void testIncrement() throws IOException {
    // Initialize data
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    long value1 = new Random().nextInt();
    long incr1 = new Random().nextInt();
    byte[] qual2 = dataHelper.randomData("qual-");
    long value2 = new Random().nextInt();
    long incr2 = new Random().nextInt();

    // Put and increment
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual1, Bytes.toBytes(value1));
    put.add(COLUMN_FAMILY, qual2, Bytes.toBytes(value2));
    table.put(put);
    Increment increment = new Increment(rowKey);
    increment.addColumn(COLUMN_FAMILY, qual1, incr1);
    increment.addColumn(COLUMN_FAMILY, qual2, incr2);
    Result result = table.increment(increment);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1))));
    Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2))));

    // Double-check values with a Get
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    result = table.get(get);
    Assert.assertEquals("Expected four results, two for each column", 4, result.size());
    Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1))));
    Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2))));

    table.close();
  }

  /**
   * Requirement 6.2 - Specify a timerange (min ts, inclusive + max ts, exclusive). This will create
   * a new value that is an increment of the first value within this range, and will otherwise
   * create a new value.
   * Note: This is pretty weird.  Not sure who would use it, or if we need to support it.
   */
  @Test
  public void testIncrementWithTimerange() throws IOException {
    // Initialize data
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");

    // Put and increment in range.  Test min is inclusive, max is exclusive.
    Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, 101L, Bytes.toBytes(101L))
      .add(COLUMN_FAMILY, qual, 102L, Bytes.toBytes(102L))
      .add(COLUMN_FAMILY, qual, 103L, Bytes.toBytes(103L));
    table.put(put);

    // Increment doesn't increment in place.
    Increment increment = new Increment(rowKey);
    increment.setTimeRange(101L, 103L);
    increment.addColumn(COLUMN_FAMILY, qual, 1L);
    Result result = table.increment(increment);
    Assert.assertEquals("Should increment only 1 value", 1, result.size());
    Assert.assertEquals("It shoudl have incremented 102", 102L + 1L,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual))));

    Get get = new Get(rowKey).setMaxVersions(10);
    result = table.get(get);
    System.out.println(result + "");
    System.out
      .println(Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual))));
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
    get.setMaxVersions(10);
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

  @Test
  public void testTimestamp() throws IOException {
    long now = System.currentTimeMillis();
    long oneMinute = 60 * 1000;
    long fifteenMinutes = 15 * 60 * 1000;

    HTableInterface table = connection.getTable(TABLE_NAME);
    table.setAutoFlushTo(true);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qualifier, 1L, Bytes.toBytes(100L));
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
    }
    table.increment(increment);
    result = table.get(get);
    long timestamp2 = result.getColumnLatestCell(COLUMN_FAMILY, qualifier).getTimestamp();
    Assert.assertTrue("Time increases strictly", timestamp2 > timestamp1);
    Assert.assertTrue("Time doesn't move too fast", (timestamp2 - timestamp1) < oneMinute);
    table.close();
  }

  @Test
  public void testFailOnIncrementInt() throws IOException {
    // Initialize
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    int value = new Random().nextInt();
    Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, Bytes.toBytes(value));
    table.put(put);

    // Increment
    Increment increment = new Increment(rowKey).addColumn(COLUMN_FAMILY, qual, 1L);
    expectedException.expect(DoNotRetryIOException.class);
    expectedException.expectMessage("Attempted to increment field that isn't 64 bits wide");
    table.increment(increment);
  }

  @Test
  public void testFailOnIncrementString() throws IOException {
    // Initialize
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");
    Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, value);
    table.put(put);

    // Increment
    Increment increment = new Increment(rowKey).addColumn(COLUMN_FAMILY, qual, 1L);
    expectedException.expect(DoNotRetryIOException.class);
    expectedException.expectMessage("Attempted to increment field that isn't 64 bits wide");
    table.increment(increment);
  }

  /**
   * HBase should increment an 8-byte array just like it would a long.
   */
  @Test
  public void testIncrementEightBytes() throws IOException {
    // Initialize
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    byte[] value = new byte[8];
    new Random().nextBytes(value);
    ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
    buffer.put(value);
    buffer.flip();
    long equivalentValue = buffer.getLong();

    // Put the bytes in
    Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, value);
    table.put(put);

    // Increment
    Increment increment = new Increment(rowKey).addColumn(COLUMN_FAMILY, qual, 1L);
    Result result = table.increment(increment);
    Assert.assertEquals("Should have incremented the bytes like a long", equivalentValue + 1L,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual))));
  }

}
