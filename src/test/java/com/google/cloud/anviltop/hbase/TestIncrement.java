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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class TestIncrement extends AbstractTest {
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
    table.increment(increment);

    // Check values
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("Expected four results, two for each column", 4, result.size());
    Assert.assertTrue("Expected qual1", result.containsColumn(COLUMN_FAMILY, qual1));
    Assert.assertTrue("Expected qual2", result.containsColumn(COLUMN_FAMILY, qual2));
    Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1))));
    Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2))));

    table.close();
  }

  /**
   * Requirement 6.2 - Specify a timerange (min ts, inclusive + max ts, exclusive). This will
   * create a new value that is an increment of the first value within this range, and will
   * otherwise create a new value.
   *
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
    Assert.assertEquals("It shoudl have incremented 102", 102L + 1L, Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual))));

    Get get = new Get(rowKey).setMaxVersions(10);
    result = table.get(get);
    System.out.println(result + "");
    System.out.println(Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual))));
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
}
