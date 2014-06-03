package com.google.cloud.anviltop.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.junit.Assert;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/*
 * Copyright (c) 2013 Google Inc.
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
public class TestGet extends AbstractTest {
  /**
   * Requirement 3.2 - If a column family is requested, but no qualifier, all columns in that family
   * are returned
   */
  @Test
  public void testNoQualifier() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] qual1 = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    byte[] value1 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] qual2 = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    byte[] value2 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] qual3 = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    byte[] value3 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual1, value1);
    put.add(COLUMN_FAMILY, qual2, value2);
    put.add(COLUMN_FAMILY, qual3, value3);
    table.put(put);

    Get get = new Get(rowKey);
    get.addFamily(COLUMN_FAMILY);
    Result result = table.get(get);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual1));
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual2));
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual3));
    Assert.assertArrayEquals(value1,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals(value2,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2)));
    Assert.assertArrayEquals(value3,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual3)));

    Delete delete = new Delete(rowKey);
    table.delete(delete);

    table.close();
  }

  /**
   * Requirement 3.3 - Multiple family:qualifiers can be requested for a single row.
   */
  @Test
  public void testMultipleQualifiers() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] qual1 = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    byte[] value1 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] qual2 = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    byte[] value2 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] qual3 = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    byte[] value3 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual1, value1);
    put.add(COLUMN_FAMILY, qual2, value2);
    put.add(COLUMN_FAMILY, qual3, value3);
    table.put(put);

    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qual1);
    get.addColumn(COLUMN_FAMILY, qual3);
    Result result = table.get(get);

    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual1));
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual3));
    Assert.assertArrayEquals(value1,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals(value3,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual3)));

    Delete delete = new Delete(rowKey);
    table.delete(delete);

    table.close();
  }

  /**
   * Requirement 3.4 - A time range of minTimestamp (inclusive) - maxTimestamp (exclusive) can be
   * specified.
   */
  @Test
  public void testTimeRange() throws IOException {
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] qual = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    long timestamp1 = System.currentTimeMillis();
    long timestamp2 = timestamp1 + 1;
    long timestamp3 = timestamp2 + 1;
    long timestamp4 = timestamp3 + 1;
    long timestamp5 = timestamp4 + 1;
    byte[] value1 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value2 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value3 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value4 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value5 = Bytes.toBytes("value-" + RandomStringUtils.random(8));

    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual, timestamp1, value1);
    put.add(COLUMN_FAMILY, qual, timestamp2, value2);
    put.add(COLUMN_FAMILY, qual, timestamp3, value3);
    put.add(COLUMN_FAMILY, qual, timestamp4, value4);
    put.add(COLUMN_FAMILY, qual, timestamp5, value5);
    table.put(put);

    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qual);
    get.setTimeRange(timestamp2, timestamp5);
    get.setMaxVersions(5);
    Result result = table.get(get);

    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertEquals(3, cells.size());

    // Cells return in descending order
    Assert.assertEquals(timestamp4, cells.get(0).getTimestamp());
    Assert.assertArrayEquals(value4, CellUtil.cloneValue(cells.get(0)));
    Assert.assertEquals(timestamp3, cells.get(1).getTimestamp());
    Assert.assertArrayEquals(value3, CellUtil.cloneValue(cells.get(1)));
    Assert.assertEquals(timestamp2, cells.get(2).getTimestamp());
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(cells.get(2)));

    Delete delete = new Delete(rowKey);
    table.delete(delete);

    table.close();
  }

  /**
   * Requirement 3.5 - A single timestamp can be specified.
   */
  @Test
  public void testSingleTimestamp() throws IOException {
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] qual = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    long timestamp1 = System.currentTimeMillis();
    long timestamp2 = timestamp1 + 1;
    long timestamp3 = timestamp2 + 1;
    byte[] value1 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value2 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value3 = Bytes.toBytes("value-" + RandomStringUtils.random(8));

    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual, timestamp1, value1);
    put.add(COLUMN_FAMILY, qual, timestamp2, value2);
    put.add(COLUMN_FAMILY, qual, timestamp3, value3);
    table.put(put);

    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qual);
    get.setTimeStamp(timestamp2);
    get.setMaxVersions(5);
    Result result = table.get(get);

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertEquals(1, cells.size());

    // Cells return in descending order
    Assert.assertEquals(timestamp2, cells.get(0).getTimestamp());
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(cells.get(0)));

    Delete delete = new Delete(rowKey);
    table.delete(delete);

    table.close();
  }

  /**
   * Requirement 3.6 - Client can request a maximum # of most recent versions returned.
   */
  @Test
  public void testMaxVersions() throws IOException {
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] qual = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    long timestamp1 = System.currentTimeMillis();
    long timestamp2 = timestamp1 + 1;
    long timestamp3 = timestamp2 + 1;
    long timestamp4 = timestamp3 + 1;
    long timestamp5 = timestamp4 + 1;
    byte[] value1 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value2 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value3 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value4 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] value5 = Bytes.toBytes("value-" + RandomStringUtils.random(8));

    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual, timestamp1, value1);
    put.add(COLUMN_FAMILY, qual, timestamp2, value2);
    put.add(COLUMN_FAMILY, qual, timestamp3, value3);
    put.add(COLUMN_FAMILY, qual, timestamp4, value4);
    put.add(COLUMN_FAMILY, qual, timestamp5, value5);
    table.put(put);

    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qual);
    get.setMaxVersions(3);
    Result result = table.get(get);

    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertEquals(3, cells.size());

    // Cells return in descending order
    Assert.assertEquals(timestamp5, cells.get(0).getTimestamp());
    Assert.assertArrayEquals(value5, CellUtil.cloneValue(cells.get(0)));
    Assert.assertEquals(timestamp4, cells.get(1).getTimestamp());
    Assert.assertArrayEquals(value4, CellUtil.cloneValue(cells.get(1)));
    Assert.assertEquals(timestamp3, cells.get(2).getTimestamp());
    Assert.assertArrayEquals(value3, CellUtil.cloneValue(cells.get(2)));

    Delete delete = new Delete(rowKey);
    table.delete(delete);

    table.close();
  }
}
