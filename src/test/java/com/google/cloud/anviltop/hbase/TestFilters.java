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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestFilters extends AbstractTest {
  /**
   * Requirement 9.1 - ColumnCountGetFilter - return first N columns on rows only
   */
  @Test
  public void testColumnCountGetFilter() throws Exception {
    // Initialize data
    int numColumns = 20;
    int numColumnsToFilter = 10;
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[][] quals = new byte[numColumns][];
    byte[][][] values = new byte[2][][];
    values[0] = dataHelper.randomData("testvalue-", numColumns);
    values[1] = dataHelper.randomData("testvalue-", numColumns);
    Put put = new Put(rowKey);
    for (int i = 0; i < numColumns; ++i) {
      quals[i] = Bytes.toBytes(i);
      // Add two timestamps to test that filter only grabs the latest version
      put.add(COLUMN_FAMILY, quals[i], 1L, values[0][i]);
      put.add(COLUMN_FAMILY, quals[i], 2L, values[1][i]);
    }
    table.put(put);

    // Filter and test
    Filter filter = new ColumnCountGetFilter(numColumnsToFilter);
    Get get = new Get(rowKey).setFilter(filter).setMaxVersions(10);
    Result result = table.get(get);
    Assert.assertEquals("Should have filtered to N columns", numColumnsToFilter, result.size());
    for (int i = 0 ; i < numColumnsToFilter; ++i) {
      Assert.assertTrue("Should contain qual " + Bytes.toInt(quals[i]),
        result.containsColumn(COLUMN_FAMILY, quals[i]));
      List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, quals[i]);
      Assert.assertEquals("Should have only the latest version", 1, cells.size());
      Assert.assertArrayEquals("Value v2 should be first and match", values[1][i],
        CellUtil.cloneValue(cells.get(0)));
    }

    table.close();
  }

  /**
   * Requirement 9.2 - ColumnPaginationFilter - same as ColumnCountGetFilter, but with an offset
   * too; offset can be a # of cols, or can be a particular qualifier byte[] value (inclusive)
   */
  @Test
  public void testColumnPaginationFilter() throws Exception {
    // Initialize data
    int numColumns = 20;
    int numColumnsToFilter = 8;
    int offset = 5;
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[][] quals = new byte[numColumns][];
    byte[][][] values = new byte[2][][];
    values[0] = dataHelper.randomData("testvalue-", numColumns);
    values[1] = dataHelper.randomData("testvalue-", numColumns);
    Put put = new Put(rowKey);
    for (int i = 0; i < numColumns; ++i) {
      quals[i] = Bytes.toBytes(i);
      // Add two timestamps to test that filter only grabs the latest version
      put.add(COLUMN_FAMILY, quals[i], 1L, values[0][i]);
      put.add(COLUMN_FAMILY, quals[i], 2L, values[1][i]);
    }
    table.put(put);

    // Filter and test
    Filter filter = new ColumnPaginationFilter(numColumnsToFilter, offset);
    Get get = new Get(rowKey).setFilter(filter).setMaxVersions(10);
    Result result = table.get(get);
    Assert.assertEquals("Should have filtered to N columns", numColumnsToFilter, result.size());
    for (int i = offset ; i < (numColumnsToFilter + offset); ++i) {
      Assert.assertTrue("Should contain qual " + Bytes.toInt(quals[i]),
        result.containsColumn(COLUMN_FAMILY, quals[i]));
      List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, quals[i]);
      Assert.assertEquals("Should have only the latest version", 1, cells.size());
      Assert.assertArrayEquals("Value v2 should be first and match", values[1][i],
        CellUtil.cloneValue(cells.get(0)));
    }

    table.close();
  }

  /**
   * Requirement 9.2
   */
  @Test
  public void testColumnPaginationFilter_StartingAtParticularQualifier() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    Put put = new Put(rowKey);
    byte[] value = Bytes.toBytes("someval");
    put.add(COLUMN_FAMILY, Bytes.toBytes("A"), value);
    put.add(COLUMN_FAMILY, Bytes.toBytes("AA"), value);
    put.add(COLUMN_FAMILY, Bytes.toBytes("B"), value);
    put.add(COLUMN_FAMILY, Bytes.toBytes("BB"), value);
    put.add(COLUMN_FAMILY, Bytes.toBytes("C"), value);
    table.put(put);

    // Filter and test
    Filter filter = new ColumnPaginationFilter(3, Bytes.toBytes("AA"));
    Get get = new Get(rowKey).setFilter(filter).setMaxVersions(10);
    Result result = table.get(get);
    Assert.assertEquals("Should have filtered to N columns", 3, result.size());
    Assert.assertEquals("AA", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[0])));
    Assert.assertEquals("B", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[1])));
    Assert.assertEquals("BB", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[2])));

    table.close();
  }

  /**
   * Requirement 9.1
   * Requirement 9.2
   */
  @Test
  public void testColumnFilterScan() throws Exception {
    // Initialize data
    int numRows = 5;
    int numColumns = 20;
    int numColumnsToFilter = 8;
    int offset = 5;
    HTableInterface table = connection.getTable(TABLE_NAME);
    String rowPrefix = "testColumnFilterScan" + RandomStringUtils.randomAlphanumeric(5);
    String endRowKey = "testColumnFilterScan" + "zzzzzzz";
    byte[][] rowKeys = dataHelper.randomData(rowPrefix + "-", numRows);
    byte[][] quals = dataHelper.randomData("testqual-", numColumns);
    byte[][] values = dataHelper.randomData("testvalue-", numColumns);
    for (int i = 0; i < numRows; ++i) {
      Put put = new Put(rowKeys[i]);
      for (int j = 0; j < numColumns; ++j) {
        put.add(COLUMN_FAMILY, quals[j], values[j]);
      }
      table.put(put);
    }

    // Test ColumnCountGetFilter on scan.  ColumnCountGetFilter is not made for scans, and once
    // the column limit has been met, Filter#filterAllRemaining() returns true.
    Filter filter = new ColumnCountGetFilter(numColumnsToFilter);
    Scan scan = new Scan(Bytes.toBytes(rowPrefix), Bytes.toBytes(endRowKey)).setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    Result[] results = scanner.next(1000);
    Assert.assertEquals(1, results.length);

    // Test ColumnPaginationFilter on scan
    filter = new ColumnPaginationFilter(numColumnsToFilter, offset);
    scan = new Scan(Bytes.toBytes(rowPrefix), Bytes.toBytes(endRowKey)).setFilter(filter);
    scanner = table.getScanner(scan);
    results = scanner.next(1000);
    Assert.assertEquals(numRows, results.length);
    for (int i = 0; i < numRows; ++i) {
      Result result = results[i];
      Assert.assertEquals("Should have filtered to N columns", numColumnsToFilter, result.size());
    }

    table.close();
  }

  /**
   * Requirement 9.3 - ColumnPrefixFilter - select keys with columns that match a particular prefix
   */
  @Test
  public void testColumnPrefixFilter() throws Exception {
    // Initialize
    int numGoodCols = 5;
    int numBadCols = 20;
    String goodColPrefix = "bueno";
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testRow-");
    Put put = new Put(rowKey);
    for (int i = 0; i < numBadCols; ++i) {
      put.add(COLUMN_FAMILY, dataHelper.randomData(""), Bytes.toBytes("someval"));
    }
    for (int i = 0; i < numGoodCols; ++i) {
      put.add(COLUMN_FAMILY, dataHelper.randomData(goodColPrefix), Bytes.toBytes("someval"));
    }
    table.put(put);

    // Filter for results
    Filter filter = new ColumnPrefixFilter(Bytes.toBytes("bueno"));
    Get get = new Get(rowKey).setFilter(filter);
    Result result = table.get(get);
    Assert.assertEquals("Should only return good columns", numGoodCols, result.size());
    Cell[] cells = result.rawCells();
    for (Cell cell : cells) {
      Assert.assertTrue("Should have good column prefix",
        Bytes.toString(CellUtil.cloneQualifier(cell)).startsWith(goodColPrefix));
    }

    table.close();
  }

  /**
   * Requirement 9.4 - ColumnRangeFilter - select keys with columns between minColumn and maxColumn
   *
   * Insert 5 cols: A, AA, B, BB, C, CC.  Test filtering on these columns with different
   * combinations of start/end keys being inclusive/exclusive.
   */
  @Test
  public void testColumnRangeFilter() throws Exception {
    // Initialize
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testRow-");
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, Bytes.toBytes("A"), Bytes.toBytes("someval"));
    put.add(COLUMN_FAMILY, Bytes.toBytes("AA"), Bytes.toBytes("someval"));
    put.add(COLUMN_FAMILY, Bytes.toBytes("B"), Bytes.toBytes("someval"));
    put.add(COLUMN_FAMILY, Bytes.toBytes("BB"), Bytes.toBytes("someval"));
    put.add(COLUMN_FAMILY, Bytes.toBytes("C"), Bytes.toBytes("someval"));
    put.add(COLUMN_FAMILY, Bytes.toBytes("CC"), Bytes.toBytes("someval"));
    table.put(put);

    // Filter for "B" exclusive, "C" exclusive
    Filter filter = new ColumnRangeFilter(Bytes.toBytes("B"), false, Bytes.toBytes("C"), false);
    Get get = new Get(rowKey).setFilter(filter);
    Result result = table.get(get);
    Assert.assertEquals("Should only return \"BB\"", 1, result.size());
    Assert.assertEquals("BB", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[0])));

    // Filter for "B" exclusive, "C" inclusive
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), false, Bytes.toBytes("C"), true);
    get = new Get(rowKey).setFilter(filter);
    result = table.get(get);
    Assert.assertEquals("Should return \"BB\", \"C\"", 2, result.size());
    Assert.assertEquals("BB", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[0])));
    Assert.assertEquals("C", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[1])));

    // Filter for "B" inclusive, "C" exclusive
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), true, Bytes.toBytes("C"), false);
    get = new Get(rowKey).setFilter(filter);
    result = table.get(get);
    Assert.assertEquals("Should return \"B\", \"BB\"", 2, result.size());
    Assert.assertEquals("B", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[0])));
    Assert.assertEquals("BB", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[1])));

    // Filter for "B" inclusive, "C" inclusive
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), true, Bytes.toBytes("C"), true);
    get = new Get(rowKey).setFilter(filter);
    result = table.get(get);
    Assert.assertEquals("Should return \"B\", \"BB\", \"C\"", 3, result.size());
    Assert.assertEquals("B", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[0])));
    Assert.assertEquals("BB", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[1])));
    Assert.assertEquals("C", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[2])));

    table.close();
  }
}
