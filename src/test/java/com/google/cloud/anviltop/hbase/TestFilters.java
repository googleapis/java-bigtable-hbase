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

import static com.google.cloud.anviltop.hbase.IntegrationTests.TABLE_NAME;
import static com.google.cloud.anviltop.hbase.IntegrationTests.COLUMN_FAMILY;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
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
    Table table = connection.getTable(TABLE_NAME);
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
    Table table = connection.getTable(TABLE_NAME);
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
    Table table = connection.getTable(TABLE_NAME);
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
    Table table = connection.getTable(TABLE_NAME);
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
    Table table = connection.getTable(TABLE_NAME);
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
    Table table = connection.getTable(TABLE_NAME);
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

  /**
   * Requirement 9.5 - RowFilter - filter by rowkey against a given Comparable
   *
   * Test the BinaryComparator against EQUAL, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL,
   * NOT_EQUAL, and NO_OP.  BinaryComparator compares two byte arrays lexicographically using
   * Bytes.compareTo(byte[], byte[]).
   */
  @Test
  public void testRowFilterBinaryComparator() throws Exception {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    String rowKeyPrefix = "testRowFilter-" + RandomStringUtils.randomAlphabetic(10);
    byte[] rowKey1 = Bytes.toBytes(rowKeyPrefix + "A");
    byte[] rowKey2 = Bytes.toBytes(rowKeyPrefix + "AA");
    byte[] rowKey3 = Bytes.toBytes(rowKeyPrefix + "B");
    byte[] rowKey4 = Bytes.toBytes(rowKeyPrefix + "BB");
    byte[] qual = Bytes.toBytes("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { rowKey1, rowKey2, rowKey3, rowKey4}) {
      Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, value);
      table.put(put);
    }

    // Test BinaryComparator - EQUAL
    ByteArrayComparable rowKey2Comparable = new BinaryComparator(rowKey2);
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, rowKey2Comparable);
    Result[] results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKey2, results[0].getRow());

    // Test BinaryComparator - GREATER
    filter = new RowFilter(CompareFilter.CompareOp.GREATER, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKey3, results[0].getRow());

    // Test BinaryComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowKey2, results[0].getRow());
    Assert.assertArrayEquals(rowKey3, results[1].getRow());

    // Test BinaryComparator - LESS
    filter = new RowFilter(CompareFilter.CompareOp.LESS, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKey1, results[0].getRow());

    // Test BinaryComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowKey1, results[0].getRow());
    Assert.assertArrayEquals(rowKey2, results[1].getRow());

    // Test BinaryComparator - NOT_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowKey1, results[0].getRow());
    Assert.assertArrayEquals(rowKey3, results[1].getRow());

    // Test BinaryComparator - NO_OP
    filter = new RowFilter(CompareFilter.CompareOp.NO_OP, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }

  /**
   * Requirement 9.5
   *
   * Test the BinaryPrefixComparator against EQUAL, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL,
   * NOT_EQUAL, and NO_OP.  BinaryPrefixComparator compares against a specified byte array, up to
   * the length of this byte array.
   */
  @Test
  public void testRowFilterBinaryPrefixComparator() throws Exception {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    String rowKeyPrefix = "testRowFilter-" + RandomStringUtils.randomAlphabetic(10);
    byte[] rowA = Bytes.toBytes(rowKeyPrefix + "A");
    byte[] rowAA = Bytes.toBytes(rowKeyPrefix + "AA");
    byte[] rowB = Bytes.toBytes(rowKeyPrefix + "B");
    byte[] rowBB = Bytes.toBytes(rowKeyPrefix + "BB");
    byte[] rowC = Bytes.toBytes(rowKeyPrefix + "C");
    byte[] rowCC = Bytes.toBytes(rowKeyPrefix + "CC");
    byte[] rowD = Bytes.toBytes(rowKeyPrefix + "D");
    byte[] qual = Bytes.toBytes("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { rowA, rowAA, rowB, rowBB, rowC, rowCC, rowD }) {
      Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, value);
      table.put(put);
    }

    // Test BinaryPrefixComparator - EQUAL
    ByteArrayComparable rowBComparable = new BinaryPrefixComparator(rowB);
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, rowBComparable);
    Result[] results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowB, results[0].getRow());
    Assert.assertArrayEquals(rowBB, results[1].getRow());

    // Test BinaryPrefixComparator - GREATER
    filter = new RowFilter(CompareFilter.CompareOp.GREATER, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowC, results[0].getRow());
    Assert.assertArrayEquals(rowCC, results[1].getRow());

    // Test BinaryPrefixComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowB, results[0].getRow());
    Assert.assertArrayEquals(rowBB, results[1].getRow());
    Assert.assertArrayEquals(rowC, results[2].getRow());
    Assert.assertArrayEquals(rowCC, results[3].getRow());

    // Test BinaryPrefixComparator - LESS
    filter = new RowFilter(CompareFilter.CompareOp.LESS, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());
    Assert.assertArrayEquals(rowAA, results[1].getRow());

    // Test BinaryPrefixComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());
    Assert.assertArrayEquals(rowAA, results[1].getRow());
    Assert.assertArrayEquals(rowB, results[2].getRow());
    Assert.assertArrayEquals(rowBB, results[3].getRow());

    // Test BinaryPrefixComparator - NOT_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());
    Assert.assertArrayEquals(rowAA, results[1].getRow());
    Assert.assertArrayEquals(rowC, results[2].getRow());
    Assert.assertArrayEquals(rowCC, results[3].getRow());

    // Test BinaryPrefixComparator - NO_OP
    filter = new RowFilter(CompareFilter.CompareOp.NO_OP, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }

  /**
   * Requirement 9.5
   *
   * Test the BitComparator with XOR against EQUAL, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL,
   * NOT_EQUAL, and NO_OP.  Perform XOR bit operation on the specified array and returns whether the
   * result is non-zero.  When comparing arrays of different length, the comparison fails regardless
   * of the operation.  If the comparison fails, it is returned as LESS THAN by the comparator.
   */
  @Test
  public void testRowFilterBitComparatorXOR() throws Exception {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    String rowKeyPrefix = "testRowFilter-" + RandomStringUtils.randomAlphabetic(10);
    byte[] row0000 = Bytes.fromHex("00");
    byte[] row0101 = Bytes.fromHex("55");
    byte[] row1010 = Bytes.fromHex("aa");
    byte[] row1111 = Bytes.fromHex("ff");
    byte[] rowDiffLength = Bytes.fromHex("abcd");
    byte[] rowMax = Bytes.fromHex("ffffff");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { row0000, row0101, row1010, row1111, rowDiffLength}) {
      Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, value);
      table.put(put);
    }

    // Test BitComparator - XOR - EQUAL
    ByteArrayComparable rowBComparable = new BitComparator(row0101, BitComparator.BitwiseOp.XOR);
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, rowBComparable);
    Result[] results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1010, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1111, results[2].getRow());

    // Test BitComparator - XOR - GREATER (effectively no values)
    filter = new RowFilter(CompareFilter.CompareOp.GREATER, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BitComparator - XOR - GREATER_OR_EQUAL (same effect as EQUAL)
    filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1010, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1111, results[2].getRow());

    // Test BitComparator - XOR - LESS (same effect as NOT_EQUAL)
    filter = new RowFilter(CompareFilter.CompareOp.LESS, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0101, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), rowDiffLength, results[1].getRow());

    // Test BitComparator - XOR - LESS_OR_EQUAL (effectively all values)
    filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 5, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), rowDiffLength, results[3].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[4].getRow()), row1111, results[4].getRow());

    // Test BitComparator - XOR - NOT_EQUAL (same effect as LESS)
    filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0101, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), rowDiffLength, results[1].getRow());

    // Test BitComparator - XOR - NO_OP (no values)
    filter = new RowFilter(CompareFilter.CompareOp.NO_OP, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }

  /**
   * Requirement 9.5
   *
   * Test the BitComparator with AND against EQUAL, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL,
   * NOT_EQUAL, and NO_OP.  Perform AND bit operation on the specified array and returns whether the
   * result is non-zero.  When comparing arrays of different length, the comparison fails regardless
   * of the operation.  If the comparison fails, it is returned as LESS THAN by the comparator.
   */
  @Test
  public void testRowFilterBitComparatorAND() throws Exception {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    String rowKeyPrefix = "testRowFilter-" + RandomStringUtils.randomAlphabetic(10);
    byte[] row0000 = Bytes.fromHex("00");
    byte[] row0101 = Bytes.fromHex("55");
    byte[] row1010 = Bytes.fromHex("aa");
    byte[] row1111 = Bytes.fromHex("ff");
    byte[] rowDiffLength = Bytes.fromHex("abcd");
    byte[] rowMax = Bytes.fromHex("ffffff");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { row0000, row0101, row1010, row1111, rowDiffLength}) {
      Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, value);
      table.put(put);
    }

    // Test BitComparator - AND - EQUAL
    ByteArrayComparable rowBComparable = new BitComparator(row0101, BitComparator.BitwiseOp.AND);
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, rowBComparable);
    Result[] results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0101, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1111, results[1].getRow());

    // Test BitComparator - AND - GREATER (effectively no values)
    filter = new RowFilter(CompareFilter.CompareOp.GREATER, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BitComparator - AND - GREATER_OR_EQUAL (same effect as EQUAL)
    filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0101, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1111, results[1].getRow());

    // Test BitComparator - AND - LESS (same effect as NOT_EQUAL)
    filter = new RowFilter(CompareFilter.CompareOp.LESS, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1010, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), rowDiffLength, results[2].getRow());

    // Test BitComparator - AND - LESS_OR_EQUAL (effectively all values)
    filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 5, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), rowDiffLength, results[3].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[4].getRow()), row1111, results[4].getRow());

    // Test BitComparator - AND - NOT_EQUAL (same effect as LESS)
    filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1010, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), rowDiffLength, results[2].getRow());

    // Test BitComparator - AND - NO_OP (no values)
    filter = new RowFilter(CompareFilter.CompareOp.NO_OP, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }

  /**
   * Requirement 9.5
   *
   * Test the BitComparator with AND against EQUAL, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL,
   * NOT_EQUAL, and NO_OP.  Perform AND bit operation on the specified array and returns whether the
   * result is non-zero.  When comparing arrays of different length, the comparison fails regardless
   * of the operation.  If the comparison fails, it is returned as LESS THAN by the comparator.
   */
  @Test
  public void testRowFilterBitComparatorOR() throws Exception {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    String rowKeyPrefix = "testRowFilter-" + RandomStringUtils.randomAlphabetic(10);
    byte[] row0000 = Bytes.fromHex("00");
    byte[] row0101 = Bytes.fromHex("55");
    byte[] row1010 = Bytes.fromHex("aa");
    byte[] row1111 = Bytes.fromHex("ff");
    byte[] rowDiffLength = Bytes.fromHex("abcd");
    byte[] rowMax = Bytes.fromHex("ffffff");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { row0000, row0101, row1010, row1111, rowDiffLength}) {
      Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, value);
      table.put(put);
    }

    // Test BitComparator - OR - EQUAL
    ByteArrayComparable rowBComparable = new BitComparator(row0101, BitComparator.BitwiseOp.OR);
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, rowBComparable);
    Result[] results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), row1111, results[3].getRow());

    // Test BitComparator - OR - GREATER (effectively no values)
    filter = new RowFilter(CompareFilter.CompareOp.GREATER, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BitComparator - OR - GREATER_OR_EQUAL (same effect as EQUAL)
    filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), row1111, results[3].getRow());

    // Test BitComparator - OR - LESS (same effect as NOT_EQUAL)
    filter = new RowFilter(CompareFilter.CompareOp.LESS, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), rowDiffLength, results[0].getRow());

    // Test BitComparator - OR - LESS_OR_EQUAL (effectively all values)
    filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 5, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), rowDiffLength, results[3].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[4].getRow()), row1111, results[4].getRow());

    // Test BitComparator - OR - NOT_EQUAL (same effect as LESS)
    filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), rowDiffLength, results[0].getRow());

    // Test BitComparator - OR - NO_OP (no values)
    filter = new RowFilter(CompareFilter.CompareOp.NO_OP, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }

  /**
   * Requirement 9.5
   *
   * Test the NullComparator against EQUAL, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL,
   * NOT_EQUAL, and NO_OP.  It behaves the same as constructing a BinaryComparator with an empty
   * byte array.
   */
  @Test
  public void testRowFilterNullComparator() throws Exception {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    String rowKeyPrefix = "testRowFilter-" + RandomStringUtils.randomAlphabetic(10);
    byte[] rowKeyA = Bytes.toBytes(rowKeyPrefix + "A");
    byte[] rowKeyB = Bytes.toBytes(rowKeyPrefix + "B");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    Put put = new Put(rowKeyA).add(COLUMN_FAMILY, qual, value);
    table.put(put);

    // Test BinaryComparator - EQUAL
    ByteArrayComparable nullComparator = new NullComparator();
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, nullComparator);
    Result[] results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BinaryComparator - GREATER
    filter = new RowFilter(CompareFilter.CompareOp.GREATER, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BinaryComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BinaryComparator - LESS
    filter = new RowFilter(CompareFilter.CompareOp.LESS, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKeyA, results[0].getRow());

    // Test BinaryComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKeyA, results[0].getRow());

    // Test BinaryComparator - NOT_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKeyA, results[0].getRow());

    // Test BinaryComparator - NO_OP
    filter = new RowFilter(CompareFilter.CompareOp.NO_OP, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }

  /**
   * Requirement 9.5
   *
   * Test the SubstringComparator.  Case-insensitive check for values containing the given
   * substring. Only EQUAL and NOT_EQUAL tests are valid with this comparator, but the other
   * operators can still return deterministic results in HBase.
   */
  @Test
  public void testRowFilterSubstringComparator() throws Exception {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    byte[] rowab = Bytes.toBytes("ab");  // Substring match, but out of row range
    byte[] rowA = Bytes.toBytes("A");
    byte[] rowAB= Bytes.toBytes("AB");
    byte[] rowAbC = Bytes.toBytes("AbC");
    byte[] rowDaB = Bytes.toBytes("DaB");
    byte[] rowDabE = Bytes.toBytes("DabE");
    byte[] rowZ = Bytes.toBytes("Z");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { rowab, rowA, rowAB, rowAbC, rowDaB, rowDabE}) {
      Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, value);
      table.put(put);
    }

    // Test SubstringComparator - EQUAL
    ByteArrayComparable rowKey2Comparable = new SubstringComparator("AB");
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, rowKey2Comparable);
    Result[] results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowAB, results[0].getRow());
    Assert.assertArrayEquals(rowAbC, results[1].getRow());
    Assert.assertArrayEquals(rowDaB, results[2].getRow());
    Assert.assertArrayEquals(rowDabE, results[3].getRow());

    // Test SubstringComparator - GREATER
    filter = new RowFilter(CompareFilter.CompareOp.GREATER, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test SubstringComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowAB, results[0].getRow());
    Assert.assertArrayEquals(rowAbC, results[1].getRow());
    Assert.assertArrayEquals(rowDaB, results[2].getRow());
    Assert.assertArrayEquals(rowDabE, results[3].getRow());

    // Test SubstringComparator - LESS
    filter = new RowFilter(CompareFilter.CompareOp.LESS, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());

    // Test SubstringComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 5, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());
    Assert.assertArrayEquals(rowAB, results[1].getRow());
    Assert.assertArrayEquals(rowAbC, results[2].getRow());
    Assert.assertArrayEquals(rowDaB, results[3].getRow());
    Assert.assertArrayEquals(rowDabE, results[4].getRow());

    // Test SubstringComparator - NOT_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());

    // Test SubstringComparator - NO_OP
    filter = new RowFilter(CompareFilter.CompareOp.NO_OP, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }

  /**
   * Requirement 9.5
   *
   * Test the SubstringComparator.  Case-insensitive check for values containing the given
   * substring. Only EQUAL and NOT_EQUAL tests are valid with this comparator, but the other
   * operators can still return deterministic results in HBase.
   */
  @Test
  public void testRowFilterRegexStringComparator() throws Exception {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    byte[] row0 = Bytes.toBytes("0");  // Substring match, but out of row range
    byte[] rowGoodIP1 = Bytes.toBytes("192.168.2.13");
    byte[] rowGoodIP2 = Bytes.toBytes("8.8.8.8");
    byte[] rowGoodIPv6 = Bytes.toBytes("FE80:0000:0000:0000:0202:B3FF:FE1E:8329");
    byte[] rowBadIP = Bytes.toBytes("1.2.278.0");
    byte[] rowTelephone = Bytes.toBytes("1-212-867-5309");
    byte[] rowRandom = dataHelper.randomData("9-rowkey");
    byte[] endRow = Bytes.fromHex("ffffff");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { row0, rowGoodIP1, rowGoodIP2, rowGoodIPv6, rowBadIP,
        rowTelephone, rowRandom }) {
      Put put = new Put(rowKey).add(COLUMN_FAMILY, qual, value);
      table.put(put);
    }
    String regexIPAddr =
      // v4 IP address
      "(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3,3}" +
        "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))(\\/[0-9]+)?" +
        "|" +
        // v6 IP address
        "((([\\dA-Fa-f]{1,4}:){7}[\\dA-Fa-f]{1,4})(:([\\d]{1,3}.)" +
        "{3}[\\d]{1,3})?)(\\/[0-9]+)?";

    // Test RegexStringComparator - EQUAL
    ByteArrayComparable rowKey2Comparable = new RegexStringComparator(regexIPAddr);
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, rowKey2Comparable);
    Result[] results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(rowGoodIP1, results[0].getRow());
    Assert.assertArrayEquals(rowGoodIP2, results[1].getRow());
    Assert.assertArrayEquals(rowGoodIPv6, results[2].getRow());

    // Test RegexStringComparator - NOT_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(row0, results[0].getRow());
    Assert.assertArrayEquals(rowTelephone, results[1].getRow());
    Assert.assertArrayEquals(rowBadIP, results[2].getRow());
    Assert.assertArrayEquals(rowRandom, results[3].getRow());

    // Test RegexStringComparator - GREATER
    filter = new RowFilter(CompareFilter.CompareOp.GREATER, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test RegexStringComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(rowGoodIP1, results[0].getRow());
    Assert.assertArrayEquals(rowGoodIP2, results[1].getRow());
    Assert.assertArrayEquals(rowGoodIPv6, results[2].getRow());

    // Test RegexStringComparator - LESS
    filter = new RowFilter(CompareFilter.CompareOp.LESS, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(row0, results[0].getRow());
    Assert.assertArrayEquals(rowTelephone, results[1].getRow());
    Assert.assertArrayEquals(rowBadIP, results[2].getRow());
    Assert.assertArrayEquals(rowRandom, results[3].getRow());

    // Test RegexStringComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 7, results.length);
    Assert.assertArrayEquals(row0, results[0].getRow());
    Assert.assertArrayEquals(rowTelephone, results[1].getRow());
    Assert.assertArrayEquals(rowBadIP, results[2].getRow());
    Assert.assertArrayEquals(rowGoodIP1, results[3].getRow());
    Assert.assertArrayEquals(rowGoodIP2, results[4].getRow());
    Assert.assertArrayEquals(rowRandom, results[5].getRow());
    Assert.assertArrayEquals(rowGoodIPv6, results[6].getRow());

    // Test RegexStringComparator - NO_OP
    filter = new RowFilter(CompareFilter.CompareOp.NO_OP, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }

  private Result[] scanWithFilter(Table t, byte[] startRow, byte[] endRow, byte[] qual,
      Filter f) throws IOException {
    Scan scan = new Scan(startRow, endRow).setFilter(f).addColumn(COLUMN_FAMILY, qual);
    ResultScanner scanner = t.getScanner(scan);
    Result[] results = scanner.next(10);
    return results;
  }
}
