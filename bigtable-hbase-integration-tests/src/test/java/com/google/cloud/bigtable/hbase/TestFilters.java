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

import static com.google.cloud.bigtable.hbase.IntegrationTests.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[][] quals = new byte[numColumns][];
    byte[][][] values = new byte[2][][];
    values[0] = dataHelper.randomData("testvalue-", numColumns);
    values[1] = dataHelper.randomData("testvalue-", numColumns);
    Put put = new Put(rowKey);
    for (int i = 0; i < numColumns; ++i) {
      quals[i] = Bytes.toBytes(i);
      // Add two timestamps to test that filter only grabs the latest version
      put.addColumn(COLUMN_FAMILY, quals[i], 1L, values[0][i]);
      put.addColumn(COLUMN_FAMILY, quals[i], 2L, values[1][i]);
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
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[][] quals = new byte[numColumns][];
    byte[][][] values = new byte[2][][];
    values[0] = dataHelper.randomData("testvalue-", numColumns);
    values[1] = dataHelper.randomData("testvalue-", numColumns);
    Put put = new Put(rowKey);
    for (int i = 0; i < numColumns; ++i) {
      quals[i] = Bytes.toBytes(i);
      // Add two timestamps to test that filter only grabs the latest version
      put.addColumn(COLUMN_FAMILY, quals[i], 1L, values[0][i]);
      put.addColumn(COLUMN_FAMILY, quals[i], 2L, values[1][i]);
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
  @Category(KnownGap.class)
  public void testColumnPaginationFilter_StartingAtParticularQualifier() throws Exception {
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    Put put = new Put(rowKey);
    byte[] value = Bytes.toBytes("someval");
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("A"), value);
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("AA"), value);
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("B"), value);
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("BB"), value);
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("C"), value);
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
  @Category(KnownGap.class)
  public void testColumnFilterScan() throws Exception {
    // Initialize data
    int numRows = 5;
    int numColumns = 20;
    int numColumnsToFilter = 8;
    int offset = 5;
    Table table = getConnection().getTable(TABLE_NAME);
    String rowPrefix = "testColumnFilterScan" + RandomStringUtils.randomAlphanumeric(5);
    String endRowKey = "testColumnFilterScan" + "zzzzzzz";
    byte[][] rowKeys = dataHelper.randomData(rowPrefix + "-", numRows);
    byte[][] quals = dataHelper.randomData("testqual-", numColumns);
    byte[][] values = dataHelper.randomData("testvalue-", numColumns);
    for (int i = 0; i < numRows; ++i) {
      Put put = new Put(rowKeys[i]);
      for (int j = 0; j < numColumns; ++j) {
        put.addColumn(COLUMN_FAMILY, quals[j], values[j]);
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
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testRow-");
    Put put = new Put(rowKey);
    for (int i = 0; i < numBadCols; ++i) {
      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(""), Bytes.toBytes("someval"));
    }
    for (int i = 0; i < numGoodCols; ++i) {
      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(goodColPrefix), Bytes.toBytes("someval"));
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
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testRow-");
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("A"), Bytes.toBytes("someval"));
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("AA"), Bytes.toBytes("someval"));
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("B"), Bytes.toBytes("someval"));
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("BB"), Bytes.toBytes("someval"));
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("C"), Bytes.toBytes("someval"));
    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("CC"), Bytes.toBytes("someval"));
    table.put(put);

    // Filter for "B" exclusive, "C" exclusive
    Filter filter = new ColumnRangeFilter(Bytes.toBytes("B"), false, Bytes.toBytes("C"), false);
    Get get = new Get(rowKey).setFilter(filter).addFamily(COLUMN_FAMILY);
    Result result = table.get(get);
    Assert.assertEquals("Should only return \"BB\"", 1, result.size());
    Assert.assertEquals("BB", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[0])));

    // Filter for "B" exclusive, "C" inclusive
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), false, Bytes.toBytes("C"), true);
    get = new Get(rowKey).setFilter(filter).addFamily(COLUMN_FAMILY);
    result = table.get(get);
    Assert.assertEquals("Should return \"BB\", \"C\"", 2, result.size());
    Assert.assertEquals("BB", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[0])));
    Assert.assertEquals("C", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[1])));

    // Filter for "B" inclusive, "C" exclusive
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), true, Bytes.toBytes("C"), false);
    get = new Get(rowKey).setFilter(filter).addFamily(COLUMN_FAMILY);
    result = table.get(get);
    Assert.assertEquals("Should return \"B\", \"BB\"", 2, result.size());
    Assert.assertEquals("B", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[0])));
    Assert.assertEquals("BB", Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[1])));

    // Filter for "B" inclusive, "C" inclusive
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), true, Bytes.toBytes("C"), true);
    get = new Get(rowKey).setFilter(filter).addFamily(COLUMN_FAMILY);
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
  @Category(KnownGap.class)
  public void testRowFilterBinaryComparator() throws Exception {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    String rowKeyPrefix = "testRowFilter-" + RandomStringUtils.randomAlphabetic(10);
    byte[] rowKey1 = Bytes.toBytes(rowKeyPrefix + "A");
    byte[] rowKey2 = Bytes.toBytes(rowKeyPrefix + "AA");
    byte[] rowKey3 = Bytes.toBytes(rowKeyPrefix + "B");
    byte[] rowKey4 = Bytes.toBytes(rowKeyPrefix + "BB");
    byte[] qual = Bytes.toBytes("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { rowKey1, rowKey2, rowKey3, rowKey4}) {
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
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
  @Category(KnownGap.class)
  public void testRowFilterBinaryPrefixComparator() throws Exception {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
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
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
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
  @Category(KnownGap.class)
  public void testRowFilterBitComparatorXOR() throws Exception {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] row0000 = Bytes.fromHex("00");
    byte[] row0101 = Bytes.fromHex("55");
    byte[] row1010 = Bytes.fromHex("aa");
    byte[] row1111 = Bytes.fromHex("ff");
    byte[] rowDiffLength = Bytes.fromHex("abcd");
    byte[] rowMax = Bytes.fromHex("ffffff");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { row0000, row0101, row1010, row1111, rowDiffLength}) {
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
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
  @Category(KnownGap.class)
  public void testRowFilterBitComparatorAND() throws Exception {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] row0000 = Bytes.fromHex("00");
    byte[] row0101 = Bytes.fromHex("55");
    byte[] row1010 = Bytes.fromHex("aa");
    byte[] row1111 = Bytes.fromHex("ff");
    byte[] rowDiffLength = Bytes.fromHex("abcd");
    byte[] rowMax = Bytes.fromHex("ffffff");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { row0000, row0101, row1010, row1111, rowDiffLength}) {
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
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
  @Category(KnownGap.class)
  public void testRowFilterBitComparatorOR() throws Exception {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] row0000 = Bytes.fromHex("00");
    byte[] row0101 = Bytes.fromHex("55");
    byte[] row1010 = Bytes.fromHex("aa");
    byte[] row1111 = Bytes.fromHex("ff");
    byte[] rowDiffLength = Bytes.fromHex("abcd");
    byte[] rowMax = Bytes.fromHex("ffffff");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    for (byte[] rowKey : new byte[][] { row0000, row0101, row1010, row1111, rowDiffLength}) {
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
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
  @Category(KnownGap.class)
  public void testRowFilterNullComparator() throws Exception {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    String rowKeyPrefix = "testRowFilter-" + RandomStringUtils.randomAlphabetic(10);
    byte[] rowKeyA = Bytes.toBytes(rowKeyPrefix + "A");
    byte[] rowKeyB = Bytes.toBytes(rowKeyPrefix + "B");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    Put put = new Put(rowKeyA).addColumn(COLUMN_FAMILY, qual, value);
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
  @Category(KnownGap.class)
  public void testRowFilterSubstringComparator() throws Exception {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
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
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
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
  @Category(KnownGap.class)
  public void testRowFilterRegexStringComparator() throws Exception {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
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
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
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

  /**
   * Test RowFilter with a RegexStringComparator and EQUAL comparator.
   * @throws IOException
   */
  @Test
  @Category(KnownGap.class)
  public void testDeterministRowRegexFilter() throws IOException {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
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
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value);
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

    table.close();
  }

  @Test
  @Category(KnownGap.class)
  public void testWhileMatchFilter_simple() throws IOException {
    String rowKeyPrefix = "wmf-simple-";
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForWhileMatchFilterTest(rowKeyPrefix, qualA);

    ByteArrayComparable rowValue2Comparable = new BinaryComparator(Bytes.toBytes("12"));
    ValueFilter valueFilter =
        new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, rowValue2Comparable);
    WhileMatchFilter simpleWhileMatch = new WhileMatchFilter(valueFilter);
    Scan scan = new Scan(Bytes.toBytes(rowKeyPrefix));
    scan.setFilter(simpleWhileMatch);

    int[] expected = {0, 1, 10, 11};
    assertWhileMatchFilterResult(qualA, table, scan, expected);
  }

  @Test
  @Category(KnownGap.class)
  public void testWhileMatchFilter_twoInterleaves() throws IOException {
    String rowKeyPrefix = "wmf-interleaves-";
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForWhileMatchFilterTest(rowKeyPrefix, qualA);

    ByteArrayComparable rowValue2Comparable1 = new BinaryComparator(Bytes.toBytes("12"));
    ValueFilter valueFilter1 =
        new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, rowValue2Comparable1);
    WhileMatchFilter simpleWhileMatch1 = new WhileMatchFilter(valueFilter1);
    ByteArrayComparable rowValue2Comparable2 = new BinaryComparator(Bytes.toBytes("15"));
    ValueFilter valueFilter2 =
        new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, rowValue2Comparable2);
    WhileMatchFilter simpleWhileMatch2 = new WhileMatchFilter(valueFilter2);
    FilterList filterList = new FilterList(
      Operator.MUST_PASS_ONE,
      simpleWhileMatch1,
      simpleWhileMatch2);
    Scan scan = new Scan(Bytes.toBytes(rowKeyPrefix));
    scan.setFilter(filterList);

    int[] expected = {0, 1, 10, 11, 12, 13, 14};
    assertWhileMatchFilterResult(qualA, table, scan, expected);
  }

  @Test
  @Category(KnownGap.class)
  public void testWhileMatchFilter_twoChained() throws IOException {
    String rowKeyPrefix = "wmf-chained-";
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForWhileMatchFilterTest(rowKeyPrefix, qualA);

    ByteArrayComparable rowValue2Comparable1 = new BinaryComparator(Bytes.toBytes("12"));
    ValueFilter valueFilter1 =
        new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, rowValue2Comparable1);
    WhileMatchFilter simpleWhileMatch1 = new WhileMatchFilter(valueFilter1);
    ByteArrayComparable rowValue2Comparable2 = new BinaryComparator(Bytes.toBytes("15"));
    ValueFilter valueFilter2 =
        new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, rowValue2Comparable2);
    WhileMatchFilter simpleWhileMatch2 = new WhileMatchFilter(valueFilter2);
    FilterList filterList = new FilterList(
      Operator.MUST_PASS_ALL,
      simpleWhileMatch1,
      simpleWhileMatch2);
    Scan scan = new Scan(Bytes.toBytes(rowKeyPrefix));
    scan.setFilter(filterList);

    int[] expected = {0, 1, 10, 11};
    assertWhileMatchFilterResult(qualA, table, scan, expected);
  }

  @Test
  @Category(KnownGap.class)
  public void testWhileMatchFilter_twoNested() throws IOException {
    String rowKeyPrefix = "wmf-nested-";
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForWhileMatchFilterTest(rowKeyPrefix, qualA);

    ByteArrayComparable rowValue2Comparable1 = new BinaryComparator(Bytes.toBytes("12"));
    ValueFilter valueFilter1 =
        new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, rowValue2Comparable1);
    WhileMatchFilter simpleWhileMatch1 = new WhileMatchFilter(valueFilter1);
    WhileMatchFilter simpleWhileMatch2 = new WhileMatchFilter(simpleWhileMatch1);
    Scan scan = new Scan(Bytes.toBytes(rowKeyPrefix));
    scan.setFilter(simpleWhileMatch2);

    int[] expected = {0, 1, 10, 11};
    assertWhileMatchFilterResult(qualA, table, scan, expected);
  }

  private Table addDataForWhileMatchFilterTest(String rowKeyPrefix, byte[] qualA)
      throws IOException {
    Table table = getConnection().getTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      String indexStr = String.valueOf(i);
      byte[] rowKey = Bytes.toBytes(rowKeyPrefix + indexStr);
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qualA, Bytes.toBytes(indexStr));
      table.put(put);
    }
    return table;
  }

  private void assertWhileMatchFilterResult(byte[] qualA, Table table, Scan scan, int[] expected)
      throws IOException {
    int[] actual = new int[expected.length];
    int i = 0;
    try (ResultScanner scanner = table.getScanner(scan)) {
      Iterator<Result> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        for (Cell cell : iterator.next().getColumnCells(COLUMN_FAMILY, qualA)) {
          int cellIntValue = Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell)));
          actual[i++] = cellIntValue;
        }
      }
    }
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testValueFilter() throws IOException {
    // Initialize
    int numGoodCols = 5;
    int numBadCols = 20;
    String goodValue = "includeThisValue";
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testRow-");
    Put put = new Put(rowKey);
    for (int i = 0; i < numBadCols; ++i) {
      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(""), Bytes.toBytes("someval"));
    }
    for (int i = 0; i < numGoodCols; ++i) {
      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(""), Bytes.toBytes(goodValue));
    }
    table.put(put);

    // Filter for results
    Filter filter = new ValueFilter(
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes(goodValue)));

    Get get = new Get(rowKey).setFilter(filter);
    Result result = table.get(get);
    Assert.assertEquals("Should only return good values", numGoodCols, result.size());
    Cell[] cells = result.rawCells();
    for (Cell cell : cells) {
      Assert.assertTrue("Should have good value",
          Bytes.toString(CellUtil.cloneValue(cell)).startsWith(goodValue));
    }

    table.close();
  }

  @Test
  public void testFirstKeyFilter() throws IOException {
    // Initialize
    int numCols = 5;
    String columnValue = "includeThisValue";
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testRow-");
    Put put = new Put(rowKey);
    for (int i = 0; i < numCols; ++i) {
      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(""), Bytes.toBytes(columnValue));
    }
    table.put(put);

    // Filter for results
    Filter filter = new FirstKeyOnlyFilter();

    Get get = new Get(rowKey).setFilter(filter);
    Result result = table.get(get);
    Assert.assertEquals("Should only return 1 keyvalue", 1, result.size());

    table.close();
  }

  @Test
  public void testKeyOnlyFilter() throws IOException {
    // Initialize
    int numCols = 5;
    String goodValue = "includeThisValue";
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testRow-");
    Put put = new Put(rowKey);
    for (int i = 0; i < numCols; ++i) {
      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(""), Bytes.toBytes(goodValue));
    }
    table.put(put);

    // Filter for results
    Filter filter = new KeyOnlyFilter();

    Get get = new Get(rowKey).setFilter(filter);
    Result result = table.get(get);
    Cell[] cells = result.rawCells();
    for (Cell cell : cells) {
      Assert.assertEquals(
          "Should NOT have a length.",
          0L,
          cell.getValueLength());
    }

    table.close();
  }

  @Test
  public void testMultipleColumnPrefixes() throws IOException {
    // Initialize
    String goodValue = "includeThisValue";
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testRow-");
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, dataHelper.randomData("a-"), Bytes.toBytes(goodValue));
    put.addColumn(COLUMN_FAMILY, dataHelper.randomData("b-"), Bytes.toBytes(goodValue));
    put.addColumn(COLUMN_FAMILY, dataHelper.randomData("c-"), Bytes.toBytes(goodValue));
    put.addColumn(COLUMN_FAMILY, dataHelper.randomData("d-"), Bytes.toBytes(goodValue));
    table.put(put);

    // Filter for results
    Filter filter = new MultipleColumnPrefixFilter(new byte[][]{
        Bytes.toBytes("a-"),
        Bytes.toBytes("b-")
    });

    Get get = new Get(rowKey).setFilter(filter);
    Result result = table.get(get);
    Cell[] cells = result.rawCells();
    Assert.assertEquals("Should have two cells, prefixes a- and b-.", 2, cells.length);
    byte[] qualifier0 = CellUtil.cloneQualifier(cells[0]);
    Assert.assertTrue("qualifier0 should start with a-",
        qualifier0[0] == 'a' && qualifier0[1] == '-');

    byte[] qualifier1 = CellUtil.cloneQualifier(cells[1]);
    Assert.assertTrue("qualifier1 should start with b-",
        qualifier1[0] == 'b' && qualifier1[1] == '-');

    table.close();
  }

  @Test
  public void testTimestampsFilter() throws IOException {
    // Initialize
    int numCols = 5;
    String goodValue = "includeThisValue";
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testRow-");
    Put put = new Put(rowKey);
    for (int i = 0; i < numCols; ++i) {
      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(""), i, Bytes.toBytes(goodValue));
    }
    table.put(put);

    // Filter for results
    Filter filter = new TimestampsFilter(ImmutableList.<Long>of(0L, 1L));

    Get get = new Get(rowKey).setFilter(filter);
    Result result = table.get(get);
    Cell[] cells = result.rawCells();
    Assert.assertEquals("Should have two cells, timestamps 0 and 1.", 2, cells.length);

    // Since the qualifiers are random, ignore the order of the returned cells.
    long[] timestamps = new long[]{cells[0].getTimestamp(), cells[1].getTimestamp()};
    Arrays.sort(timestamps);
    Assert.assertArrayEquals(new long[]{0L, 1L}, timestamps);

    table.close();
  }

  @Test
  public void testSingleColumnValueFilter() throws IOException {
    // Set up:
    // Row 1: f:qualifier1 = value1_1, f:qualifier2 = value2_1
    // Row 2: f:qualifier1 (ts1) = value1_1, f:qualifier1 (ts2) = value1_2

    // Cases to test:
    // a: Qualifier exists in the row and the value matches the latest version
    // b: Qualifier exists in the row and the value matches any version
    // c: Qualifier exists in the row and the value does NOT match
    // d: Qualifier does not exist in the row.

    byte[] rowKey1 = dataHelper.randomData("scvfrk1");
    byte[] rowKey2 = dataHelper.randomData("scvfrk2");
    byte[] qualifier1 = dataHelper.randomData("scvfq1");
    byte[] qualifier2 = dataHelper.randomData("scvfq2");
    byte[] value1_1 = dataHelper.randomData("val1.1");
    byte[] value1_2 = dataHelper.randomData("val1.2");
    byte[] value2_1 = dataHelper.randomData("val2.1");

    Table table = getConnection().getTable(TABLE_NAME);
    Put put = new Put(rowKey1);
    put.addColumn(COLUMN_FAMILY, qualifier1, value1_1);
    put.addColumn(COLUMN_FAMILY, qualifier2, value2_1);
    table.put(put);

    put = new Put(rowKey2);
    put.addColumn(COLUMN_FAMILY, qualifier1, 1L, value1_1);
    put.addColumn(COLUMN_FAMILY, qualifier1, 2L, value1_2);
    table.put(put);

    Result[] results;
    Scan scan = new Scan();
    scan.addColumn(COLUMN_FAMILY, qualifier1);
    scan.addColumn(COLUMN_FAMILY, qualifier2);

    // This is not intuitive. In order to get filter.setLatestVersionOnly to have an effect,
    // we must enable the scanner to see more versions:
    scan.setMaxVersions(3);

    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, qualifier1, CompareOp.EQUAL,  value1_1);
    filter.setFilterIfMissing(false);
    filter.setLatestVersionOnly(false);

    // a: Qualifier exists in the row and the value matches the latest version (row1)
    // b: Qualifier exists in the row and the value matches any version (row2)
    scan.setFilter(filter);
    results = table.getScanner(scan).next(10);
    Assert.assertEquals(2, results.length);

    // a: Qualifier exists in the row and the value matches the latest version (row1)
    filter.setLatestVersionOnly(true);
    scan.setFilter(filter);
    results = table.getScanner(scan).next(10);
    Assert.assertEquals(1, results.length);

    // a: Qualifier exists in the row and the value matches the latest version (row1)
    // d: Qualifier does not exist in the row: (row2)
    filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, qualifier2, CompareOp.EQUAL,  value2_1);
    filter.setFilterIfMissing(false);
    scan.setFilter(filter);
    results = table.getScanner(scan).next(10);
    Assert.assertEquals(2, results.length);

    // a: Qualifier exists in the row and the value matches the latest version (row1):
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    results = table.getScanner(scan).next(10);
    Assert.assertEquals(1, results.length);

    // Test qualifier exists and value never matches:
    // c: Qualifier exists in the row and the value does NOT match
    filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, qualifier2, CompareOp.EQUAL,  value1_1);
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    results = table.getScanner(scan).next(10);
    Assert.assertEquals(0, results.length);
  }

  @Test
  public void testRandomRowFilter() throws IOException {
    byte[][] rowKeys = dataHelper.randomData("trandA", 100);
    byte[] qualifier = dataHelper.randomData("trandq-");
    byte[] value = dataHelper.randomData("value-");
    Table table = getConnection().getTable(TABLE_NAME);

    List<Put> puts = new ArrayList<>();
    for (byte[] rowKey : rowKeys) {
      Put put = new Put(rowKey);
      put.addColumn(COLUMN_FAMILY, qualifier, value);
      puts.add(put);
    }
    table.put(puts);
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes("trandA"));
    scan.setStopRow(Bytes.toBytes("trandB"));
    RandomRowFilter filter = new RandomRowFilter(0.5f);
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    Result[] results = scanner.next(100);

    Assert.assertTrue(
        String.format("Using p=0.5, expected half of added rows, found %s", results.length),
        25 <= results.length && results.length <= 75);
  }

  @Test
  public void testSingleColumnValueExcludeFilter() throws IOException {
    byte[] rowKey1 = dataHelper.randomData("scvfrk1");
    byte[] qualifier1 = dataHelper.randomData("scvfq1");
    byte[] qualifier2 = dataHelper.randomData("scvfq2");
    byte[] value1_1 = dataHelper.randomData("val1.1");
    byte[] value2_1 = dataHelper.randomData("val2.1");

    Table table = getConnection().getTable(TABLE_NAME);
    Put put = new Put(rowKey1);
    put.addColumn(COLUMN_FAMILY, qualifier1, value1_1);
    put.addColumn(COLUMN_FAMILY, qualifier2, value2_1);
    table.put(put);

    Scan scan = new Scan();
    scan.addFamily(COLUMN_FAMILY);

    SingleColumnValueExcludeFilter excludeFilter =
        new SingleColumnValueExcludeFilter(COLUMN_FAMILY, qualifier1, CompareOp.EQUAL, value1_1);
    excludeFilter.setFilterIfMissing(true);
    excludeFilter.setLatestVersionOnly(false);

    scan.setFilter(excludeFilter);

    ResultScanner scanner = table.getScanner(scan);
    Result[] results = scanner.next(10);
    // Expect 1 row with value2_1 in qualifier2:
    Assert.assertEquals(1, results.length);
    Result result = results[0];
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qualifier2));
    Assert.assertFalse(result.containsColumn(COLUMN_FAMILY, qualifier1));
    Assert.assertArrayEquals(
        value2_1,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qualifier2)));
  }

  @Test
  public void testPrefixFilter() throws IOException {
    String prefix = "testPrefixFilter";
    int rowCount = 10;
    byte[][] rowKeys = dataHelper.randomData(prefix, rowCount);
    List<Put> puts = new ArrayList<>();
    for (byte[] rowKey : rowKeys) {
      puts.add(
          new Put(rowKey)
              .addColumn(COLUMN_FAMILY, Bytes.toBytes("q1"), Bytes.toBytes("val1")));
    }
    Table table = getConnection().getTable(TABLE_NAME);
    table.put(puts);

    PrefixFilter filter = new PrefixFilter(Bytes.toBytes(prefix));
    Scan scan = new Scan().addFamily(COLUMN_FAMILY).setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    Result[] results = scanner.next(rowCount + 2);
    Assert.assertEquals(rowCount, results.length);
    Arrays.sort(rowKeys, Bytes.BYTES_COMPARATOR);
    // Both results[] and rowKeys[] should be in the same order now. Iterate over both
    // and verify rowkeys.
    for (int i = 0; i < rowCount; i++) {
      Assert.assertArrayEquals(rowKeys[i], results[i].getRow());
    }
  }

  @Test
  public void testQualifierFilter() throws IOException {
    byte[] rowKey = dataHelper.randomData("testQaulifierFilter");
    byte[] qualA = dataHelper.randomData("qualA");
    byte[] qualAValue = dataHelper.randomData("qualA-value");
    byte[] qualB = dataHelper.randomData("qualB");
    byte[] qualBValue = dataHelper.randomData("qualB-value");
    byte[] qualC = dataHelper.randomData("qualC");
    byte[] qualCValue = dataHelper.randomData("qualC-value");
    Table table = getConnection().getTable(TABLE_NAME);
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, qualA, qualAValue);
    put.addColumn(COLUMN_FAMILY, qualB, qualBValue);
    put.addColumn(COLUMN_FAMILY, qualC, qualCValue);
    table.put(put);

    Get get = new Get(rowKey).addFamily(COLUMN_FAMILY);

    QualifierFilter equalsQualA =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(qualA));
    get.setFilter(equalsQualA);
    Result result = table.get(get);
    Assert.assertEquals(1, result.size());

    QualifierFilter greaterThanQualA =
        new QualifierFilter(CompareOp.GREATER, new BinaryComparator(qualA));
    get.setFilter(greaterThanQualA);
    result = table.get(get);
    Assert.assertEquals(2, result.size());

    QualifierFilter greaterThanEqualQualA =
        new QualifierFilter(
            CompareOp.GREATER_OR_EQUAL, new BinaryComparator(qualA));
    get.setFilter(greaterThanEqualQualA);
    result = table.get(get);
    Assert.assertEquals(3, result.size());

    QualifierFilter lessThanQualB =
        new QualifierFilter(CompareOp.LESS, new BinaryComparator(qualB));
    get.setFilter(lessThanQualB);
    result = table.get(get);
    Assert.assertEquals(1, result.size());

    QualifierFilter lessThanEqualQualB =
        new QualifierFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(qualB));
    get.setFilter(lessThanEqualQualB);
    result = table.get(get);
    Assert.assertEquals(2, result.size());

    QualifierFilter notEqualQualB =
        new QualifierFilter(CompareOp.NOT_EQUAL, new BinaryComparator(qualB));
    get.setFilter(notEqualQualB);
    result = table.get(get);
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(qualAValue, result.getValue(COLUMN_FAMILY, qualA));
    Assert.assertArrayEquals(qualCValue, result.getValue(COLUMN_FAMILY, qualC));

    // \\C* is not supported by Java regex.
    QualifierFilter regexQualFilter =
        new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("qualA.*"));
    get.setFilter(regexQualFilter);
    result = table.get(get);
    Assert.assertEquals(1, result.size());
  }

  @Test
  public void testPageFilters() throws IOException {
    byte[][] rowKeys = dataHelper.randomData("pageFilter-", 100);
    byte[] qualA = dataHelper.randomData("qualA");
    byte[] value = Bytes.toBytes("Important data goes here");
    Table table = getConnection().getTable(TABLE_NAME);
    for (byte[] rowKey : rowKeys) {
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qualA, value);
      table.put(put);
    }

    Scan scan = new Scan(Bytes.toBytes("pageFilter-"));

    PageFilter pageFilter = new PageFilter(20);
    scan.setFilter(pageFilter);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Assert.assertEquals(20,  Iterators.size(scanner.iterator()));
    }

    FilterList filterList = new FilterList(
        Operator.MUST_PASS_ALL,
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(qualA)),
        pageFilter);
    scan.setFilter(filterList);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Assert.assertEquals(20,  Iterators.size(scanner.iterator()));
    }
  }

  private Result[] scanWithFilter(Table t, byte[] startRow, byte[] endRow, byte[] qual,
      Filter f) throws IOException {
    Scan scan = new Scan(startRow, endRow).setFilter(f).addColumn(COLUMN_FAMILY, qual);
    ResultScanner scanner = t.getScanner(scan);
    Result[] results = scanner.next(10);
    return results;
  }
}
