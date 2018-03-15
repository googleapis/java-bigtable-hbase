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
import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY2;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
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
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
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
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.wrappers.Filters;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.hbase.filter.BigtableFilter;
import com.google.cloud.bigtable.hbase.filter.TimestampRangeFilter;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

public class TestFilters extends AbstractTest {
  /**
   * Requirement 9.1 - ColumnCountGetFilter - return first N columns on rows only
   */
  @Test
  public void testColumnCountGetFilter() throws Exception {
    // Initialize data
    int numColumns = 20;
    int numColumnsToFilter = 10;
    Table table = getTable();
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
    Get get = new Get(rowKey).setFilter(filter).readVersions(10);
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

  private Table getTable() throws IOException {
    return getDefaultTable();
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
    Table table = getTable();
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
    Get get = new Get(rowKey).setFilter(filter).readVersions(10);
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
    Table table = getTable();
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
    Get get = new Get(rowKey).setFilter(filter).readVersions(10);
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
    Table table = getTable();
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
    Table table = getTable();
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
   * Insert 6 cols: A, AA, B, BB, C, CC.  Test filtering on these columns with different
   * combinations of start/end keys being inclusive/exclusive.
   */
  @Test
  public void testColumnRangeFilter() throws Exception {
    // Initialize
    Table table = getTable();
    byte[] rowKey = dataHelper.randomData("testRow-");
    final byte[] value = Bytes.toBytes("someval");
    table.put(new Put(rowKey)
        .addColumn(COLUMN_FAMILY, Bytes.toBytes("A"), value)
        .addColumn(COLUMN_FAMILY, Bytes.toBytes("AA"), value)
        .addColumn(COLUMN_FAMILY, Bytes.toBytes("B"), value)
        .addColumn(COLUMN_FAMILY, Bytes.toBytes("BB"), value)
        .addColumn(COLUMN_FAMILY, Bytes.toBytes("C"), value)
        .addColumn(COLUMN_FAMILY, Bytes.toBytes("CC"), value));

    // Filter for "B" exclusive, "C" exclusive
    Filter filter = new ColumnRangeFilter(Bytes.toBytes("B"), false, Bytes.toBytes("C"), false);
    testColumnRangeFilterCells(table, rowKey, filter, "BB");

    // Filter for "B" exclusive, "C" inclusive
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), false, Bytes.toBytes("C"), true);
    testColumnRangeFilterCells(table, rowKey, filter, "BB", "C");

    // Filter for "B" inclusive, "C" exclusive
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), true, Bytes.toBytes("C"), false);
    testColumnRangeFilterCells(table, rowKey, filter, "B", "BB");

    // Filter for "B" inclusive, "C" inclusive
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), true, Bytes.toBytes("C"), true);
    testColumnRangeFilterCells(table, rowKey, filter, "B", "BB", "C");

    // Filter for "B" inclusive, until the end.
    filter = new ColumnRangeFilter(Bytes.toBytes("B"), true, null, true);
    testColumnRangeFilterCells(table, rowKey, filter, "B", "BB", "C", "CC");

    // Filter for all until "BB"
    filter = new ColumnRangeFilter(null, true, Bytes.toBytes("BB"), true);
    testColumnRangeFilterCells(table, rowKey, filter, "A", "AA", "B", "BB");

    table.close();
  }

  private static void testColumnRangeFilterCells(Table table, byte[] rowKey, Filter filter,
      String... columnQualifiers) throws IOException {
    Get get = new Get(rowKey).setFilter(filter).addFamily(COLUMN_FAMILY);
    Result result = table.get(get);
    Assert.assertEquals("Should return " + concat(columnQualifiers), columnQualifiers.length,
      result.size());
    for (int i = 0; i < columnQualifiers.length; i++) {
      String qualifier = columnQualifiers[i];
      Assert.assertEquals(qualifier, Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[i])));
    }
  }

  private static String concat(String... columnQualifiers) {
    StringBuilder sb = new StringBuilder();
    String prepend = "";
    for (String qualifier : columnQualifiers) {
      sb.append(prepend).append('"').append(qualifier).append('"');
    }
    return sb.toString();
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
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowKey2Comparable);
    Result[] results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKey2, results[0].getRow());

    // Test BinaryComparator - GREATER
    filter = new RowFilter(CompareOperator.GREATER, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKey3, results[0].getRow());

    // Test BinaryComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowKey2, results[0].getRow());
    Assert.assertArrayEquals(rowKey3, results[1].getRow());

    // Test BinaryComparator - LESS
    filter = new RowFilter(CompareOperator.LESS, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKey1, results[0].getRow());

    // Test BinaryComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowKey1, results[0].getRow());
    Assert.assertArrayEquals(rowKey2, results[1].getRow());

    // Test BinaryComparator - NOT_EQUAL
    filter = new RowFilter(CompareOperator.NOT_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowKey1, results[0].getRow());
    Assert.assertArrayEquals(rowKey3, results[1].getRow());

    // Test BinaryComparator - NO_OP
    filter = new RowFilter(CompareOperator.NO_OP, rowKey2Comparable);
    results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }


  @Test
  public void testRowFilterBinaryComparator_Equals() throws Exception {
    // Initialize data
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowKey2Comparable);
    Result[] results = scanWithFilter(table, rowKey1, rowKey4, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKey2, results[0].getRow());
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
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowBComparable);
    Result[] results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowB, results[0].getRow());
    Assert.assertArrayEquals(rowBB, results[1].getRow());

    // Test BinaryPrefixComparator - GREATER
    filter = new RowFilter(CompareOperator.GREATER, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowC, results[0].getRow());
    Assert.assertArrayEquals(rowCC, results[1].getRow());

    // Test BinaryPrefixComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowB, results[0].getRow());
    Assert.assertArrayEquals(rowBB, results[1].getRow());
    Assert.assertArrayEquals(rowC, results[2].getRow());
    Assert.assertArrayEquals(rowCC, results[3].getRow());

    // Test BinaryPrefixComparator - LESS
    filter = new RowFilter(CompareOperator.LESS, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());
    Assert.assertArrayEquals(rowAA, results[1].getRow());

    // Test BinaryPrefixComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());
    Assert.assertArrayEquals(rowAA, results[1].getRow());
    Assert.assertArrayEquals(rowB, results[2].getRow());
    Assert.assertArrayEquals(rowBB, results[3].getRow());

    // Test BinaryPrefixComparator - NOT_EQUAL
    filter = new RowFilter(CompareOperator.NOT_EQUAL, rowBComparable);
    results = scanWithFilter(table, rowA, rowD, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());
    Assert.assertArrayEquals(rowAA, results[1].getRow());
    Assert.assertArrayEquals(rowC, results[2].getRow());
    Assert.assertArrayEquals(rowCC, results[3].getRow());

    // Test BinaryPrefixComparator - NO_OP
    filter = new RowFilter(CompareOperator.NO_OP, rowBComparable);
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
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowBComparable);
    Result[] results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1010, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1111, results[2].getRow());

    // Test BitComparator - XOR - GREATER (effectively no values)
    filter = new RowFilter(CompareOperator.GREATER, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BitComparator - XOR - GREATER_OR_EQUAL (same effect as EQUAL)
    filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1010, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1111, results[2].getRow());

    // Test BitComparator - XOR - LESS (same effect as NOT_EQUAL)
    filter = new RowFilter(CompareOperator.LESS, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0101, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), rowDiffLength, results[1].getRow());

    // Test BitComparator - XOR - LESS_OR_EQUAL (effectively all values)
    filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 5, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), rowDiffLength, results[3].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[4].getRow()), row1111, results[4].getRow());

    // Test BitComparator - XOR - NOT_EQUAL (same effect as LESS)
    filter = new RowFilter(CompareOperator.NOT_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0101, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), rowDiffLength, results[1].getRow());

    // Test BitComparator - XOR - NO_OP (no values)
    filter = new RowFilter(CompareOperator.NO_OP, rowBComparable);
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
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowBComparable);
    Result[] results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0101, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1111, results[1].getRow());

    // Test BitComparator - AND - GREATER (effectively no values)
    filter = new RowFilter(CompareOperator.GREATER, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BitComparator - AND - GREATER_OR_EQUAL (same effect as EQUAL)
    filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 2, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0101, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1111, results[1].getRow());

    // Test BitComparator - AND - LESS (same effect as NOT_EQUAL)
    filter = new RowFilter(CompareOperator.LESS, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1010, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), rowDiffLength, results[2].getRow());

    // Test BitComparator - AND - LESS_OR_EQUAL (effectively all values)
    filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 5, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), rowDiffLength, results[3].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[4].getRow()), row1111, results[4].getRow());

    // Test BitComparator - AND - NOT_EQUAL (same effect as LESS)
    filter = new RowFilter(CompareOperator.NOT_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row1010, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), rowDiffLength, results[2].getRow());

    // Test BitComparator - AND - NO_OP (no values)
    filter = new RowFilter(CompareOperator.NO_OP, rowBComparable);
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
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowBComparable);
    Result[] results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), row1111, results[3].getRow());

    // Test BitComparator - OR - GREATER (effectively no values)
    filter = new RowFilter(CompareOperator.GREATER, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BitComparator - OR - GREATER_OR_EQUAL (same effect as EQUAL)
    filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), row1111, results[3].getRow());

    // Test BitComparator - OR - LESS (same effect as NOT_EQUAL)
    filter = new RowFilter(CompareOperator.LESS, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), rowDiffLength, results[0].getRow());

    // Test BitComparator - OR - LESS_OR_EQUAL (effectively all values)
    filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 5, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), row0000, results[0].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[1].getRow()), row0101, results[1].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[2].getRow()), row1010, results[2].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[3].getRow()), rowDiffLength, results[3].getRow());
    Assert.assertArrayEquals(Bytes.toHex(results[4].getRow()), row1111, results[4].getRow());

    // Test BitComparator - OR - NOT_EQUAL (same effect as LESS)
    filter = new RowFilter(CompareOperator.NOT_EQUAL, rowBComparable);
    results = scanWithFilter(table, row0000, rowMax, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(Bytes.toHex(results[0].getRow()), rowDiffLength, results[0].getRow());

    // Test BitComparator - OR - NO_OP (no values)
    filter = new RowFilter(CompareOperator.NO_OP, rowBComparable);
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
    Table table = getTable();
    String rowKeyPrefix = "testRowFilter-" + RandomStringUtils.randomAlphabetic(10);
    byte[] rowKeyA = Bytes.toBytes(rowKeyPrefix + "A");
    byte[] rowKeyB = Bytes.toBytes(rowKeyPrefix + "B");
    byte[] qual = dataHelper.randomData("testqual");
    byte[] value = Bytes.toBytes("testvalue");
    Put put = new Put(rowKeyA).addColumn(COLUMN_FAMILY, qual, value);
    table.put(put);

    // Test BinaryComparator - EQUAL
    ByteArrayComparable nullComparator = new NullComparator();
    Filter filter = new RowFilter(CompareOperator.EQUAL, nullComparator);
    Result[] results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BinaryComparator - GREATER
    filter = new RowFilter(CompareOperator.GREATER, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BinaryComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test BinaryComparator - LESS
    filter = new RowFilter(CompareOperator.LESS, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKeyA, results[0].getRow());

    // Test BinaryComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKeyA, results[0].getRow());

    // Test BinaryComparator - NOT_EQUAL
    filter = new RowFilter(CompareOperator.NOT_EQUAL, nullComparator);
    results = scanWithFilter(table, rowKeyA, rowKeyB, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowKeyA, results[0].getRow());

    // Test BinaryComparator - NO_OP
    filter = new RowFilter(CompareOperator.NO_OP, nullComparator);
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
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowKey2Comparable);
    Result[] results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowAB, results[0].getRow());
    Assert.assertArrayEquals(rowAbC, results[1].getRow());
    Assert.assertArrayEquals(rowDaB, results[2].getRow());
    Assert.assertArrayEquals(rowDabE, results[3].getRow());

    // Test SubstringComparator - GREATER
    filter = new RowFilter(CompareOperator.GREATER, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test SubstringComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(rowAB, results[0].getRow());
    Assert.assertArrayEquals(rowAbC, results[1].getRow());
    Assert.assertArrayEquals(rowDaB, results[2].getRow());
    Assert.assertArrayEquals(rowDabE, results[3].getRow());

    // Test SubstringComparator - LESS
    filter = new RowFilter(CompareOperator.LESS, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());

    // Test SubstringComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 5, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());
    Assert.assertArrayEquals(rowAB, results[1].getRow());
    Assert.assertArrayEquals(rowAbC, results[2].getRow());
    Assert.assertArrayEquals(rowDaB, results[3].getRow());
    Assert.assertArrayEquals(rowDabE, results[4].getRow());

    // Test SubstringComparator - NOT_EQUAL
    filter = new RowFilter(CompareOperator.NOT_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, rowA, rowZ, qual, filter);
    Assert.assertEquals("# results", 1, results.length);
    Assert.assertArrayEquals(rowA, results[0].getRow());

    // Test SubstringComparator - NO_OP
    filter = new RowFilter(CompareOperator.NO_OP, rowKey2Comparable);
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
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowKey2Comparable);
    Result[] results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(rowGoodIP1, results[0].getRow());
    Assert.assertArrayEquals(rowGoodIP2, results[1].getRow());
    Assert.assertArrayEquals(rowGoodIPv6, results[2].getRow());

    // Test RegexStringComparator - NOT_EQUAL
    filter = new RowFilter(CompareOperator.NOT_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(row0, results[0].getRow());
    Assert.assertArrayEquals(rowTelephone, results[1].getRow());
    Assert.assertArrayEquals(rowBadIP, results[2].getRow());
    Assert.assertArrayEquals(rowRandom, results[3].getRow());

    // Test RegexStringComparator - GREATER
    filter = new RowFilter(CompareOperator.GREATER, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    // Test RegexStringComparator - GREATER_OR_EQUAL
    filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(rowGoodIP1, results[0].getRow());
    Assert.assertArrayEquals(rowGoodIP2, results[1].getRow());
    Assert.assertArrayEquals(rowGoodIPv6, results[2].getRow());

    // Test RegexStringComparator - LESS
    filter = new RowFilter(CompareOperator.LESS, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 4, results.length);
    Assert.assertArrayEquals(row0, results[0].getRow());
    Assert.assertArrayEquals(rowTelephone, results[1].getRow());
    Assert.assertArrayEquals(rowBadIP, results[2].getRow());
    Assert.assertArrayEquals(rowRandom, results[3].getRow());

    // Test RegexStringComparator - LESS_OR_EQUAL
    filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, rowKey2Comparable);
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
    filter = new RowFilter(CompareOperator.NO_OP, rowKey2Comparable);
    results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 0, results.length);

    table.close();
  }

  @Test
  public void testRowFilterRegexStringComparator_Equals() throws Exception {
    // Initialize data
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowKey2Comparable);
    Result[] results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(rowGoodIP1, results[0].getRow());
    Assert.assertArrayEquals(rowGoodIP2, results[1].getRow());
    Assert.assertArrayEquals(rowGoodIPv6, results[2].getRow());
  }

  /**
   * Test RowFilter with a RegexStringComparator and EQUAL comparator.
   * @throws IOException
   */
  @Test
  @Category(KnownGap.class)
  public void testDeterministRowRegexFilter() throws IOException {
    // Initialize data
    Table table = getTable();
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
    Filter filter = new RowFilter(CompareOperator.EQUAL, rowKey2Comparable);
    Result[] results = scanWithFilter(table, row0, endRow, qual, filter);
    Assert.assertEquals("# results", 3, results.length);
    Assert.assertArrayEquals(rowGoodIP1, results[0].getRow());
    Assert.assertArrayEquals(rowGoodIP2, results[1].getRow());
    Assert.assertArrayEquals(rowGoodIPv6, results[2].getRow());

    table.close();
  }

  @Test
  public void testWhileMatchFilter_simple() throws IOException {
    String rowKeyPrefix = dataHelper.randomString("wmf-simple-");
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForTesting(rowKeyPrefix, qualA);

    ByteArrayComparable rowValue2Comparable = new BinaryComparator(Bytes.toBytes("12"));
    ValueFilter valueFilter =
        new ValueFilter(CompareOperator.NOT_EQUAL, rowValue2Comparable);
    WhileMatchFilter simpleWhileMatch = new WhileMatchFilter(valueFilter);
    Scan scan = new Scan(Bytes.toBytes(rowKeyPrefix));
    scan.setFilter(simpleWhileMatch);

    int[] expected = {0, 1, 10, 11};
    assertWhileMatchFilterResult(qualA, table, scan, expected);
  }

  @Test
  public void testWhileMatchFilter_singleChained() throws IOException {
    String rowKeyPrefix = dataHelper.randomString("wmf-sc-");
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForTesting(rowKeyPrefix, qualA);

    ByteArrayComparable valueComparable = new BinaryComparator(String.valueOf(2).getBytes());
    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
        COLUMN_FAMILY, qualA, CompareOperator.NOT_EQUAL, valueComparable);
    WhileMatchFilter simpleWhileMatch = new WhileMatchFilter(valueFilter);
    ColumnPrefixFilter prefixFilter = new ColumnPrefixFilter(Bytes.toBytes("qua"));
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, simpleWhileMatch, prefixFilter);
    Scan scan = new Scan();
    scan.setFilter(filterList);

    int[] expected = {0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
    assertWhileMatchFilterResult(qualA, table, scan, expected);
  }

  @Test
  public void testWhileMatchFilter_withUpdate() throws IOException {
    String rowKeyPrefix = dataHelper.randomString("wmf-wu-");
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForTesting(rowKeyPrefix, qualA);

    table.put(new Put(Bytes.toBytes(rowKeyPrefix + "14")).addColumn(COLUMN_FAMILY, qualA,
      Bytes.toBytes(String.valueOf(2))));

    ByteArrayComparable valueComparable = new BinaryComparator(String.valueOf(2).getBytes());
    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
        COLUMN_FAMILY, qualA, CompareOperator.NOT_EQUAL, valueComparable);
    Scan scan = new Scan().setFilter(new WhileMatchFilter(valueFilter));

    int[] expected = {0, 1, 10, 11, 12, 13};
    assertWhileMatchFilterResult(qualA, table, scan, expected);
  }

  @Test
  public void testSingleValueFilterAscii() throws IOException {
    byte[] qual = dataHelper.randomData("testSingleValueFilterCompOps");
    // Add {, }, and @ to make sure that they do not need to be encoded
    byte[] rowKeyA = dataHelper.randomData("KeyA{");
    byte[] rowKeyB = dataHelper.randomData("KeyB}");
    byte[] rowKeyC = dataHelper.randomData("KeyC@");
    byte[] valueA = dataHelper.randomData("ValueA{");
    byte[] valueB = dataHelper.randomData("ValueB}");
    byte[] valueC = dataHelper.randomData("ValueC@");
    try (Table table = getTable()) {
      table.put(Arrays.asList(
        new Put(rowKeyA).addColumn(COLUMN_FAMILY, qual, valueA),
        new Put(rowKeyB).addColumn(COLUMN_FAMILY, qual, valueB),
        new Put(rowKeyC).addColumn(COLUMN_FAMILY, qual, valueC)));

      // {A} == A
      assertKeysReturnedForSCVF(table, qual, CompareOperator.EQUAL, valueA, rowKeyA);
      // Nothing should match this.
      assertKeysReturnedForSCVF(table, qual, CompareOperator.EQUAL, Bytes.toBytes("ValueA*"));
      // {B, C} > A
      assertKeysReturnedForSCVF(table, qual, CompareOperator.GREATER, valueA, rowKeyB, rowKeyC);
      // {A, B, C} >= A
      assertKeysReturnedForSCVF(table, qual, CompareOperator.GREATER_OR_EQUAL, valueA, rowKeyA, rowKeyB,
        rowKeyC);
      // {A} < B
      assertKeysReturnedForSCVF(table, qual, CompareOperator.LESS, valueB, rowKeyA);
      // {A} <= A
      assertKeysReturnedForSCVF(table, qual, CompareOperator.LESS_OR_EQUAL, valueA, rowKeyA);
      // {A, B} <= B
      assertKeysReturnedForSCVF(table, qual, CompareOperator.LESS_OR_EQUAL, valueB, rowKeyA, rowKeyB);
      // {A, C} != B
      assertKeysReturnedForSCVF(table, qual, CompareOperator.NOT_EQUAL, valueB, rowKeyA, rowKeyC);

      // Check to make sure that EQUALS doesn't actually do a regex filter.
      assertKeysReturnedForSCVF(table, qual, CompareOperator.EQUAL, Bytes.toBytes("ValueC.*"));

      // Check to make sure that EQUALS with regex does work
      SingleColumnValueFilter filter =
          new SingleColumnValueFilter(COLUMN_FAMILY, qual, CompareOperator.EQUAL,
              new RegexStringComparator("ValueC.*"));
      filter.setFilterIfMissing(true);
      assertKeysReturnedForFilter(table, filter, rowKeyC);
    }
  }

  @Test
  public void testSingleValueFilterEmpty() throws IOException {
    byte[] qual = dataHelper.randomData("testSingleValueFilterEmpty");
    // Add {, }, and @ to make sure that they do not need to be encoded
    byte[] rowKey = dataHelper.randomData("Empty");
    byte[] empty = new byte[0];
    try (Table table = getTable()) {
      table.put(new Put(rowKey).addColumn(COLUMN_FAMILY, qual, empty));
      assertKeysReturnedForSCVF(table, qual, CompareOperator.EQUAL, empty, rowKey);
    }
  }

  private void assertKeysReturnedForSCVF(Table table, byte[] qualifier, CompareOperator operator,
      byte[] value, byte[]... expectedKeys) throws IOException {
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, qualifier, operator,
            new BinaryComparator(value));
    filter.setFilterIfMissing(true);
    assertKeysReturnedForFilter(table, filter, expectedKeys);
  }

  private void assertKeysReturnedForFilter(Table table, Filter filter, byte[]... expectedKeys)
      throws IOException {
    Set<String> expected = new TreeSet<>();
    for (byte[] expectedKey : expectedKeys) {
      expected.add(Bytes.toString(expectedKey));
    }
    Set<String> found = new TreeSet<>();
    Scan scan = new Scan().setFilter(filter);
    try (ResultScanner result = table.getScanner(scan)) {
      for (Result curr : result) {
        found.add(Bytes.toString(curr.getRow()));
      }
    }
    Assert.assertEquals(expected, found);
  }

  @Test
  @Category(KnownGap.class)
  public void testWhileMatchFilter_twoInterleaves() throws IOException {
    String rowKeyPrefix = dataHelper.randomString("wmf-interleaves-");
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForTesting(rowKeyPrefix, qualA);

    ByteArrayComparable rowValue2Comparable1 = new BinaryComparator(Bytes.toBytes("12"));
    ValueFilter valueFilter1 =
        new ValueFilter(CompareOperator.NOT_EQUAL, rowValue2Comparable1);
    WhileMatchFilter simpleWhileMatch1 = new WhileMatchFilter(valueFilter1);
    ByteArrayComparable rowValue2Comparable2 = new BinaryComparator(Bytes.toBytes("15"));
    ValueFilter valueFilter2 =
        new ValueFilter(CompareOperator.NOT_EQUAL, rowValue2Comparable2);
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
    String rowKeyPrefix = dataHelper.randomString("wmf-chained-");
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForTesting(rowKeyPrefix, qualA);

    ByteArrayComparable rowValue2Comparable1 = new BinaryComparator(Bytes.toBytes("12"));
    ValueFilter valueFilter1 =
        new ValueFilter(CompareOperator.NOT_EQUAL, rowValue2Comparable1);
    WhileMatchFilter simpleWhileMatch1 = new WhileMatchFilter(valueFilter1);
    ByteArrayComparable rowValue2Comparable2 = new BinaryComparator(Bytes.toBytes("15"));
    ValueFilter valueFilter2 =
        new ValueFilter(CompareOperator.NOT_EQUAL, rowValue2Comparable2);
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
    String rowKeyPrefix = dataHelper.randomString("wmf-nested-");
    byte[] qualA = dataHelper.randomData("qualA");
    Table table = addDataForTesting(rowKeyPrefix, qualA);

    ByteArrayComparable rowValue2Comparable1 = new BinaryComparator(Bytes.toBytes("12"));
    ValueFilter valueFilter1 =
        new ValueFilter(CompareOperator.NOT_EQUAL, rowValue2Comparable1);
    WhileMatchFilter simpleWhileMatch1 = new WhileMatchFilter(valueFilter1);
    WhileMatchFilter simpleWhileMatch2 = new WhileMatchFilter(simpleWhileMatch1);
    Scan scan = new Scan(Bytes.toBytes(rowKeyPrefix));
    scan.setFilter(simpleWhileMatch2);

    int[] expected = {0, 1, 10, 11};
    assertWhileMatchFilterResult(qualA, table, scan, expected);
  }

  private void assertWhileMatchFilterResult(byte[] qualA, Table table, Scan scan, int[] expected)
      throws IOException {
    int[] actual = new int[expected.length];
    int i = 0;
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result r : scanner) {
        List<Cell> cells = r.getColumnCells(COLUMN_FAMILY, qualA);
        if (!cells.isEmpty()) {
          Assert.assertEquals("Expected 1 result, but got " + cells.size(), 1, cells.size());
          if (i < expected.length) {
            actual[i] = Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cells.get(0))));
          }
          i++;
        }
      }
    }
    Assert.assertEquals(expected.length, i);
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testValueFilter() throws IOException {
    // Initialize
    int numGoodCols = 5;
    int numBadCols = 20;
    String goodValue = "includeThisValue";
    Table table = getTable();
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
        CompareOperator.EQUAL,
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
    Table table = getTable();
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
    Table table = getTable();
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
    Table table = getTable();
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
    Table table = getTable();
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
  public void testTimestampRangeFilter() throws IOException {
    // Initialize
    int numCols = 10;
    String goodValue = "includeThisValue";
    Table table = getTable();
    byte[] rowKey = dataHelper.randomData("testRow-TimestampRange-");
    Put put = new Put(rowKey);
    for (int i = 0; i < numCols; ++i) {
      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(""), i, Bytes.toBytes(goodValue));
    }
    table.put(put);

    // Filter for results
    Filter filter = new TimestampRangeFilter(4, 6);

    Get get = new Get(rowKey).setFilter(filter);
    Result result = table.get(get);
    Cell[] cells = result.rawCells();
    Assert.assertEquals("Should have three cells, timestamps 4 and 5.", 2, cells.length);

    // Since the qualifiers are random, ignore the order of the returned cells.
    long[] timestamps =
        new long[] { cells[0].getTimestamp(), cells[1].getTimestamp() };
    Arrays.sort(timestamps);
    Assert.assertArrayEquals(new long[] { 4L, 5L }, timestamps);

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

    Table table = getTable();
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
    scan.readVersions(3);

    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, qualifier1, CompareOperator.EQUAL,  value1_1);
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
        new SingleColumnValueFilter(COLUMN_FAMILY, qualifier2, CompareOperator.EQUAL,  value2_1);
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
        new SingleColumnValueFilter(COLUMN_FAMILY, qualifier2, CompareOperator.EQUAL,  value1_1);
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
    Table table = getTable();

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
  public void testSingleValueLongCompares() throws IOException {
    byte[] rowKey = dataHelper.randomData("rowKeyNumeric-");
    byte[] qualToCheck = dataHelper.randomData("toCheckNumeric-");

    Table table = getDefaultTable();

    table.put(new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      Bytes.toBytes(2000l)));

    Scan rootScan = new Scan()
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck)
        .setStartRow(rowKey).setStopRow(rowKey);

    Assert.assertNull("< 1000 should fail",
      getFirst(table, rootScan, CompareOperator.LESS, 1000l));
    Assert.assertNotNull("> 1000 should succeed",
      getFirst(table, rootScan, CompareOperator.GREATER, 1000l));
    Assert.assertNull("<= 1000 should fail",
      getFirst(table, rootScan, CompareOperator.LESS_OR_EQUAL, 1000l));
    Assert.assertNotNull(">= 1000 should succeed",
      getFirst(table, rootScan, CompareOperator.GREATER_OR_EQUAL, 1000l));
    Assert.assertNotNull("<= 2000 should succeed",
      getFirst(table, rootScan, CompareOperator.LESS_OR_EQUAL, 2000l));
    Assert.assertNotNull(">= 2000 should succeed",
      getFirst(table, rootScan, CompareOperator.GREATER_OR_EQUAL, 2000l));
  }

  protected Result getFirst(Table table, Scan rootScan, CompareOperator comparitor, long value)
      throws IOException {
    try (ResultScanner results = table.getScanner(new Scan(rootScan)
        .setFilter(new ValueFilter(comparitor, new BinaryComparator(Bytes.toBytes(value)))))) {
      return results.next();
    }
  }
  @Test
  public void testSingleColumnValueExcludeFilter() throws IOException {
    byte[] rowKey1 = dataHelper.randomData("scvfrk1");
    byte[] qualifier1 = dataHelper.randomData("scvfq1");
    byte[] qualifier2 = dataHelper.randomData("scvfq2");
    byte[] value1_1 = dataHelper.randomData("val1.1");
    byte[] value2_1 = dataHelper.randomData("val2.1");

    Table table = getTable();
    Put put = new Put(rowKey1);
    put.addColumn(COLUMN_FAMILY, qualifier1, value1_1);
    put.addColumn(COLUMN_FAMILY, qualifier2, value2_1);
    table.put(put);

    Scan scan = new Scan();
    scan.addFamily(COLUMN_FAMILY);

    SingleColumnValueExcludeFilter excludeFilter =
        new SingleColumnValueExcludeFilter(COLUMN_FAMILY, qualifier1, CompareOperator.EQUAL, value1_1);
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
    Arrays.sort(rowKeys, Bytes.BYTES_COMPARATOR);
    List<Put> puts = new ArrayList<>();
    for (byte[] rowKey : rowKeys) {
      puts.add(
          new Put(rowKey)
              .addColumn(COLUMN_FAMILY, Bytes.toBytes("q1"), Bytes.toBytes("val1")));
    }
    Table table = getTable();
    table.put(puts);

    PrefixFilter filter = new PrefixFilter(Bytes.toBytes(prefix));
    Scan scan = new Scan().addFamily(COLUMN_FAMILY).setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    Result[] results = scanner.next(rowCount + 2);
    Assert.assertEquals(rowCount, results.length);

    // Both results[] and rowKeys[] should be in the same order now. Iterate over both
    // and verify rowkeys.
    for (int i = 0; i < rowCount; i++) {
      Assert.assertArrayEquals(rowKeys[i], results[i].getRow());
    }

    // Make sure that it works with start & end rows: exclude first & last row
    Scan boundedScan = new Scan().addFamily(COLUMN_FAMILY).setFilter(filter)
        .setStartRow(rowKeys[1])
        .setStopRow(rowKeys[rowKeys.length - 1]);

    ResultScanner boundedScanner = table.getScanner(boundedScan);
    Result[] boundedResults = boundedScanner.next(rowCount + 2);
    Assert.assertEquals(rowCount - 2, boundedResults.length);
    for(int i=0; i < rowCount - 2; i++) {
      Assert.assertArrayEquals(rowKeys[i+1], boundedResults[i].getRow());
    }
  }

  @Test
  public void testMultiRangeFilter() throws IOException {
    String prefix = "testMultiRangeFilter_";
    int rowCount = 10;
    byte[][] rowKeys = dataHelper.randomData(prefix, rowCount);
    Arrays.sort(rowKeys, Bytes.BYTES_COMPARATOR);
    List<Put> puts = new ArrayList<>();
    for (byte[] rowKey : rowKeys) {
      puts.add(
          new Put(rowKey)
              .addColumn(COLUMN_FAMILY, Bytes.toBytes("q1"), Bytes.toBytes("val1")));
    }
    Table table = getTable();
    table.put(puts);

    MultiRowRangeFilter filter = new MultiRowRangeFilter(Arrays.asList(
        // rows 1 & 2
        new RowRange(rowKeys[1], true, rowKeys[3], false),
        // rows 6 & 7
        new RowRange(rowKeys[5], false, rowKeys[7], true)
    ));

    Scan scan = new Scan().addFamily(COLUMN_FAMILY).setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    Result[] results = scanner.next(rowCount + 2);
    Assert.assertEquals(4, results.length);

    // first range: rows 1 & 2
    Assert.assertArrayEquals(rowKeys[1], results[0].getRow());
    Assert.assertArrayEquals(rowKeys[2], results[1].getRow());
    // second range: rows 6 & 7
    Assert.assertArrayEquals(rowKeys[6], results[2].getRow());
    Assert.assertArrayEquals(rowKeys[7], results[3].getRow());
  }

  @Test
  public void testMultiRangeFilterOrList() throws IOException {
    String prefix = "testMultiRangeFilterOrList_";
    int rowCount = 10;
    byte[][] rowKeys = dataHelper.randomData(prefix, rowCount);
    Arrays.sort(rowKeys, Bytes.BYTES_COMPARATOR);
    List<Put> puts = new ArrayList<>();
    for (byte[] rowKey : rowKeys) {
      puts.add(
          new Put(rowKey)
              .addColumn(COLUMN_FAMILY, Bytes.toBytes("q1"), Bytes.toBytes("val1")));
    }
    Table table = getTable();
    table.put(puts);

    MultiRowRangeFilter rangeFilter = new MultiRowRangeFilter(Arrays.asList(
        // rows 1 & 2
        new RowRange(rowKeys[1], true, rowKeys[3], false)
    ));

    PrefixFilter prefixFilter = new PrefixFilter(rowKeys[8]);

    FilterList filterList = new FilterList(Operator.MUST_PASS_ONE, rangeFilter, prefixFilter);

    Scan scan = new Scan().addFamily(COLUMN_FAMILY).setFilter(filterList);
    ResultScanner scanner = table.getScanner(scan);
    Result[] results = scanner.next(rowCount + 2);
    Assert.assertEquals(3, results.length);

    // first range: rows 1 & 2
    Assert.assertArrayEquals(rowKeys[1], results[0].getRow());
    Assert.assertArrayEquals(rowKeys[2], results[1].getRow());
    // second range: rows 9
    Assert.assertArrayEquals(rowKeys[8], results[2].getRow());
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
    Table table = getTable();
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, qualA, qualAValue);
    put.addColumn(COLUMN_FAMILY, qualB, qualBValue);
    put.addColumn(COLUMN_FAMILY, qualC, qualCValue);
    table.put(put);

    Get get = new Get(rowKey).addFamily(COLUMN_FAMILY);

    QualifierFilter equalsQualA =
        new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(qualA));
    get.setFilter(equalsQualA);
    Result result = table.get(get);
    Assert.assertEquals(1, result.size());

    QualifierFilter greaterThanQualA =
        new QualifierFilter(CompareOperator.GREATER, new BinaryComparator(qualA));
    get.setFilter(greaterThanQualA);
    result = table.get(get);
    Assert.assertEquals(2, result.size());

    QualifierFilter greaterThanEqualQualA =
        new QualifierFilter(
            CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(qualA));
    get.setFilter(greaterThanEqualQualA);
    result = table.get(get);
    Assert.assertEquals(3, result.size());

    QualifierFilter lessThanQualB =
        new QualifierFilter(CompareOperator.LESS, new BinaryComparator(qualB));
    get.setFilter(lessThanQualB);
    result = table.get(get);
    Assert.assertEquals(1, result.size());

    QualifierFilter lessThanEqualQualB =
        new QualifierFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(qualB));
    get.setFilter(lessThanEqualQualB);
    result = table.get(get);
    Assert.assertEquals(2, result.size());

    QualifierFilter notEqualQualB =
        new QualifierFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(qualB));
    get.setFilter(notEqualQualB);
    result = table.get(get);
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(qualAValue, result.getValue(COLUMN_FAMILY, qualA));
    Assert.assertArrayEquals(qualCValue, result.getValue(COLUMN_FAMILY, qualC));

    // \\C* is not supported by Java regex.
    QualifierFilter regexQualFilter =
        new QualifierFilter(CompareOperator.EQUAL, new RegexStringComparator("qualA.*"));
    get.setFilter(regexQualFilter);
    result = table.get(get);
    Assert.assertEquals(1, result.size());
  }


  @Test
  public void testFamilyFilter() throws IOException {
    byte[] rowKey = dataHelper.randomData("family-filter-");
    byte[] qualA = dataHelper.randomData("family-filter-qualA-");
    byte[] qualAValue = dataHelper.randomData("qualA-value");
    byte[] qualB = dataHelper.randomData("family-filter-qualB-");
    byte[] qualBValue = dataHelper.randomData("qualB-value");

    Table table = getTable();
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, qualA, qualAValue);
    put.addColumn(COLUMN_FAMILY2, qualB, qualBValue);
    table.put(put);

    {
      Get get = new Get(rowKey)
          .setFilter(new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(COLUMN_FAMILY)));
      Result result = table.get(get);
      Assert.assertEquals(1, result.size());
      Cell cell = result.rawCells()[0];
      Assert.assertTrue(CellUtil.matchingFamily(cell, COLUMN_FAMILY));
      Assert.assertTrue(CellUtil.matchingQualifier(cell, qualA));
      Assert.assertTrue(CellUtil.matchingValue(cell, qualAValue));
    }

    {
      Get get = new Get(rowKey)
          .setFilter(new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(COLUMN_FAMILY2)));
      Result result = table.get(get);
      Assert.assertEquals(1, result.size());
      Cell cell = result.rawCells()[0];
      Assert.assertTrue(CellUtil.matchingFamily(cell, COLUMN_FAMILY2));
      Assert.assertTrue(CellUtil.matchingQualifier(cell, qualB));
      Assert.assertTrue(CellUtil.matchingValue(cell, qualBValue));
    }
  }

  @Test
  public void testPageFilters() throws IOException {
    byte[][] rowKeys = dataHelper.randomData("pageFilter-", 100);
    byte[] qualA = dataHelper.randomData("qualA");
    byte[] value = Bytes.toBytes("Important data goes here");
    List<Put> puts = new ArrayList<>();
    for (byte[] rowKey : rowKeys) {
      puts.add(new Put(rowKey).addColumn(COLUMN_FAMILY, qualA, value));
    }

    Table table = getTable();
    table.put(puts);

    Scan scan = new Scan(Bytes.toBytes("pageFilter-"));

    PageFilter pageFilter = new PageFilter(20);
    scan.setFilter(pageFilter);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Assert.assertEquals(20,  Iterators.size(scanner.iterator()));
    }

    FilterList filterList = new FilterList(
        Operator.MUST_PASS_ALL,
        new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(qualA)),
        pageFilter);
    scan.setFilter(filterList);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Assert.assertEquals(20,  Iterators.size(scanner.iterator()));
    }
  }

  /**
   * Test {@link FuzzyRowFilter} to make sure that a String that matches the following regex is
   * matched:
   * '.{8}-fuzzy-row-suffix'
   */
  @Test
  public void testFuzzyRowFilter() throws IOException {
    if (!sharedTestEnv.isBigtable()) {
      // HBase doesn't seem to work as expected.  Test to make sure that bigtable does the right thing.
      return;
    }
    final String rowSuffix = "-fuzzy-row-suffix";
    final byte[] qualA = dataHelper.randomData("qualA");
    byte[] value = Bytes.toBytes("Important data goes here");

    // 'bad' comes before 'fuzzy' alphabetically and 'other' comes after 'fuzzy'.
    byte[] missKey1 = dataHelper.randomData("a", "-bad-row-suffix");
    byte[] missKey2 = dataHelper.randomData("b", "-fuzzy-bad-row-suffix");
    byte[] missKey3 = dataHelper.randomData("c", "-other-row-suffix");

    // dataHelper.randomData() adds 8 random characters between the prefix and suffix
    byte[] hitKey1 = dataHelper.randomData("a", rowSuffix);
    byte[] hitKey2 = dataHelper.randomData("b", rowSuffix);
    byte[] hitKey3 = dataHelper.randomData("c", rowSuffix);
    byte[] hitKey4 = dataHelper.randomData("d", rowSuffix);
    StringBuilder filterString = new StringBuilder();
    final int prefixSize = 9;
    int size = prefixSize + rowSuffix.length();
    byte[] filterBytes = new byte[size];
    for (int i = 0; i < prefixSize; i++) {
      filterString.append("\\x00");
      filterBytes[i] = 1;
    }
    filterString.append(rowSuffix);
    for (int i = 0; i < rowSuffix.length(); i++) {
      filterBytes[i + prefixSize] = 0;
    }
    FuzzyRowFilter fuzzyFilter = new FuzzyRowFilter(
      Arrays.asList(
       new Pair<byte[], byte[]>(
         Bytes.toBytesBinary(filterString.toString()),
         filterBytes)));

    Scan scan = new Scan();
    scan.setFilter(fuzzyFilter);

    Table table = getTable();
    List<Put> puts = new ArrayList<>();
    for (byte[] key : Arrays.asList(missKey1, missKey2, missKey3, hitKey1, hitKey2, hitKey3, hitKey4)) {
      puts.add(new Put(key).addColumn(COLUMN_FAMILY, qualA, value));
    }
    table.put(puts);

    try (ResultScanner scanner = table.getScanner(scan)) {
      assertNextEquals(scanner, hitKey1);
      assertNextEquals(scanner, hitKey2);
      assertNextEquals(scanner, hitKey3);
      assertNextEquals(scanner, hitKey4);
      Assert.assertNull(scanner.next());
    }
  }

  private static void assertNextEquals(ResultScanner scanner, byte expectedKey[])
      throws IOException {
    Result current = scanner.next();
    Assert.assertNotNull(current);
    Assert.assertEquals(Bytes.toString(expectedKey),  Bytes.toString(current.getRow()));
  }

  @Test
  public void testInterleaveNoDuplicateCells() throws IOException {
    String rowKeyPrefix = dataHelper.randomString("interleave-no-dups-");
    byte[] qualA = dataHelper.randomData("interleave-no-dups-qual");
    Table table = addDataForTesting(rowKeyPrefix, qualA);

    ColumnPrefixFilter prefixFilter1 =
        new ColumnPrefixFilter(Bytes.toBytes("interleave-no-dups"));
    ColumnPrefixFilter prefixFilter2 =
        new ColumnPrefixFilter(Bytes.toBytes("interleave-no-dups-qual"));
    FilterList filterList = new FilterList(
        Operator.MUST_PASS_ONE,
        prefixFilter1,
        prefixFilter2);
    Scan scan = new Scan(Bytes.toBytes(rowKeyPrefix));
    scan.setFilter(filterList);

    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result result: scanner) {
        Assert.assertEquals(1,  result.getColumnCells(COLUMN_FAMILY, qualA).size());
      }
    }
  }

  @Test
  public void testBigtableFilter() throws IOException {
    if (!sharedTestEnv.isBigtable()) {
      return;
    }

    byte[] rowKey = dataHelper.randomData("cbt-filter-");
    byte[] qualA = Bytes.toBytes("a");
    byte[] qualB = Bytes.toBytes("b");
    byte[] valA = dataHelper.randomData("a");
    byte[] valB = dataHelper.randomData("b");

    try(Table table = getTable()){
      table.put(new Put(rowKey)
        .addColumn(COLUMN_FAMILY, qualA, valA)
        .addColumn(COLUMN_FAMILY, qualB, valB));

      Filters.Filter qualAFilter =
          Filters.FILTERS.qualifier().exactMatch(ByteString.copyFrom(qualA));
      BigtableFilter bigtableFilter = new BigtableFilter(qualAFilter);
      Result result = table.get(new Get(rowKey).setFilter(bigtableFilter));

      Assert.assertEquals(1, result.size());
      Assert.assertTrue(CellUtil.matchingValue(result.rawCells()[0], valA));
    }
  }
  private Result[] scanWithFilter(Table t, byte[] startRow, byte[] endRow, byte[] qual,
      Filter f) throws IOException {
    Scan scan = new Scan(startRow, endRow).setFilter(f).addColumn(COLUMN_FAMILY, qual);
    ResultScanner scanner = t.getScanner(scan);
    Result[] results = scanner.next(10);
    return results;
  }

  private Table addDataForTesting(String rowKeyPrefix, byte[] qualA)
      throws IOException {
    Table table = getTable();
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String indexStr = String.valueOf(i);
      byte[] rowKey = Bytes.toBytes(rowKeyPrefix + indexStr);
      puts.add(new Put(rowKey).addColumn(COLUMN_FAMILY, qualA, Bytes.toBytes(indexStr)));
    }
    table.put(puts);

    return table;
  }


  @Test
  public void testFuzzyDifferentSizes() throws Exception {
    Table table = getDefaultTable();
    List<byte[]> keys = Collections.unmodifiableList(
        Arrays.asList(
            createKey(1, 2, 3, 4, 5, 6),
            createKey(1, 9, 9, 4, 9, 9),
            createKey(2, 3, 4, 5, 6, 7)));

    List<Put> puts = new ArrayList<>();
    for(byte[] key : keys) {
      puts.add(new Put(key).addColumn(SharedTestEnvRule.COLUMN_FAMILY,
          Bytes.toBytes(0), Bytes.toBytes(0)));
    }

    table.put(puts);

    // match keys with 1 in the first position and 4 in the 4th position
    Pair<byte[], byte[]> fuzzyData = Pair
        .newPair(
            createKey(1, 0, 0, 4),
            createKey(0, 1, 1, 0));

    Scan scan = new Scan().setFilter(new FuzzyRowFilter(ImmutableList.of(fuzzyData)));

    // only the first and second keys should be matched
    try (ResultScanner scanner = table.getScanner(scan)) {
      assertMatchingRow(scanner.next(), keys.get(0));
      assertMatchingRow(scanner.next(), keys.get(1));
      assertNull(scanner.next());
    }
  }

  private static byte[] createKey(int... values) {
    byte[] bytes = new byte[4 * values.length];
    for (int i = 0; i < values.length; i++) {
      System.arraycopy(Bytes.toBytes(values[i]), 0, bytes, 4 * i, 4);
    }
    return bytes;
  }

  private static void assertMatchingRow(Result result, byte[] key) {
    assertNotNull(result);
    assertTrue(CellUtil.matchingRows(result.rawCells()[0], key));
  }

}
