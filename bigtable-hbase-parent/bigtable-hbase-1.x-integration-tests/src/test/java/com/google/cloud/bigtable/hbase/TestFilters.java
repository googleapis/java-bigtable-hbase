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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.wrappers.Filters;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.hbase.filter.BigtableFilter;
import com.google.cloud.bigtable.hbase.filter.TimestampRangeFilter;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

public class TestFilters extends AbstractTestFilters {
  
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

  @Override
  protected void getGetAddVersion(Get get, int version) throws IOException {
    get.setMaxVersions(version);
  }

  @Override
  protected void scanAddVersion(Scan scan, int version) throws IOException {
    scan.setMaxVersions(version);
  }

  @Override
  protected Filter getFilter(String enumVal, ByteArrayComparable rowKey2Comparable) throws IOException {
    if(LESS.equalsIgnoreCase(enumVal)){
      return new RowFilter(CompareOp.LESS, rowKey2Comparable);
    }else if(LESS_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new RowFilter(CompareOp.LESS_OR_EQUAL, rowKey2Comparable);
    }else if(EQUAL.equalsIgnoreCase(enumVal)){
      return new RowFilter(CompareOp.EQUAL, rowKey2Comparable);
    }if(NOT_EQUAL.equalsIgnoreCase(enumVal)){
      return new RowFilter(CompareOp.NOT_EQUAL, rowKey2Comparable);
    }else if(GREATER_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new RowFilter(CompareOp.GREATER_OR_EQUAL, rowKey2Comparable);
    }else if(GREATER.equalsIgnoreCase(enumVal)){
      return new RowFilter(CompareOp.GREATER, rowKey2Comparable);
    }else if(NO_OP.equalsIgnoreCase(enumVal)){
      return new RowFilter(CompareOp.NO_OP, rowKey2Comparable);
    }
    return null;
  }

  @Override
  protected ValueFilter getValueFilter(String enumVal, ByteArrayComparable rowKey2Comparable) throws IOException {
    if(LESS.equalsIgnoreCase(enumVal)){
      return new ValueFilter(CompareOp.LESS, rowKey2Comparable);
    }else if(LESS_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new ValueFilter(CompareOp.LESS_OR_EQUAL, rowKey2Comparable);
    }else if(EQUAL.equalsIgnoreCase(enumVal)){
      return new ValueFilter(CompareOp.EQUAL, rowKey2Comparable);
    }if(NOT_EQUAL.equalsIgnoreCase(enumVal)){
      return new ValueFilter(CompareOp.NOT_EQUAL, rowKey2Comparable);
    }else if(GREATER_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new ValueFilter(CompareOp.GREATER_OR_EQUAL, rowKey2Comparable);
    }else if(GREATER.equalsIgnoreCase(enumVal)){
      return new ValueFilter(CompareOp.GREATER, rowKey2Comparable);
    }else if(NO_OP.equalsIgnoreCase(enumVal)){
      return new ValueFilter(CompareOp.NO_OP, rowKey2Comparable);
    }
    return null;
  }

  @Override
  protected SingleColumnValueFilter getSingleColumnValueFilter(String enumVal, byte[] qualA,
      ByteArrayComparable valueComparable) throws IOException {
    if(LESS.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueFilter(COLUMN_FAMILY, qualA, CompareOp.LESS, valueComparable);
    }else if(LESS_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueFilter(COLUMN_FAMILY, qualA, CompareOp.LESS_OR_EQUAL, valueComparable);
    }else if(EQUAL.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueFilter(COLUMN_FAMILY, qualA, CompareOp.EQUAL, valueComparable);
    }if(NOT_EQUAL.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueFilter(COLUMN_FAMILY, qualA, CompareOp.NOT_EQUAL, valueComparable);
    }else if(GREATER_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueFilter(COLUMN_FAMILY, qualA, CompareOp.GREATER_OR_EQUAL, valueComparable);
    }else if(GREATER.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueFilter(COLUMN_FAMILY, qualA, CompareOp.GREATER, valueComparable);
    }else if(NO_OP.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueFilter(COLUMN_FAMILY, qualA, CompareOp.NO_OP, valueComparable);
    }
    return null;
  }
  
  @Override
  protected void assertKeysReturnedForSCVF(Table table, byte[] qualifier, String enumVal, byte[] value,
      byte[]... expectedKeys) throws IOException {
    if(LESS.equalsIgnoreCase(enumVal)){
      assertKeysReturnedForSCVFPrivate(table, qualifier, CompareOp.LESS, value, expectedKeys);
    }else if(LESS_OR_EQUAL.equalsIgnoreCase(enumVal)){
      assertKeysReturnedForSCVFPrivate(table, qualifier, CompareOp.LESS_OR_EQUAL, value, expectedKeys);
    }else if(EQUAL.equalsIgnoreCase(enumVal)){
      assertKeysReturnedForSCVFPrivate(table, qualifier, CompareOp.EQUAL, value, expectedKeys);
    }if(NOT_EQUAL.equalsIgnoreCase(enumVal)){
      assertKeysReturnedForSCVFPrivate(table, qualifier, CompareOp.NOT_EQUAL, value, expectedKeys);
    }else if(GREATER_OR_EQUAL.equalsIgnoreCase(enumVal)){
      assertKeysReturnedForSCVFPrivate(table, qualifier, CompareOp.GREATER_OR_EQUAL, value, expectedKeys);
    }else if(GREATER.equalsIgnoreCase(enumVal)){
      assertKeysReturnedForSCVFPrivate(table, qualifier, CompareOp.GREATER, value, expectedKeys);
    }else if(NO_OP.equalsIgnoreCase(enumVal)){
      assertKeysReturnedForSCVFPrivate(table, qualifier, CompareOp.NO_OP, value, expectedKeys);
    }
  }
  
  @Override
  protected Result getFirst(Table table, Scan rootScan, String enumVal, long value) throws IOException {
    CompareOp comparitor = null;
    if(LESS.equalsIgnoreCase(enumVal)){
      comparitor = CompareOp.LESS;
    }else if(LESS_OR_EQUAL.equalsIgnoreCase(enumVal)){
      comparitor = CompareOp.LESS_OR_EQUAL;
    }else if(EQUAL.equalsIgnoreCase(enumVal)){
      comparitor = CompareOp.EQUAL;
    }if(NOT_EQUAL.equalsIgnoreCase(enumVal)){
      comparitor = CompareOp.NOT_EQUAL;
    }else if(GREATER_OR_EQUAL.equalsIgnoreCase(enumVal)){
      comparitor = CompareOp.GREATER_OR_EQUAL;
    }else if(GREATER.equalsIgnoreCase(enumVal)){
      comparitor = CompareOp.GREATER;
    }else if(NO_OP.equalsIgnoreCase(enumVal)){
      comparitor = CompareOp.NO_OP;
    }
    try (ResultScanner results = table.getScanner(new Scan(rootScan)
        .setFilter(new ValueFilter(comparitor, new BinaryComparator(Bytes.toBytes(value)))))) {
      return results.next();
    }
  }
  @Override
  protected QualifierFilter getQualifierFilter(String enumVal, ByteArrayComparable rowKey2Comparable) throws IOException {
    if(LESS.equalsIgnoreCase(enumVal)){
      return new QualifierFilter(CompareOp.LESS, rowKey2Comparable);
    }else if(LESS_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new QualifierFilter(CompareOp.LESS_OR_EQUAL, rowKey2Comparable);
    }else if(EQUAL.equalsIgnoreCase(enumVal)){
      return new QualifierFilter(CompareOp.EQUAL, rowKey2Comparable);
    }if(NOT_EQUAL.equalsIgnoreCase(enumVal)){
      return new QualifierFilter(CompareOp.NOT_EQUAL, rowKey2Comparable);
    }else if(GREATER_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new QualifierFilter(CompareOp.GREATER_OR_EQUAL, rowKey2Comparable);
    }else if(GREATER.equalsIgnoreCase(enumVal)){
      return new QualifierFilter(CompareOp.GREATER, rowKey2Comparable);
    }else if(NO_OP.equalsIgnoreCase(enumVal)){
      return new QualifierFilter(CompareOp.NO_OP, rowKey2Comparable);
    }
    return null;
  }
  
  @Override
  protected SingleColumnValueExcludeFilter getSingleColumnExludeFilter(String enumVal, byte[] qualifier1,
      ByteArrayComparable rowKey2Comparable) throws IOException {
    if(LESS.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueExcludeFilter(COLUMN_FAMILY, qualifier1, CompareOp.LESS, rowKey2Comparable);
    }else if(LESS_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueExcludeFilter(COLUMN_FAMILY, qualifier1, CompareOp.LESS_OR_EQUAL, rowKey2Comparable);
    }else if(EQUAL.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueExcludeFilter(COLUMN_FAMILY, qualifier1, CompareOp.EQUAL, rowKey2Comparable);
    }if(NOT_EQUAL.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueExcludeFilter(COLUMN_FAMILY, qualifier1, CompareOp.NOT_EQUAL, rowKey2Comparable);
    }else if(GREATER_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueExcludeFilter(COLUMN_FAMILY, qualifier1, CompareOp.GREATER_OR_EQUAL, rowKey2Comparable);
    }else if(GREATER.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueExcludeFilter(COLUMN_FAMILY, qualifier1, CompareOp.GREATER, rowKey2Comparable);
    }else if(NO_OP.equalsIgnoreCase(enumVal)){
      return new SingleColumnValueExcludeFilter(COLUMN_FAMILY, qualifier1, CompareOp.NO_OP, rowKey2Comparable);
    }
    return null;
  }

  @Override
  protected FamilyFilter getFamilyFilter(String enumVal, ByteArrayComparable rowKey2Comparable) throws IOException {
    if(LESS.equalsIgnoreCase(enumVal)){
      return new FamilyFilter(CompareOp.LESS, rowKey2Comparable);
    }else if(LESS_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new FamilyFilter(CompareOp.LESS_OR_EQUAL, rowKey2Comparable);
    }else if(EQUAL.equalsIgnoreCase(enumVal)){
      return new FamilyFilter(CompareOp.EQUAL, rowKey2Comparable);
    }if(NOT_EQUAL.equalsIgnoreCase(enumVal)){
      return new FamilyFilter(CompareOp.NOT_EQUAL, rowKey2Comparable);
    }else if(GREATER_OR_EQUAL.equalsIgnoreCase(enumVal)){
      return new FamilyFilter(CompareOp.GREATER_OR_EQUAL, rowKey2Comparable);
    }else if(GREATER.equalsIgnoreCase(enumVal)){
      return new FamilyFilter(CompareOp.GREATER, rowKey2Comparable);
    }else if(NO_OP.equalsIgnoreCase(enumVal)){
      return new FamilyFilter(CompareOp.NO_OP, rowKey2Comparable);
    }
    return null;
  }
  
  private static void assertMatchingRow(Result result, byte[] key) {
    assertNotNull(result);
    assertTrue(CellUtil.matchingRow(result.rawCells()[0], key));
  }

  /**
   * This method is added to handle enum types in 1.x test cases & 2.x test cases
   * 
   * @param table
   * @param qualifier
   * @param operator
   * @param value
   * @param expectedKeys
   * @throws IOException
   */
  private void assertKeysReturnedForSCVFPrivate(Table table, byte[] qualifier, CompareOp operator,
      byte[] value, byte[]... expectedKeys) throws IOException {
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, qualifier, operator,
            new BinaryComparator(value));
    filter.setFilterIfMissing(true);
    assertKeysReturnedForFilter(table, filter, expectedKeys);
  }

}
