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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class TestSingleColumnValueFilter extends AbstractTest {

  private static final int count = 10;
  private static final String PREFIX = "TestSingleColumnValueFilter";
  private static final String NOT_STRICT_KEY = PREFIX + "_NotStrict";
  private static final byte[] QUALIFIER = dataHelper.randomData(PREFIX);
  private static final byte[] OTHER_QUALIFIER = dataHelper.randomData(NOT_STRICT_KEY);

  private static Set<String> keys = null;
  private static Table table;

  @BeforeClass
  public static void fillTable() throws IOException {
    table = sharedTestEnv.getDefaultTable();

    List<Put> puts = new ArrayList<>();
    ImmutableSet.Builder<String> keyBuilder = ImmutableSet.builder();
    for (long i = 0; i < count; i++) {
      byte[] row = dataHelper.randomData(PREFIX);
      keyBuilder.add(Bytes.toString(row));
      int randomValue = (int) Math.floor(count * Math.random());
      puts.add(new Put(row)
        .addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes(i))
        .addColumn(COLUMN_FAMILY, OTHER_QUALIFIER, Bytes.toBytes(randomValue)));
    }

    byte[] otheRow = dataHelper.randomData(NOT_STRICT_KEY);
    byte[] stringVal = Bytes.toBytes("Not a number");
    puts.add(new Put(otheRow).addColumn(COLUMN_FAMILY, OTHER_QUALIFIER, stringVal));

    keys = keyBuilder.build();
    table.put(puts);
  }

  @AfterClass
  public static void delete() throws IOException {
    table.close();
    table = null;
    keys = null;
  }

  @Test
  public void testInf() throws IOException {
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL,
            Bytes.toBytes(2L));
    filter.setFilterIfMissing(true);
    Map<String, Long> rowKeys = getResult(filter);
    Assert.assertEquals(3, rowKeys.size());
    for (Entry<String, Long> entry : rowKeys.entrySet()) {
      checkKey(entry.getKey());
      Long v = entry.getValue();
      Assert.assertTrue(String.format("%d > 2", v), v <= 2);
    }
  }

  private static void checkKey(String key) {
    Assert.assertNotNull(key + " was incorrectly returned", keys.contains(key));
  }

  private static Map<String, Long> getResult(SingleColumnValueFilter filter) throws IOException {
    Scan scan = new Scan();
    scan.setRowPrefixFilter(Bytes.toBytes(PREFIX));
    scan.setFilter(filter);

    Map<String, Long> rowKeys = new HashMap<>();
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result result : scanner) {
        String key = Bytes.toString(result.getRow());

        if (!key.startsWith(PREFIX)) {
          Assert.fail(String.format("Found key %s which does not start with %s", key, PREFIX));
        } else if (key.startsWith(NOT_STRICT_KEY)) {
          long randomValue = (long) ((Math.random() * 30) - 15);
          rowKeys.put(key, randomValue);
        } else {
          Assert.assertEquals(2, result.size());
          byte[] val = CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, QUALIFIER));
          rowKeys.put(key, Bytes.toLong(val));
        }
      }
    }
    return rowKeys;
  }

  @Test
  public void testNotStrictInf() throws IOException {
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.LESS, Bytes.toBytes(4L));
    filter.setFilterIfMissing(false);
    boolean foundRowWithMissingColumn = false;
    Map<String, Long> rowKeys = getResult(filter);
    Assert.assertEquals(5, rowKeys.size());
    for (Entry<String, Long> entry : rowKeys.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(NOT_STRICT_KEY)) {
        foundRowWithMissingColumn = true;
      } else {
        Long v = entry.getValue();
        Assert.assertTrue(String.format("%d >= 4", v), v < 4);
      }
    }
    Assert.assertTrue(foundRowWithMissingColumn);
  }

  @Test
  public void testStrictInf() throws IOException {
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.LESS, Bytes.toBytes(4L));
    filter.setFilterIfMissing(true);
    Map<String, Long> rowKeys = getResult(filter);
    Assert.assertEquals(4, rowKeys.size());
    for (Entry<String, Long> entry : rowKeys.entrySet()) {
      checkKey(entry.getKey());
      Long v = entry.getValue();
      Assert.assertTrue(String.format("%d >= 4", v), v < 4);
    }
  }

  @Test
  public void testStrictSup() throws IOException {
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.GREATER, Bytes.toBytes(4L));
    filter.setFilterIfMissing(true);
    Map<String, Long> rowKeys = getResult(filter);
    Assert.assertEquals(5, rowKeys.size());
    for (Entry<String, Long> entry : rowKeys.entrySet()) {
      checkKey(entry.getKey());
      Long v = entry.getValue();
      Assert.assertTrue(String.format("%d <= 4", v), v > 4);
    }
  }

  @Test
  public void testEqual() throws IOException {
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.EQUAL, Bytes.toBytes(4L));
    filter.setFilterIfMissing(true);
    Map<String, Long> rowKeys = getResult(filter);
    Assert.assertEquals(1, rowKeys.size());
    for (Entry<String, Long> entry : rowKeys.entrySet()) {
      checkKey(entry.getKey());
      Long v = entry.getValue();
      Assert.assertTrue(String.format("%d != 4", v), v == 4);
    }
  }

  @Test
  public void testSup() throws IOException {
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL,
            Bytes.toBytes(5L));
    filter.setFilterIfMissing(true);
    Map<String, Long> rowKeys = getResult(filter);
    Assert.assertEquals(5, rowKeys.size());
    for (Entry<String, Long> entry : rowKeys.entrySet()) {
      checkKey(entry.getKey());
      Long v = entry.getValue();
      Assert.assertTrue(String.format("%d < 5", v), v >= 5);
    }
  }

  @Test
  public void testNot() throws IOException {
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.NOT_EQUAL,
            Bytes.toBytes(5L));
    filter.setFilterIfMissing(true);
    Map<String, Long> rowKeys = getResult(filter);
    Assert.assertEquals(9, rowKeys.size());
    for (Entry<String, Long> entry : rowKeys.entrySet()) {
      checkKey(entry.getKey());
      Long v = entry.getValue();
      Assert.assertTrue(String.format("%d == 5", v), v != 5);
    }
  }
}
