/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import static com.google.cloud.bigtable.hbase.IntegrationTests.COLUMN_FAMILY;
import static com.google.cloud.bigtable.hbase.IntegrationTests.TABLE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSingleColumnValueFilter extends AbstractTest {

  static final int count = 10;
  final byte[] QUALIFIER = dataHelper.randomData("TestSingleColumnValueFilter");

  private Map<String, Long> result = new HashMap<>();
  private Table table;
  private boolean added = false;

  @Before
  public void fillTable() throws IOException {
    table = getConnection().getTable(TABLE_NAME);
    if (!added) {
      result.clear();
      List<Put> puts = new ArrayList<>();
      for (long i = 0; i < count; i++) {
        final UUID rowKey = UUID.randomUUID();
        byte[] row = Bytes.toBytes(rowKey.toString());
        result.put(rowKey.toString(), i);
        puts.add(new Put(row).addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes(i)));
      }
      table.put(puts);
      added = true;
    }
  }

  @After
  public void delete() throws IOException {
    table.close();
    table = null;
    result.clear();
  }

  @Test
  public void testInf() throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL,
            Bytes.toBytes(2L));
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    Set<String> rowKeys = getResult(scan);
    Assert.assertEquals(3, rowKeys.size());
    for (final String k : rowKeys) {
      Long v = result.get(k);
      Assert.assertNotNull(v);
      Assert.assertTrue(v <= 2);
    }
  }

  private Set<String> getResult(Scan scan) throws IOException {
    Set<String> rowKeys = new HashSet<>();
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result result : scanner) {
        rowKeys.add(Bytes.toString(result.getRow()));
      }
    }
    return rowKeys;
  }

  @Test
  public void testStrictInf() throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.LESS, Bytes.toBytes(4L));
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    Set<String> rowKeys = getResult(scan);
    Assert.assertEquals(4, rowKeys.size());
    for (final String k : rowKeys) {
      Long v = result.get(k);
      Assert.assertNotNull(v);
      Assert.assertTrue(v < 4);
    }
  }

  @Test
  public void testStrictSup() throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.GREATER, Bytes.toBytes(4L));
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    Set<String> rowKeys = getResult(scan);
    Assert.assertEquals(5, rowKeys.size());
    for (final String k : rowKeys) {
      Long v = result.get(k);
      Assert.assertNotNull(v);
      Assert.assertTrue(v > 4);
    }
  }

  @Test
  public void testEqual() throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.EQUAL, Bytes.toBytes(4L));
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    Set<String> rowKeys = getResult(scan);
    Assert.assertEquals(1, rowKeys.size());
    for (final String k : rowKeys) {
      Long v = result.get(k);
      Assert.assertNotNull(v);
      Assert.assertTrue(v == 4);
    }
  }

  @Test
  public void testSup() throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL,
            Bytes.toBytes(5L));
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    Set<String> rowKeys = getResult(scan);
    Assert.assertEquals(5, rowKeys.size());
    for (final String k : rowKeys) {
      Long v = result.get(k);
      Assert.assertNotNull(v);
      Assert.assertTrue(v >= 5);
    }
  }

  @Test
  public void testNot() throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(COLUMN_FAMILY, QUALIFIER, CompareOp.NOT_EQUAL,
            Bytes.toBytes(5L));
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    Set<String> rowKeys = getResult(scan);
    Assert.assertEquals(9, rowKeys.size());
    for (final String k : rowKeys) {
      Long v = result.get(k);
      Assert.assertNotNull(v);
      Assert.assertTrue(v != 5);
    }
  }
}
