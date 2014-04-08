package com.google.cloud.anviltop.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.junit.Assert;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

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
public class TestPut extends AbstractTest {
  static final String TABLE_NAME = "test";
  static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");
  static final int NUM_CELLS = 100;
  static final int NUM_ROWS = 100;

  /**
   * Test inserting a row with multiple cells.
   *
   * @throws IOException
   */
  //@Test - TODO(carterpage) - enable once supported
  public void testPutMultipleCellsOneRow() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[][] testQualifiers = new byte[NUM_CELLS][];
    byte[][] testValues= new byte[NUM_CELLS][];
    for (int i = 0 ; i < NUM_CELLS ; ++i) {
      testQualifiers[i] = Bytes.toBytes("testQualifier-" + RandomStringUtils.random(8));
      testValues[i] = Bytes.toBytes("testValue-" + RandomStringUtils.random(8));
    }

    // Send put
    Put put = new Put(rowKey);
    for (int i = 0 ; i < NUM_CELLS ; ++i) {
      put.add(COLUMN_FAMILY, testQualifiers[i], testValues[i]);
    }
    table.put(put);

    // Get
    Get get = new Get(rowKey);
    Result result = table.get(get);
    List<Cell> cells = result.listCells();
    Assert.assertEquals(NUM_CELLS, cells.size());
    // Results are sorted, so calculate the order we expect
    SortedMap<ByteBuffer, ByteBuffer > expectedResults = new TreeMap<ByteBuffer, ByteBuffer>();
    for (int i = 0 ; i < NUM_CELLS ; ++i) {
      expectedResults.put(ByteBuffer.wrap(testQualifiers[i]), ByteBuffer.wrap(testValues[i]));
    }
    int i = 0;
    for (Map.Entry<ByteBuffer, ByteBuffer> expectedResult : expectedResults.entrySet()) {
      Cell cell = cells.get(i);
      Assert.assertArrayEquals(expectedResult.getKey().array(), cell.getQualifierArray());
      Assert.assertArrayEquals(expectedResult.getValue().array(), cell.getValueArray());
    }

    // Delete
    Delete delete = new Delete(rowKey);
    table.delete(delete);

    // Confirm gone
    Assert.assertFalse(table.exists(get));
  }

  /**
   * Test inserting multiple rows at one time.
   *
   * @throws IOException
   */
  //@Test - TODO(carterpage) - enable once supported
  public void testPutGetDeleteMultipleRows() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>();
    Map<ByteBuffer, ByteBuffer> qualifiers = new TreeMap<ByteBuffer, ByteBuffer>();
    Map<ByteBuffer, ByteBuffer> values = new TreeMap<ByteBuffer, ByteBuffer>();
    for (int i = 0 ; i < NUM_ROWS ; ++i) {
      byte[] testRowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
      byte[] testQualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.random(8));
      byte[] testValue = Bytes.toBytes("testValue-" + RandomStringUtils.random(8));
      ByteBuffer rowKey = ByteBuffer.wrap(testRowKey);
      rowKeys.add(rowKey);
      qualifiers.put(rowKey, ByteBuffer.wrap(testQualifier));
      values.put(rowKey, ByteBuffer.wrap(testValue));
    }

    // Put
    List<Put> puts = new ArrayList<Put>(NUM_ROWS);
    for (ByteBuffer rowKey : rowKeys) {
      Put put = new Put(rowKey.array());
      put.add(COLUMN_FAMILY, qualifiers.get(rowKey).array(), values.get(rowKey).array());
      puts.add(put);
    }
    table.put(puts);

    // Get
    List<Get> gets = new ArrayList<Get>(NUM_ROWS);
    Collections.shuffle(rowKeys);  // Make sure results are returned in same order as Get list
    for (ByteBuffer rowKey : rowKeys) {
      Get get = new Get(rowKey.array());
      get.addColumn(COLUMN_FAMILY, qualifiers.get(rowKey).array());
      gets.add(get);
    }
    Result[] result = table.get(gets);
    Assert.assertEquals(NUM_ROWS, result.length);
    for (int i = 0 ; i < NUM_ROWS ; ++i) {
      ByteBuffer rowKey = rowKeys.get(i);
      Assert.assertArrayEquals(rowKey.array(), result[i].getRow());
      byte[] qualifier = qualifiers.get(rowKey).array();
      String descriptor = "Row " + i + " (" + Bytes.toString(rowKey.array()) + ": ";
      Assert.assertEquals(descriptor, 1, result[i].size());
      Assert.assertTrue(descriptor, result[i].containsNonEmptyColumn(COLUMN_FAMILY, qualifier));
      Assert.assertArrayEquals(descriptor, values.get(rowKey).array(),
          result[i].getColumnCells(COLUMN_FAMILY, qualifier).get(0).getValueArray());
    }

    // Delete
    List<Delete> deletes = new ArrayList<Delete>(NUM_ROWS);
    for (ByteBuffer rowKey : rowKeys) {
      Delete delete = new Delete(rowKey.array());
      delete.deleteColumn(COLUMN_FAMILY, qualifiers.get(rowKey).array());
      deletes.add(delete);
    }
    table.delete(deletes);

    // Confirm they are gone
    Boolean[] checks = table.exists(gets);
    for (Boolean check : checks) {
      Assert.assertFalse(check);
    }
  }
}
