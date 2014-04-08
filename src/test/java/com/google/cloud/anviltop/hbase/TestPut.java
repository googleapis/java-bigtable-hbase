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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
  final String TABLE_NAME = "test";
  final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");

  public void testPutMultipleCellsOneRow() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    int numCells = 100;
    byte[][] testQualifiers = new byte[numCells][];
    byte[][] testValues= new byte[numCells][];
    for (int i = 0 ; i < numCells ; ++i) {
      testQualifiers[i] = Bytes.toBytes("testQualifier-" + RandomStringUtils.random(8));
      testValues[i] = Bytes.toBytes("testValue-" + RandomStringUtils.random(8));
    }

    // Send put
    Put put = new Put(rowKey);
    for (int i = 0 ; i < numCells ; ++i) {
      put.add(COLUMN_FAMILY, testQualifiers[i], testValues[i]);
    }
    table.put(put);

    // Get
    Get get = new Get(rowKey);
    Result result = table.get(get);
    List<Cell> cells = result.listCells();
    Assert.assertEquals(numCells, cells.size());
    // Results are sorted, so calculate the order we expect
    SortedMap<ByteBuffer, ByteBuffer > expectedResults = new TreeMap<ByteBuffer, ByteBuffer>();
    for (int i = 0 ; i < numCells ; ++i) {
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
}
