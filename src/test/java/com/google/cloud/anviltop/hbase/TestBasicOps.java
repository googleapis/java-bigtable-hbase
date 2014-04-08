package com.google.cloud.anviltop.hbase;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

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
public class TestBasicOps extends AbstractTest {
  final String TABLE_NAME = "test";
  final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");

  //@Test  TODO(carterpage) - enable once implemented
  public void testPutGetDelete() throws IOException {
    // Initialize
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] testQualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.random(8));
    byte[] testValue = Bytes.toBytes("testValue-" + RandomStringUtils.random(8));
    putGetDeleteExists(rowKey, testQualifier, testValue);
  }

  //@Test  TODO(carterpage) - enable once implemented
  public void testBinaryPutGetDelete() throws IOException {
    // Initialize
    Random random = new Random();
    byte[] rowKey = new byte[100];
    random.nextBytes(rowKey);
    byte[] testQualifier = new byte[100];
    random.nextBytes(testQualifier);
    byte[] testValue = new byte[100];
    random.nextBytes(testValue);
    // TODO(carterpage) - need to test that column-family can work as raw binary

    // Put
    putGetDeleteExists(rowKey, testQualifier, testValue);
  }

  //@Test TODO(carterpage) - enable once implemented
  public void testNullQualifier() throws IOException {
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] testQualifier = null;
    byte[] testValue = Bytes.toBytes("testValue-" + RandomStringUtils.random(8));
    putGetDeleteExists(rowKey, testQualifier, testValue);
  }

  private void putGetDeleteExists(byte[] rowKey, byte[] testQualifier, byte[] testValue) throws IOException {
    HTableInterface table = connection.getTable(TABLE_NAME);

    // Put
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, testQualifier, testValue);
    table.put(put);

    // Get
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, testQualifier);
    Result result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(1, cells.size());
    Assert.assertArrayEquals(testValue, cells.get(0).getValueArray());

    // Delete
    Delete delete = new Delete(rowKey);
    delete.deleteColumn(COLUMN_FAMILY, testQualifier);
    table.delete(delete);

    // Confirm deleted
    Assert.assertFalse(table.exists(get));
  }
}
