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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.IntegrationTests.*;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class TestBasicOps extends AbstractTest {
  /**
   * Happy path for a single value.
   */
  @Test
  public void testPutGetDelete() throws IOException {
    // Initialize
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    byte[] testValue = dataHelper.randomData("testValue-");
    putGetDeleteExists(rowKey, testQualifier, testValue);
  }

  /**
   * Requirement 1.2 - Rowkey, family, qualifer, and value are byte[]
   */
  @Test
  public void testBinaryPutGetDelete() throws IOException {
    // Initialize
    Random random = new Random();
    byte[] rowKey = new byte[100];
    random.nextBytes(rowKey);
    byte[] testQualifier = new byte[100];
    random.nextBytes(testQualifier);
    byte[] testValue = new byte[100];
    random.nextBytes(testValue);
    // TODO(carterpage) - test that column-family can work as raw binary

    // Put
    putGetDeleteExists(rowKey, testQualifier, testValue);
  }

  /**
   * Requirement 1.9 - Referring to a column without the qualifier implicity sets a special "empty"
   * qualifier.
   */
  @Test
  public void testNullQualifier() throws IOException {
    // Initialize values
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] testValue = dataHelper.randomData("testValue-");

    // Insert value with null qualifier
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, null, testValue);
    table.put(put);

    // This is treated the same as an empty String (which is just an empty byte array).
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, Bytes.toBytes(""));
    Result result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, null));
    Assert.assertArrayEquals(testValue,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, null)));

    // Get as a null.  This should work.
    get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, null);
    result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, null));
    Assert.assertArrayEquals(testValue,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, null)));

    // This should return when selecting the whole family too.
    get = new Get(rowKey);
    get.addFamily(COLUMN_FAMILY);
    result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, null));
    Assert.assertArrayEquals(testValue,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, null)));

    // Delete
    Delete delete = new Delete(rowKey);
    delete.addColumns(COLUMN_FAMILY, null);
    table.delete(delete);

    // Confirm deleted
    Assert.assertFalse(table.exists(get));
    table.close();
  }

  /**
   * Requirement 2.4 - Maximum cell size is 10MB by default.  Can be overriden using
   * hbase.client.keyvalue.maxsize property.
   *
   * Cell size includes value and key info, so the value needs to a bit less than the max to work.
   */
  @Test
  public void testPutBigValue() throws IOException {
    // Initialize variables
    byte[] testRowKey = dataHelper.randomData("testrow-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    byte[] testValue = new byte[(10 << 20) - 1024];  // 10 MB - 1kB
    new Random().nextBytes(testValue);
    putGetDeleteExists(testRowKey, testQualifier, testValue);
  }

  /**
   * Requirement 2.4 - Maximum cell size is 10MB by default.  Can be overriden using
   * hbase.client.keyvalue.maxsize property.
   *
   * Ensure the failure case.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testPutTooBigValue() throws IOException {
    // Initialize variables
    byte[] testRowKey = dataHelper.randomData("testrow-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    int metadataSize =  (20 + testRowKey.length + COLUMN_FAMILY.length + testQualifier.length);
    byte[] testValue = new byte[(10 << 20) - metadataSize + 1];  // 10 MB + 1
    new Random().nextBytes(testValue);
    putGetDeleteExists(testRowKey, testQualifier, testValue);
  }

  @Test
  public void testPutAlmostTooBigValue() throws IOException {
    // Initialize variables
    byte[] testRowKey = dataHelper.randomData("testrow-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    int metadataSize =  (20 + testRowKey.length + COLUMN_FAMILY.length + testQualifier.length);
    byte[] testValue = new byte[(10 << 20) - metadataSize];  // 10 MB
    new Random().nextBytes(testValue);
    putGetDeleteExists(testRowKey, testQualifier, testValue);
  }

  private void putGetDeleteExists(byte[] rowKey, byte[] testQualifier, byte[] testValue)
      throws IOException {
    Table table = getConnection().getTable(TABLE_NAME);

    // Put
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, testQualifier, testValue);
    table.put(put);

    // Get
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, testQualifier);
    Result result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(1, cells.size());
    Assert.assertArrayEquals(testValue, CellUtil.cloneValue(cells.get(0)));

    // Delete
    Delete delete = new Delete(rowKey);
    delete.addColumns(COLUMN_FAMILY, testQualifier);
    table.delete(delete);

    // Confirm deleted
    Assert.assertFalse(table.exists(get));
    table.close();
  }
}
