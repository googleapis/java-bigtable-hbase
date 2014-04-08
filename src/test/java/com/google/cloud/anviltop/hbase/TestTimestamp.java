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
public class TestTimestamp extends AbstractTest {
  final String TABLE_NAME = "test";
  final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");

  //@Test TODO(carterpage) - enable once supported
  public void testArbitraryTimestamp() throws IOException {
    // Initialize
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] testQualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.random(8));
    byte[] testValue1 = Bytes.toBytes("testValue-" + RandomStringUtils.random(8));
    byte[] testValue2 = Bytes.toBytes("testValue-" + RandomStringUtils.random(8));
    byte[] testValue3 = Bytes.toBytes("testValue-" + RandomStringUtils.random(8));

    // Put three versions
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, testQualifier, 1L, testValue1);
    put.add(COLUMN_FAMILY, testQualifier, 2L, testValue2);
    put.add(COLUMN_FAMILY, testQualifier, 3L, testValue3);
    table.put(put);

    // Confirm they are all here, in descending version number
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, testQualifier);
    Result result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(3, cells.size());
    Assert.assertEquals(3L, cells.get(0).getTimestamp());
    Assert.assertEquals(testValue3, cells.get(0).getValueArray());
    Assert.assertEquals(2L, cells.get(1).getTimestamp());
    Assert.assertEquals(testValue2, cells.get(1).getValueArray());
    Assert.assertEquals(1L, cells.get(2).getTimestamp());
    Assert.assertEquals(testValue1, cells.get(2).getValueArray());

    // Delete the middle version
    Delete delete = new Delete(rowKey);
    delete.deleteColumn(COLUMN_FAMILY, testQualifier, 2L);
    table.delete(delete);

    // Confirm versions 1 & 3 remain
    result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(2, cells.size());
    Assert.assertEquals(3L, cells.get(0).getTimestamp());
    Assert.assertEquals(testValue3, cells.get(0).getValueArray());
    Assert.assertEquals(1L, cells.get(1).getTimestamp());
    Assert.assertEquals(testValue1, cells.get(1).getValueArray());

    // Now limit to just one version
    get.setMaxVersions(1);
    result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(1, cells.size());
    Assert.assertEquals(3L, cells.get(0).getTimestamp());
    Assert.assertEquals(testValue3, cells.get(0).getValueArray());

    // Delete row
    delete = new Delete(rowKey);
    table.delete(delete);

    // Confirm it's gone
    Assert.assertFalse(table.exists(get));
  }
}
