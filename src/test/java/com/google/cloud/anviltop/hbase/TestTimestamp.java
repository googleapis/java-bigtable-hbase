package com.google.cloud.anviltop.hbase;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
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
  @Test
  public void testArbitraryTimestamp() throws IOException {
    // Initialize
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] testQualifier = Bytes.toBytes("testQual-" + RandomStringUtils.randomAlphanumeric(8));
    String testValue1 = "testValue-" + RandomStringUtils.randomAlphanumeric(8);
    String testValue2 = "testValue-" + RandomStringUtils.randomAlphanumeric(8);
    String testValue3 = "testValue-" + RandomStringUtils.randomAlphanumeric(8);

    // Put three versions
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, testQualifier, 1L, Bytes.toBytes(testValue1));
    put.add(COLUMN_FAMILY, testQualifier, 2L, Bytes.toBytes(testValue2));
    put.add(COLUMN_FAMILY, testQualifier, 3L, Bytes.toBytes(testValue3));
    table.put(put);

    // Confirm they are all here, in descending version number
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, testQualifier);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(3, cells.size());
    Assert.assertEquals(3L, cells.get(0).getTimestamp());
    Assert.assertEquals(testValue3, Bytes.toString(CellUtil.cloneValue(cells.get(0))));
    Assert.assertEquals(2L, cells.get(1).getTimestamp());
    Assert.assertEquals(testValue2, Bytes.toString(CellUtil.cloneValue(cells.get(1))));
    Assert.assertEquals(1L, cells.get(2).getTimestamp());
    Assert.assertEquals(testValue1, Bytes.toString(CellUtil.cloneValue(cells.get(2))));

    // Now limit results to just two versions
    get.setMaxVersions(2);
    result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(2, cells.size());
    Assert.assertEquals(3L, cells.get(0).getTimestamp());
    Assert.assertEquals(testValue3, Bytes.toString(CellUtil.cloneValue(cells.get(0))));
    Assert.assertEquals(2L, cells.get(1).getTimestamp());
    Assert.assertEquals(testValue2, Bytes.toString(CellUtil.cloneValue(cells.get(1))));

    // Delete the middle version
    Delete delete = new Delete(rowKey);
    delete.deleteColumn(COLUMN_FAMILY, testQualifier, 2L);
    table.delete(delete);

    // Confirm versions 1 & 3 remain
    get.setMaxVersions(5);
    result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(2, cells.size());
    Assert.assertEquals(3L, cells.get(0).getTimestamp());
    Assert.assertEquals(testValue3, Bytes.toString(CellUtil.cloneValue(cells.get(0))));
    Assert.assertEquals(1L, cells.get(1).getTimestamp());
    Assert.assertEquals(testValue1, Bytes.toString(CellUtil.cloneValue(cells.get(1))));

    // Delete row
    delete = new Delete(rowKey);
    table.delete(delete);

    // Confirm it's gone
    Assert.assertFalse(table.exists(get));
    table.close();
  }
}
