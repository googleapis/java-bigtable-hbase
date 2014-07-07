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
package com.google.cloud.anviltop.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestDelete extends AbstractTest {
  /**
   * Requirement 4.1 - Delete all data for a given rowkey.
   */
  @Test
  public void testDeleteRow() throws IOException {
    // Initialize data
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    byte[] qual2 = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");

    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual1, 1L, value);
    put.add(COLUMN_FAMILY, qual1, 2L, value);
    put.add(COLUMN_FAMILY, qual2, 1L, value);
    table.put(put);

    // Check values
    Get get = new Get(rowKey);
    Assert.assertTrue(table.exists(get));

    // Delete row
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    Assert.assertFalse("Entire row should be deleted.", table.exists(get));

    table.close();
  }

  /**
   * Requirement 4.2 - Delete the latest version of a specific column (family:qualifier)
   */
  @Test
  public void testDeleteLatestColumnVersion() throws IOException {
    // Initialize data
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual, 1L, value);
    put.add(COLUMN_FAMILY, qual, 2L, value);
    table.put(put);

    // Check values
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals(2, result.size());

    // Delete latest column version
    Delete delete = new Delete(rowKey);
    delete.deleteColumn(COLUMN_FAMILY, qual);
    table.delete(delete);

    // Confirm results.
    result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("Version 2 should be deleted, but not version 1.",
      1L, result.getColumnLatestCell(COLUMN_FAMILY, qual).getTimestamp());

    table.close();
  }

  /**
   * Requirement 4.3 - Delete a specific version of a specific column (family:qualifer + ts)
   */
  @Test
  public void testDeleteSpecificColumnVersion() throws IOException {
    // Initialize data
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual, 1L, value);
    put.add(COLUMN_FAMILY, qual, 2L, value);
    put.add(COLUMN_FAMILY, qual, 3L, value);
    table.put(put);

    // Check values
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals(3, result.size());

    // Delete latest column version
    Delete delete = new Delete(rowKey);
    delete.deleteColumn(COLUMN_FAMILY, qual, 2L);
    table.delete(delete);

    // Confirm results
    result = table.get(get);
    Assert.assertEquals("Only one version should be deleted", 2, result.size());
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertEquals("Version 3 should be the latest version", 3L, cells.get(0).getTimestamp());
    Assert.assertEquals("Version 1 should be the oldest version", 1L, cells.get(1).getTimestamp());

    table.close();
  }
}
