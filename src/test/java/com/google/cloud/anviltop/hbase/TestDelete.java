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

import static com.google.cloud.anviltop.hbase.IntegrationTests.TABLE_NAME;
import static com.google.cloud.anviltop.hbase.IntegrationTests.COLUMN_FAMILY;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
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
    Table table = connection.getTable(TABLE_NAME);
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

  @Test
  public void testDeleteEmptyRow() throws IOException {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");

    Delete delete = new Delete(rowKey);
    table.delete(delete);

    table.close();
  }

  /**
   * Requirement 4.2 - Delete the latest version of a specific column (family:qualifier)
   */
  @Test
  public void testDeleteLatestColumnVersion() throws IOException {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
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
    Table table = connection.getTable(TABLE_NAME);
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

  /**
   * Requirement 4.4 - Delete all versions of a specific column
   */
  @Test
  public void testDeleteAllColumnVersions() throws IOException {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
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
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals(3, result.size());

    // Delete row
    Delete delete = new Delete(rowKey);
    delete.deleteColumns(COLUMN_FAMILY, qual1);
    table.delete(delete);

    // Check results
    result = table.get(get);
    Assert.assertEquals("Qual1 values should have been deleted", 1, result.size());
    Assert.assertTrue("Qual2 should be intact", result.containsColumn(COLUMN_FAMILY, qual2));
    Assert.assertArrayEquals("Qual2 value should match", qual2,
      CellUtil.cloneQualifier(result.getColumnLatestCell(COLUMN_FAMILY, qual2)));

    table.close();
  }

  /**
   * Requirement 4.5 - Delete all versions of a specific column less than or equal to a given timestamp.
   */
  @Test
  public void testDeleteOlderColumnVersions() throws IOException {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
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
    delete.deleteColumns(COLUMN_FAMILY, qual, 2L);
    table.delete(delete);

    // Confirm results
    result = table.get(get);
    Assert.assertEquals("Only one version should remain", 1, result.size());
    Assert.assertEquals("Version 3 should be the only version", 3L,
      result.getColumnLatestCell(COLUMN_FAMILY, qual).getTimestamp());

    table.close();
  }

  /**
   * Requirement 4.6 - Delete all versions of all columns of a particular family.
   */
  @Test
  public void testDeleteFamily() throws IOException {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
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
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals(3, result.size());

    // Delete row
    Delete delete = new Delete(rowKey);
    delete.deleteFamily(COLUMN_FAMILY);
    table.delete(delete);

    // Check results
    Assert.assertFalse("All of the family should be deleted", table.exists(get));

    table.close();
  }

  /**
   * Requirement 4.7 - Delete all columns of a particular family less than or equal to a timestamp.
   */
  @Test
  public void testDeleteOlderFamilyColumns() throws IOException {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    byte[] qual2 = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");

    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual1, 1L, value);
    put.add(COLUMN_FAMILY, qual1, 2L, value);
    put.add(COLUMN_FAMILY, qual1, 3L, value);
    put.add(COLUMN_FAMILY, qual2, 1L, value);
    put.add(COLUMN_FAMILY, qual2, 2L, value);
    table.put(put);

    // Check values
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals(5, result.size());

    // Delete row
    Delete delete = new Delete(rowKey);
    delete.deleteFamily(COLUMN_FAMILY, 2L);
    table.delete(delete);

    // Confirm results
    result = table.get(get);
    Assert.assertEquals("Only one version of qual1 should remain", 1, result.size());
    Assert.assertTrue("Qual1 should be the remaining cell", result.containsColumn(COLUMN_FAMILY, qual1));
    Assert.assertEquals("Version 3 should be the only version", 3L,
      result.getColumnLatestCell(COLUMN_FAMILY, qual1).getTimestamp());

    table.close();
  }

  /**
   * Requirement 4.8 - Delete all columns of a family with a specific ts.
   */
  @Test
  public void testDeleteFamilyWithSpecificTimestamp() throws IOException {
    // Initialize data
    Table table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    byte[] qual2 = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");

    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual1, 1L, value);
    put.add(COLUMN_FAMILY, qual1, 2L, value);
    put.add(COLUMN_FAMILY, qual1, 3L, value);
    put.add(COLUMN_FAMILY, qual2, 1L, value);
    put.add(COLUMN_FAMILY, qual2, 2L, value);
    table.put(put);

    // Check values
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals(5, result.size());

    // Delete row
    Delete delete = new Delete(rowKey);
    delete.deleteFamilyVersion(COLUMN_FAMILY, 2L);
    table.delete(delete);

    // Confirm results
    result = table.get(get);
    Assert.assertEquals("Three versions should remain", 3, result.size());
    Assert.assertTrue("Qual1 should have cells", result.containsColumn(COLUMN_FAMILY, qual1));
    Assert.assertTrue("Qual2 should have a cell", result.containsColumn(COLUMN_FAMILY, qual2));
    List<Cell> cells1 = result.getColumnCells(COLUMN_FAMILY, qual1);
    Assert.assertEquals("Qual1 should have 2 cells", 2, cells1.size());
    Assert.assertEquals("Version 3 should be the latest version", 3L, cells1.get(0).getTimestamp());
    Assert.assertEquals("Version 1 should be the oldest version", 1L, cells1.get(1).getTimestamp());
    List<Cell> cells2 = result.getColumnCells(COLUMN_FAMILY, qual2);
    Assert.assertEquals("Qual2 should have 1 cell", 1, cells2.size());
    Assert.assertEquals("Version 1 should be the latest version", 1L, cells2.get(0).getTimestamp());

    table.close();
  }
}
