/*
 * Copyright (c) 2014 Google Inc.
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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;

public class TestCheckAndMutate extends AbstractTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Requirement 7.1 - Atomically attempt a mutation, dependent on a successful value check within
   * the same row.
   *
   * Requirement 7.3 - Pass a null value to check for the non-existence of a column.
   */
  @Test
  public void testCheckAndPutSameQual() throws IOException {
    // Initialize
    try (Table table = getConnection().getTable(TABLE_NAME)) {
      testCheckAndMutate(dataHelper, table);
    }
  }

  public static void testCheckAndMutate(DataGenerationHelper dataHelper, Table table) throws IOException {
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Put with a bad check on a null value, then try with a good one
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value1);
    boolean success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, value2, put);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, null, put);
    Assert.assertTrue(success);

    // Fail on null check, now there's a value there
    put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value2);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, null, put);
    Assert.assertFalse("Null check should fail", success);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, value2, put);
    Assert.assertFalse("Wrong value should fail", success);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, value1, put);
    Assert.assertTrue(success);

    // Check results
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("Should be two results", 2, result.size());
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(cells.get(0)));
    Assert.assertArrayEquals(value1, CellUtil.cloneValue(cells.get(1)));
  }

  /**
   * Further tests for requirements 7.1 and 7.3.
   */
  @Test
  public void testCheckAndDeleteSameQual() throws IOException {
    // Initialize
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Delete all previous versions if the value is found.
    Delete delete = new Delete(rowKey).addColumns(COLUMN_FAMILY, qual);
    boolean success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual, value1, delete);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual, null, delete);

    // Add a value and check again
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value1);
    table.put(put);
    success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual, value2, delete);
    Assert.assertFalse("Wrong value.  Should fail.", success);
    success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual, value1, delete);
    Assert.assertTrue(success);
    Assert.assertFalse("Row should be gone", table.exists(new Get(rowKey)));

    table.close();
  }

  /**
   * Further tests for requirements 7.1 and 7.3.
   */
  @Test
  public void testCheckAndPutDiffQual() throws IOException {
    // Initialize
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual1 = dataHelper.randomData("qualifier-");
    byte[] qual2 = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Put then again
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual1, value1);
    boolean success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual2, value2, put);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual2, null, put);
    Assert.assertTrue(success);

    // Fail on null check, now there's a value there
    put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual2, value2);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual1, null, put);
    Assert.assertFalse("Null check should fail", success);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual1, value2, put);
    Assert.assertFalse("Wrong value should fail", success);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual1, value1, put);
    Assert.assertTrue(success);

    // Check results
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("Should be two results", 2, result.size());
    Assert.assertArrayEquals(value1, CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY,
      qual2)));

    table.close();
  }

  /**
   * Further tests for requirements 7.1 and 7.3.
   */
  @Test
  public void testCheckAndDeleteDiffQual() throws IOException {
    // Initialize
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual1 = dataHelper.randomData("qualifier-");
    byte[] qual2 = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Delete all versions of a column if the latest version matches
    Delete delete = new Delete(rowKey).addColumns(COLUMN_FAMILY, qual1);
    boolean success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual2, value2, delete);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual2, null, delete);
    Assert.assertTrue(success);

    // Add a value now
    Put put = new Put(rowKey)
        .addColumn(COLUMN_FAMILY, qual1, value1)
        .addColumn(COLUMN_FAMILY, qual2, value2);
    table.put(put);

    // Fail on null check, now there's a value there
    success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual2, null, delete);
    Assert.assertFalse("Null check should fail", success);
    success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual2, value1, delete);
    Assert.assertFalse("Wrong value should fail", success);
    success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual2, value2, delete);
    Assert.assertTrue(success);
    delete = new Delete(rowKey).addColumns(COLUMN_FAMILY, qual2);
    success = table.checkAndDelete(rowKey, COLUMN_FAMILY, qual1, null, delete);
    Assert.assertTrue(success);
    Assert.assertFalse("Row should be gone", table.exists(new Get(rowKey)));

    table.close();
  }

  /**
   * Requirement 7.2 - Throws an IOException if the check is for a row other than the one in the
   * mutation attempt.
   */
  @Test
  public void testCheckAndPutDiffRow() throws IOException {
    // Initialize
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey1 = dataHelper.randomData("rowKey-");
    byte[] rowKey2 = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value = dataHelper.randomData("value-");

    // Put then again
    Put put = new Put(rowKey1).addColumn(COLUMN_FAMILY, qual, value);
    expectedException.expect(DoNotRetryIOException.class);
    expectedException.expectMessage("Action's getRow must match the passed row");
    table.checkAndPut(rowKey2, COLUMN_FAMILY, qual, null, put);

    table.close();
  }

  @Test
  public void testCheckAndDeleteDiffRow() throws IOException {
    // Initialize
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey1 = dataHelper.randomData("rowKey-");
    byte[] rowKey2 = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");

    // Put then again
    Delete delete = new Delete(rowKey1).addColumns(COLUMN_FAMILY, qual);
    expectedException.expect(DoNotRetryIOException.class);
    expectedException.expectMessage("Action's getRow must match the passed row");
    table.checkAndDelete(rowKey2, COLUMN_FAMILY, qual, null, delete);

    table.close();
  }

  @Test
  public void testCheckAndMutate() throws IOException {
    // Initialize
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualCheck = dataHelper.randomData("qualifier-");
    byte[] qualPut = dataHelper.randomData("qualifier-");
    byte[] qualDelete = dataHelper.randomData("qualifier-");
    byte[] valuePut = dataHelper.randomData("value-");
    byte[] valueCheck = dataHelper.randomData("value-");

    // Delete all versions of a column if the latest version matches
    Delete delete = new Delete(rowKey).addColumns(COLUMN_FAMILY, qualDelete);
    RowMutations rm = new RowMutations(rowKey);
    rm.add(new Put(rowKey).addColumn(COLUMN_FAMILY, qualPut, valuePut));
    rm.add(new Delete(rowKey).addColumns(COLUMN_FAMILY, qualDelete));

    boolean success = table.checkAndMutate(
        rowKey, COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, valueCheck, rm);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndMutate(rowKey, COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, null, rm);
    Assert.assertTrue(success);

    // Add a value now
    Put put = new Put(rowKey)
        .addColumn(COLUMN_FAMILY, qualCheck, valueCheck)
        .addColumn(COLUMN_FAMILY, qualDelete, Bytes.toBytes("todelete"));
    table.put(put);
    // Fail on null check, now there's a value there
    success = table.checkAndMutate(rowKey, COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, null, rm);
    Assert.assertFalse("Null check should fail", success);
    // valuePut is in qualPut and not in qualCheck so this will fail:
    success = table.checkAndMutate(
        rowKey, COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, valuePut, rm);
    Assert.assertFalse("Wrong value should fail", success);
    success = table.checkAndMutate(
        rowKey, COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, valueCheck, rm);
    Assert.assertTrue(success);

    Result row = table.get(new Get(rowKey).addFamily(COLUMN_FAMILY));
    // QualCheck and QualPut should exist
    Assert.assertEquals(2, row.size());
    Assert.assertFalse(
        "QualDelete should be deleted",
        row.containsColumn(COLUMN_FAMILY, qualDelete));
    Assert.assertTrue(
        "QualPut should exist",
        row.containsColumn(COLUMN_FAMILY, qualPut));
    table.close();
  }
}
