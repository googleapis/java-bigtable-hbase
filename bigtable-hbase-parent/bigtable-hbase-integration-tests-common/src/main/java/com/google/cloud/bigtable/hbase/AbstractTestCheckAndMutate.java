/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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

public abstract class AbstractTestCheckAndMutate extends AbstractTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Requirement 7.1 - Atomically attempt a mutation, dependent on a successful value check within
   * the same row.
   *
   * Requirement 7.3 - Pass a null value to check for the non-existence of a column.
   */
  @Test
  public void testCheckAndPutSameQual() throws Exception {
    // Initialize
    try (Table table = getDefaultTable()) {
      byte[] rowKey = dataHelper.randomData("rowKey-");
      byte[] qual = dataHelper.randomData("qualifier-");
      byte[] value1 = dataHelper.randomData("value-");
      byte[] value2 = dataHelper.randomData("value-");

      // Put with a bad check on a null value, then try with a good one
      Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value1);
      boolean success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value2, put);
      Assert.assertFalse("Column doesn't exist.  Should fail.", success);
      success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, null, put);
      Assert.assertTrue(success);

      // Fail on null check, now there's a value there
      put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value2);
      success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, null, put);
      Assert.assertFalse("Null check should fail", success);
      success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value2, put);
      Assert.assertFalse("Wrong value should fail", success);
      success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value1, put);
      Assert.assertTrue(success);

      // Check results
      Get get = new Get(rowKey);
      get.setMaxVersions(5);
      Result result = table.get(get);
      Assert.assertEquals("Should be two results", 2, result.size());
      List<Cell> cells = result.getColumnCells(SharedTestEnvRule.COLUMN_FAMILY, qual);
      Assert.assertArrayEquals(value2, CellUtil.cloneValue(cells.get(0)));
      Assert.assertArrayEquals(value1, CellUtil.cloneValue(cells.get(1)));
    }
  }

  protected abstract boolean checkAndPut(byte[] rowKey, byte[] columnFamily, byte[] qual,
      byte[] value, Put put) throws Exception;

  protected abstract boolean checkAndPut(byte[] rowKey, byte[] columnFamily, byte[] qualToCheck,
      CompareOp op, byte[] bytes, Put someRandomPut) throws Exception;

  protected abstract boolean checkAndDelete(byte[] rowKey, byte[] columnFamily, byte[] qual,
      byte[] value, Delete delete) throws Exception;

  protected abstract boolean checkAndMutate(byte[] rowKey, byte[] columnFamily, byte[] qual,
      CompareOp op, byte[] value, RowMutations rm) throws Exception;

  /**
   * Further tests for requirements 7.1 and 7.3.
   */
  @Test
  public void testCheckAndDeleteSameQual() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Delete all previous versions if the value is found.
    Delete delete = new Delete(rowKey).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qual);
    boolean success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value1, delete);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, null, delete);

    // Add a value and check again
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value1);
    table.put(put);
    success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value2, delete);
    Assert.assertFalse("Wrong value.  Should fail.", success);
    success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value1, delete);
    Assert.assertTrue(success);
    Assert.assertFalse("Row should be gone", table.exists(new Get(rowKey)));

    table.close();
  }

  /**
   * Further tests for requirements 7.1 and 7.3.
   */
  @Test
  public void testCheckAndPutDiffQual() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual1 = dataHelper.randomData("qualifier-");
    byte[] qual2 = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Put then again
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual1, value1);
    boolean success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, value2, put);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, null, put);
    Assert.assertTrue(success);

    // Fail on null check, now there's a value there
    put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual2, value2);
    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual1, null, put);
    Assert.assertFalse("Null check should fail", success);
    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual1, value2, put);
    Assert.assertFalse("Wrong value should fail", success);
    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual1, value1, put);
    Assert.assertTrue(success);

    // Check results
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("Should be two results", 2, result.size());
    Assert.assertArrayEquals(value1, CellUtil.cloneValue(result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY,
      qual2)));

    table.close();
  }

  /**
   * Further tests for requirements 7.1 and 7.3.
   */
  @Test
  public void testCheckAndDeleteDiffQual() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual1 = dataHelper.randomData("qualifier-");
    byte[] qual2 = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Delete all versions of a column if the latest version matches
    Delete delete = new Delete(rowKey).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qual1);
    boolean success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, value2, delete);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, null, delete);
    Assert.assertTrue(success);

    // Add a value now
    Put put = new Put(rowKey)
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual1, value1)
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual2, value2);
    table.put(put);

    // Fail on null check, now there's a value there
    success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, null, delete);
    Assert.assertFalse("Null check should fail", success);
    success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, value1, delete);
    Assert.assertFalse("Wrong value should fail", success);
    success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, value2, delete);
    Assert.assertTrue(success);
    delete = new Delete(rowKey).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qual2);
    success = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual1, null, delete);
    Assert.assertTrue(success);
    Assert.assertFalse("Row should be gone", table.exists(new Get(rowKey)));

    table.close();
  }

  /**
   * Requirement 7.2 - Throws an IOException if the check is for a row other than the one in the
   * mutation attempt.
   */
  @Test
  public void testCheckAndPutDiffRow() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey1 = dataHelper.randomData("rowKey-");
    byte[] rowKey2 = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value = dataHelper.randomData("value-");

    // Put then again
    Put put = new Put(rowKey1).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value);
    expectedException.expect(IOException.class);
    expectedException.expectMessage("Action's getRow must match the passed row");
    checkAndPut(rowKey2, SharedTestEnvRule.COLUMN_FAMILY, qual, null, put);

    table.close();
  }

  @Test
  public void testCheckAndDeleteDiffRow() throws Exception {
    // Initialize
    try (Table table = getDefaultTable()) {
      byte[] rowKey1 = dataHelper.randomData("rowKey-");
      byte[] rowKey2 = dataHelper.randomData("rowKey-");
      byte[] qual = dataHelper.randomData("qualifier-");

      // Put then again
      Delete delete = new Delete(rowKey1).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qual);
      checkAndDelete(rowKey2, SharedTestEnvRule.COLUMN_FAMILY, qual, null, delete);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Action's getRow must match the passed row"));
    }
  }

  @Test
  public void testCheckAndMutate() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualCheck = dataHelper.randomData("qualifier-");
    byte[] qualPut = dataHelper.randomData("qualifier-");
    byte[] qualDelete = dataHelper.randomData("qualifier-");
    byte[] valuePut = dataHelper.randomData("value-");
    byte[] valueCheck = dataHelper.randomData("value-");

    // Delete all versions of a column if the latest version matches
    RowMutations rm = new RowMutations(rowKey);
    rm.add(new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualPut, valuePut));
    rm.add(new Delete(rowKey).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qualDelete));

    boolean success = checkAndMutate(
        rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, valueCheck, rm);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, null, rm);
    Assert.assertTrue(success);

    // Add a value now
    Put put = new Put(rowKey)
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualCheck, valueCheck)
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualDelete, Bytes.toBytes("todelete"));
    table.put(put);
    // Fail on null check, now there's a value there
    success = checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, null, rm);
    Assert.assertFalse("Null check should fail", success);
    // valuePut is in qualPut and not in qualCheck so this will fail:
    success = checkAndMutate(
        rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, valuePut, rm);
    Assert.assertFalse("Wrong value should fail", success);
    success = checkAndMutate(
        rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, valueCheck, rm);
    Assert.assertTrue(success);

    Result row = table.get(new Get(rowKey).addFamily(SharedTestEnvRule.COLUMN_FAMILY));
    // QualCheck and QualPut should exist
    Assert.assertEquals(2, row.size());
    Assert.assertFalse(
        "QualDelete should be deleted",
        row.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, qualDelete));
    Assert.assertTrue(
        "QualPut should exist",
        row.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, qualPut));
    table.close();
  }

  @Test
  public void testCompareOps() throws Exception {
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualToCheck = dataHelper.randomData("toCheck-");
    byte[] otherQual = dataHelper.randomData("other-");
    boolean success;

    Table table = getDefaultTable();

    table.put(new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      Bytes.toBytes(2000l)));

    Put someRandomPut =
        new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, otherQual, Bytes.toBytes(1l));

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.LESS, Bytes.toBytes(1000l), someRandomPut);
    Assert.assertTrue("1000 < 2000 should succeed", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.LESS, Bytes.toBytes(4000l), someRandomPut);
    Assert.assertFalse("4000 < 2000 should fail", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.GREATER, Bytes.toBytes(1000l), someRandomPut);
    Assert.assertFalse("1000 > 2000 should fail", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.GREATER, Bytes.toBytes(4000l), someRandomPut);
    Assert.assertTrue("4000 > 2000 should succeed", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.LESS_OR_EQUAL, Bytes.toBytes(1000l), someRandomPut);
    Assert.assertTrue("1000 <= 2000 should succeed", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.LESS_OR_EQUAL, Bytes.toBytes(4000l), someRandomPut);
    Assert.assertFalse("4000 <= 2000 should fail", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(1000l), someRandomPut);
    Assert.assertFalse("1000 >= 2000 should fail", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(4000l), someRandomPut);
    Assert.assertTrue("4000 >= 2000 should succeed", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.EQUAL, Bytes.toBytes(1000l), someRandomPut);
    Assert.assertFalse("1000 == 2000 should fail", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.EQUAL, Bytes.toBytes(2000l), someRandomPut);
    Assert.assertTrue("2000 == 2000 should succeed", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.NOT_EQUAL, Bytes.toBytes(2000l), someRandomPut);
    Assert.assertFalse("2000 != 2000 should fail", success);

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.NOT_EQUAL, Bytes.toBytes(4000l), someRandomPut);
    Assert.assertTrue("4000 != 2000 should succeed", success);

    if (sharedTestEnv.isBigtable()) {
      // This doesn't work in HBase, but we need this to work in CBT
      success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
        CompareOp.NOT_EQUAL, null, someRandomPut);
      Assert.assertTrue("4000 != null should succeed", success);
    }
  }

  @Test
  public void testCompareOpsVersions() throws Exception {
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualToCheck = dataHelper.randomData("toCheck-");
    byte[] otherQual = dataHelper.randomData("other-");
    boolean success;

    Table table = getDefaultTable();
    Put someRandomPut =
        new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, otherQual, Bytes.toBytes(1l));

    table.put(new Put(rowKey, System.currentTimeMillis() - 10000)
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck, Bytes.toBytes(2000l)));

    table.put(new Put(rowKey, System.currentTimeMillis()).addColumn(SharedTestEnvRule.COLUMN_FAMILY,
      qualToCheck, Bytes.toBytes(4000l)));

    success = checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      CompareOp.GREATER, Bytes.toBytes(3000l), someRandomPut);
    Assert.assertFalse("3000 > 4000 should fail", success);
  }
}
