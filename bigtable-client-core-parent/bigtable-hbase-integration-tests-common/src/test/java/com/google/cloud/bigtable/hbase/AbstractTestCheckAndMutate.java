/*
 * Copyright 2015 Google LLC
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
import java.io.IOException;
import java.util.List;
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
import org.junit.Test;

public abstract class AbstractTestCheckAndMutate extends AbstractTest {

  private static final byte[] ZERO_BYTES = new byte[0];

  /**
   * Requirement 7.1 - Atomically attempt a mutation, dependent on a successful value check within
   * the same row.
   *
   * <p>Requirement 7.3 - Pass a null value to check for the non-existence of a column.
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
      Put put1 = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value1);
      Assert.assertFalse(
          "Column doesn't exist. Should fail.",
          checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value2, put1));
      Assert.assertTrue(
          "Column should be created on zero bytes.",
          checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, ZERO_BYTES, put1));
      Assert.assertFalse(checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, null, put1));

      // Fail on null check, now there's a value there
      Put put2 = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value2);
      Assert.assertFalse(
          "Null check should fail",
          checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, null, put2));
      Assert.assertFalse(
          "Wrong value should fail",
          checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value2, put2));
      Assert.assertFalse(
          "Zero bytes check should fail",
          checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, ZERO_BYTES, put2));
      Assert.assertTrue(checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value1, put2));

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

  protected abstract boolean checkAndPut(
      byte[] rowKey, byte[] columnFamily, byte[] qual, byte[] value, Put put) throws Exception;

  protected abstract boolean checkAndPut(
      byte[] rowKey,
      byte[] columnFamily,
      byte[] qualToCheck,
      CompareOp op,
      byte[] bytes,
      Put someRandomPut)
      throws Exception;

  protected abstract boolean checkAndDelete(
      byte[] rowKey, byte[] columnFamily, byte[] qual, byte[] value, Delete delete)
      throws Exception;

  protected abstract boolean checkAndMutate(
      byte[] rowKey, byte[] columnFamily, byte[] qual, CompareOp op, byte[] value, RowMutations rm)
      throws Exception;

  /** Further tests for requirements 7.1 and 7.3. */
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
    Assert.assertFalse(
        "Column doesn't exist. Should fail.",
        checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value1, delete));
    Assert.assertTrue(checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, null, delete));

    // Add a value and check again
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value1);
    table.put(put);
    Assert.assertFalse(
        "Wrong value. Should fail.",
        checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value2, delete));

    // Newer versions of HBase throw an error instead of returning false
    Exception actualError = null;
    Boolean result = null;
    try {
      result = checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, ZERO_BYTES, delete);
    } catch (Exception e) {
      actualError = e;
    }

    Assert.assertTrue("Zero bytes value. Should fail.", actualError != null || !result);
    Assert.assertTrue(
        checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual, value1, delete));
    Assert.assertFalse("Row should be gone", table.exists(new Get(rowKey)));

    table.close();
  }

  /** Further tests for requirements 7.1 and 7.3. */
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
    Put put1 = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual1, value1);
    Assert.assertFalse(
        "Column doesn't exist. Should fail.",
        checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, value2, put1));
    Assert.assertTrue(
        "Column should be created on zero bytes.",
        checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, ZERO_BYTES, put1));
    Assert.assertTrue(checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, null, put1));

    // Fail on null check, now there's a value there
    Put put2 = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual2, value2);
    Assert.assertFalse(
        "Null check should fail",
        checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual1, null, put2));
    Assert.assertFalse(
        "Wrong value should fail",
        checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual1, value2, put2));
    Assert.assertTrue(checkAndPut(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual1, value1, put2));

    // Check results
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals(3, result.size());
    Assert.assertArrayEquals(
        value1,
        CellUtil.cloneValue(result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals(
        value2,
        CellUtil.cloneValue(result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qual2)));

    table.close();
  }

  /** Further tests for requirements 7.1 and 7.3. */
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
    Assert.assertFalse(
        "Column doesn't exist. Should fail.",
        checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, value2, delete));
    Assert.assertTrue(
        "Column should be created on zero bytes.",
        checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, ZERO_BYTES, delete));
    Assert.assertTrue(checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, null, delete));

    // Add a value now
    Put put =
        new Put(rowKey)
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual1, value1)
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual2, value2);
    table.put(put);

    // Fail on null check, now there's a value there
    Assert.assertFalse(
        "Null check should fail",
        checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, null, delete));
    Assert.assertFalse(
        "Wrong value should fail",
        checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, value1, delete));
    Assert.assertFalse(
        "Zero bytes check should fail",
        checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, ZERO_BYTES, delete));
    Assert.assertTrue(
        checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual2, value2, delete));
    delete = new Delete(rowKey).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qual2);
    Assert.assertTrue(checkAndDelete(rowKey, SharedTestEnvRule.COLUMN_FAMILY, qual1, null, delete));
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
    Assert.assertThrows(
        IOException.class,
        () -> checkAndPut(rowKey2, SharedTestEnvRule.COLUMN_FAMILY, qual, null, put));
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
      Assert.assertThrows(
          IOException.class,
          () -> checkAndDelete(rowKey2, SharedTestEnvRule.COLUMN_FAMILY, qual, null, delete));
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

    Assert.assertFalse(
        "Column doesn't exist. Should fail.",
        checkAndMutate(
            rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, valueCheck, rm));
    Assert.assertTrue(
        "Column should be created on zero bytes",
        checkAndMutate(
            rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, ZERO_BYTES, rm));
    Assert.assertTrue(
        checkAndMutate(
            rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, null, rm));

    // Add a value now
    Put put =
        new Put(rowKey)
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualCheck, valueCheck)
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualDelete, Bytes.toBytes("todelete"));
    table.put(put);
    // Fail on null check, now there's a value there
    Assert.assertFalse(
        "Null check should fail",
        checkAndMutate(
            rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, null, rm));
    // valuePut is in qualPut and not in qualCheck so this will fail:
    Assert.assertFalse(
        "Wrong value should fail",
        checkAndMutate(
            rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, valuePut, rm));
    Assert.assertTrue(
        checkAndMutate(
            rowKey, SharedTestEnvRule.COLUMN_FAMILY, qualCheck, CompareOp.EQUAL, valueCheck, rm));

    Result row = table.get(new Get(rowKey).addFamily(SharedTestEnvRule.COLUMN_FAMILY));
    // QualCheck and QualPut should exist
    Assert.assertEquals(2, row.size());
    Assert.assertFalse(
        "QualDelete should be deleted",
        row.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, qualDelete));
    Assert.assertTrue(
        "QualPut should exist", row.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, qualPut));
    table.close();
  }

  @Test
  public void testCompareOps() throws Exception {
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualToCheck = dataHelper.randomData("toCheck-");
    byte[] otherQual = dataHelper.randomData("other-");

    Table table = getDefaultTable();

    table.put(
        new Put(rowKey)
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck, Bytes.toBytes(2000l)));

    Put someRandomPut =
        new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, otherQual, Bytes.toBytes(1l));

    Assert.assertTrue(
        "1000 < 2000 should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.LESS,
            Bytes.toBytes(1000l),
            someRandomPut));

    Assert.assertFalse(
        "4000 < 2000 should fail",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.LESS,
            Bytes.toBytes(4000l),
            someRandomPut));

    Assert.assertFalse(
        "1000 > 2000 should fail",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.GREATER,
            Bytes.toBytes(1000l),
            someRandomPut));

    Assert.assertTrue(
        "4000 > 2000 should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.GREATER,
            Bytes.toBytes(4000l),
            someRandomPut));

    Assert.assertTrue(
        "1000 <= 2000 should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.LESS_OR_EQUAL,
            Bytes.toBytes(1000l),
            someRandomPut));

    Assert.assertFalse(
        "4000 <= 2000 should fail",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.LESS_OR_EQUAL,
            Bytes.toBytes(4000l),
            someRandomPut));

    Assert.assertFalse(
        "1000 >= 2000 should fail",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.GREATER_OR_EQUAL,
            Bytes.toBytes(1000l),
            someRandomPut));

    Assert.assertTrue(
        "4000 >= 2000 should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.GREATER_OR_EQUAL,
            Bytes.toBytes(4000l),
            someRandomPut));

    Assert.assertFalse(
        "1000 == 2000 should fail",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.EQUAL,
            Bytes.toBytes(1000l),
            someRandomPut));

    Assert.assertTrue(
        "2000 == 2000 should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.EQUAL,
            Bytes.toBytes(2000l),
            someRandomPut));

    Assert.assertFalse(
        "2000 != 2000 should fail",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.NOT_EQUAL,
            Bytes.toBytes(2000l),
            someRandomPut));

    Assert.assertTrue(
        "4000 != 2000 should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.NOT_EQUAL,
            Bytes.toBytes(4000l),
            someRandomPut));

    Assert.assertTrue(
        "4000 > MIN_BYTES should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.GREATER_OR_EQUAL,
            new byte[] {Byte.MIN_VALUE},
            someRandomPut));
  }

  @Test
  public void testCompareOpsNull() throws Exception {
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] nullQual = dataHelper.randomData("null-");
    byte[] popluatedQual = dataHelper.randomData("pupulated-");
    byte[] otherQual = dataHelper.randomData("other-");

    Table table = getDefaultTable();

    table.put(
        new Put(rowKey)
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, popluatedQual, Bytes.toBytes(2000l)));

    Put someRandomPut =
        new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, otherQual, Bytes.toBytes(1l));

    Assert.assertTrue(
        "< null should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            nullQual,
            CompareOp.LESS,
            null,
            someRandomPut));

    Assert.assertTrue(
        "> null should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            nullQual,
            CompareOp.GREATER,
            null,
            someRandomPut));

    Assert.assertTrue(
        "<= null should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            nullQual,
            CompareOp.LESS_OR_EQUAL,
            null,
            someRandomPut));

    Assert.assertTrue(
        ">= null should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            nullQual,
            CompareOp.GREATER_OR_EQUAL,
            null,
            someRandomPut));

    Assert.assertTrue(
        "== null should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            nullQual,
            CompareOp.EQUAL,
            null,
            someRandomPut));

    Assert.assertFalse(
        "> MIN_BYTES should fail",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            nullQual,
            CompareOp.GREATER_OR_EQUAL,
            new byte[] {Byte.MIN_VALUE},
            someRandomPut));
  }

  /**
   * Bigtable supports a notion of `NOT_EQUALS null` checks equating with `check for existence`.
   * HBase does not support this notion, and always uses `{any comparator} null` to mean `check for
   * non-existence`.
   */
  @Test
  public void testNotEqualsNull_BigtableOnly() throws Exception {
    if (!sharedTestEnv.isBigtable()) {
      return;
    }

    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] nullQual = dataHelper.randomData("null-");
    byte[] popluatedQual = dataHelper.randomData("pupulated-");
    byte[] otherQual = dataHelper.randomData("other-");

    Table table = getDefaultTable();

    table.put(
        new Put(rowKey)
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, popluatedQual, Bytes.toBytes(2000l)));

    Put someRandomPut =
        new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, otherQual, Bytes.toBytes(1l));

    // This doesn't work in HBase, but we need this to work in CBT
    Assert.assertFalse(
        "!= null should fail",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            nullQual,
            CompareOp.NOT_EQUAL,
            null,
            someRandomPut));

    // This doesn't work in HBase, but we need this to work in CBT
    Assert.assertTrue(
        "2000 != null should succeed",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            popluatedQual,
            CompareOp.NOT_EQUAL,
            null,
            someRandomPut));
  }

  @Test
  public void testCompareOpsVersions() throws Exception {
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualToCheck = dataHelper.randomData("toCheck-");
    byte[] otherQual = dataHelper.randomData("other-");

    Table table = getDefaultTable();
    Put someRandomPut =
        new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, otherQual, Bytes.toBytes(1l));

    table.put(
        new Put(rowKey, System.currentTimeMillis() - 10000)
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck, Bytes.toBytes(2000l)));

    table.put(
        new Put(rowKey, System.currentTimeMillis())
            .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck, Bytes.toBytes(4000l)));

    Assert.assertFalse(
        "3000 > 4000 should fail",
        checkAndPut(
            rowKey,
            SharedTestEnvRule.COLUMN_FAMILY,
            qualToCheck,
            CompareOp.GREATER,
            Bytes.toBytes(3000l),
            someRandomPut));
  }
}
