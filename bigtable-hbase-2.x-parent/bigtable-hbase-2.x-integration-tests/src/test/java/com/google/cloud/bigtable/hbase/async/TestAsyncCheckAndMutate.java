/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.async;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBase.CheckAndMutateBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(JUnit4.class)
public class TestAsyncCheckAndMutate extends AbstractAsyncTest {
  private static ExecutorService executor;
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setup() {
    executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TestAsyncCheckAndMutate").build());
  }

  @AfterClass
  public static void teardown() {
    executor.shutdownNow();
  }

  /**
   * Requirement 7.1 - Atomically attempt a mutation, dependent on a successful value check within
   * the same row.
   *
   * Requirement 7.3 - Pass a null value to check for the non-existence of a column.
   */
  @Test
  public void testCheckAndPutSameQual() throws Exception {
    // Initialize
    AsyncTable table = getDefaultAsyncTable(executor);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Put with a bad check on a null value, then try with a good one
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value1);
    boolean success = checkandMutate(table, rowKey, qual)
        .ifEquals(value2)
        .thenPut(put)
        .get();
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = checkandMutate(table, rowKey, qual)
        .ifNotExists()
        .thenPut(put)
        .get();

    Assert.assertTrue(success);

    // Fail on null check, now there's a value there
    put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value2);
    success = checkandMutate(table, rowKey, qual)
        .ifNotExists()
        .thenPut(put)
        .get();
    Assert.assertFalse("Null check should fail", success);
    success = checkandMutate(table, rowKey, qual)
        .ifEquals(value2)
        .thenPut(put)
        .get();
    Assert.assertFalse("Wrong value should fail", success);
    success = checkandMutate(table, rowKey, qual)
        .ifEquals(value1)
        .thenPut(put)
        .get();

    Assert.assertTrue(success);

    // Check results
    Get get = new Get(rowKey);
    get.readVersions(5);
    Result result = getDefaultTable().get(get);
    Assert.assertEquals("Should be two results", 2, result.size());
    List<Cell> cells = result.getColumnCells(SharedTestEnvRule.COLUMN_FAMILY, qual);
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(cells.get(0)));
    Assert.assertArrayEquals(value1, CellUtil.cloneValue(cells.get(1)));
  }

  private CheckAndMutateBuilder checkandMutate(AsyncTable table,
      byte[] rowKey, byte[] qual) {
    return table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY).qualifier(qual);
  }

  /**
   * Further tests for requirements 7.1 and 7.3.
   */
  @Test
  public void testCheckAndMutateSameQual() throws Exception {
    // Initialize
    AsyncTable table = getDefaultAsyncTable(executor);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Delete all previous versions if the value is found.
    Delete delete = new Delete(rowKey).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qual);
    boolean success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual)
        .ifEquals(value1)
        .thenDelete(delete)
        .get();
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual)
        .ifNotExists()
        .thenDelete(delete)
        .get();

    // Add a value and check again
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value1);
    getDefaultTable().put(put);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual)
        .ifEquals(value2)
        .thenDelete(delete)
        .get();
    Assert.assertFalse("Wrong value.  Should fail.", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual)
        .ifEquals(value1)
        .thenDelete(delete)
        .get();
    Assert.assertTrue(success);
    Assert.assertFalse("Row should be gone", getDefaultTable().exists(new Get(rowKey)));
  }

  /**
   * Further tests for requirements 7.1 and 7.3.
   */
  @Test
  public void testCheckAndPutDiffQual() throws Exception {
    // Initialize
    AsyncTable table = getDefaultAsyncTable(executor);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual1 = dataHelper.randomData("qualifier-");
    byte[] qual2 = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Put then again
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual1, value1);
    boolean success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual2)
        .ifEquals(value2)
        .thenPut(put)
        .get();
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual2)
        .ifNotExists()
        .thenPut(put)
        .get();
    Assert.assertTrue(success);

    // Fail on null check, now there's a value there
    put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual2, value2);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual1)
        .ifNotExists()
        .thenPut(put)
        .get();
    Assert.assertFalse("Null check should fail", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual1)
        .ifEquals(value2)
       .thenPut(put)
       .get();
    Assert.assertFalse("Wrong value should fail", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual1)
          .ifEquals(value1)
       .thenPut(put)
       .get();
    Assert.assertTrue(success);

    // Check results
    Get get = new Get(rowKey);
    get.readVersions(5);
    Result result = getDefaultTable().get(get);
    Assert.assertEquals("Should be two results", 2, result.size());
    Assert.assertArrayEquals(value1, CellUtil.cloneValue(result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY,
      qual2)));


  }

  /**
   * Further tests for requirements 7.1 and 7.3.
   */
  @Test
  public void testCheckAndMutateDiffQual() throws Exception {
    // Initialize
    AsyncTable table = getDefaultAsyncTable(executor);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual1 = dataHelper.randomData("qualifier-");
    byte[] qual2 = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Delete all versions of a column if the latest version matches
    Delete delete = new Delete(rowKey).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qual1);
    boolean success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual2)
        .ifEquals(value2)
        .thenDelete(delete)
        .get();
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
       .qualifier(qual2)
       .ifNotExists()
       .thenDelete(delete)
       .get();
    Assert.assertTrue(success);

    // Add a value now
    Put put = new Put(rowKey)
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual1, value1)
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual2, value2);
    getDefaultTable().put(put);

    // Fail on null check, now there's a value there
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual2)
        .ifNotExists()
        .thenDelete(delete)
        .get();
    Assert.assertFalse("Null check should fail", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual2)
        .ifEquals(value1)
        .thenDelete(delete)
        .get();
    Assert.assertFalse("Wrong value should fail", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual2)
        .ifEquals(value2)
        .thenDelete(delete)
        .get();
    Assert.assertTrue(success);
    delete = new Delete(rowKey).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qual2);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qual1)
        .ifNotExists()
        .thenDelete(delete)
        .get();
    Assert.assertTrue(success);
    Assert.assertFalse("Row should be gone", getDefaultTable().exists(new Get(rowKey)));
  }

  /**
   * Requirement 7.2 - Throws an IOException if the check is for a row other than the one in the
   * mutation attempt.
   */
  @Test
  public void testCheckAndPutDiffRow() throws Throwable {
    // Initialize
    AsyncTable table = getDefaultAsyncTable(executor);
    byte[] rowKey1 = dataHelper.randomData("rowKey-");
    byte[] rowKey2 = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value = dataHelper.randomData("value-");

    // Put then again
    Put put = new Put(rowKey1).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value);
    //Fix behavior as a part of Async Impl
    expectedException.expect(DoNotRetryIOException.class);
    expectedException.expectMessage("Action's getRow must match");
    try {
      table.checkAndMutate(rowKey2, SharedTestEnvRule.COLUMN_FAMILY)
          .qualifier(qual)
          .ifNotExists()
          .thenPut(put)
          .get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testCheckAndMutateDiffRow() throws Throwable {
    // Initialize
    AsyncTable table = getDefaultAsyncTable(executor);
    byte[] rowKey1 = dataHelper.randomData("rowKey-");
    byte[] rowKey2 = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");

    // Put then again
    Delete delete = new Delete(rowKey1).addColumns(SharedTestEnvRule.COLUMN_FAMILY, qual);
    expectedException.expect(DoNotRetryIOException.class);
    expectedException.expectMessage("Action's getRow must match");
    try {
      table.checkAndMutate(rowKey2, SharedTestEnvRule.COLUMN_FAMILY)
          .ifEquals(qual)
          .ifNotExists()
          .thenDelete(delete)
          .get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testCheckAndMutate() throws Exception {
    // Initialize
    AsyncTable table = getDefaultAsyncTable(executor);
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

    boolean success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
          .qualifier(qualCheck)
          .ifEquals(valueCheck)
          .thenMutate(rm)
          .get();
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
       .qualifier(qualCheck)
       .ifNotExists()
       .thenMutate(rm)
       .get();
    Assert.assertTrue(success);

    // Add a value now
    Put put = new Put(rowKey)
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualCheck, valueCheck)
        .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualDelete, Bytes.toBytes("todelete"));
    getDefaultTable().put(put);
    // Fail on null check, now there's a value there
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
       .qualifier(qualCheck)
       .ifNotExists()
       .thenMutate(rm)
       .get();
    Assert.assertFalse("Null check should fail", success);
    // valuePut is in qualPut and not in qualCheck so this will fail:
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualCheck)
        .ifEquals(valuePut)
        .thenMutate(rm)
        .get();
    Assert.assertFalse("Wrong value should fail", success);
    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualCheck)
        .ifEquals(valueCheck)
        .thenMutate(rm)
        .get();
    Assert.assertTrue(success);

    Result row = getDefaultTable().get(new Get(rowKey).addFamily(SharedTestEnvRule.COLUMN_FAMILY));
    // QualCheck and QualPut should exist
    Assert.assertEquals(2, row.size());
    Assert.assertFalse(
        "QualDelete should be deleted",
        row.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, qualDelete));
    Assert.assertTrue(
        "QualPut should exist",
        row.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, qualPut));
  }

  @Test
  public void testCompareOperators() throws Exception {
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualToCheck = dataHelper.randomData("toCheck-");
    byte[] otherQual = dataHelper.randomData("other-");
    boolean success;

    AsyncTable table = getDefaultAsyncTable(executor);

    getDefaultTable().put(new Put(rowKey)
      .addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck, Bytes.toBytes(2000l)));

    Put someRandomPut =
        new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, otherQual, Bytes.toBytes(1l));

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.LESS, Bytes.toBytes(1000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertTrue("1000 < 2000 should succeed", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.LESS, Bytes.toBytes(4000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertFalse("4000 < 2000 should fail", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.GREATER, Bytes.toBytes(1000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertFalse("1000 > 2000 should fail", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.GREATER, Bytes.toBytes(4000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertTrue("4000 > 2000 should succeed", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.LESS_OR_EQUAL, Bytes.toBytes(1000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertTrue("1000 <= 2000 should succeed", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.LESS_OR_EQUAL, Bytes.toBytes(4000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertFalse("4000 <= 2000 should fail", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes(1000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertFalse("1000 >= 2000 should fail", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes(4000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertTrue("4000 >= 2000 should succeed", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.EQUAL, Bytes.toBytes(1000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertFalse("1000 == 2000 should fail", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.EQUAL, Bytes.toBytes(2000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertTrue("2000 == 2000 should succeed", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.NOT_EQUAL, Bytes.toBytes(2000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertFalse("2000 != 2000 should fail", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.NOT_EQUAL, Bytes.toBytes(4000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertTrue("4000 != 2000 should succeed", success);

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.NOT_EQUAL, null)
        .thenPut(someRandomPut)
        .get();
    Assert.assertTrue("4000 != null should succeed", success);
  }

  @Test
  public void testCompareOperatorsVersions() throws Exception {
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualToCheck = dataHelper.randomData("toCheck-");
    byte[] otherQual = dataHelper.randomData("other-");
    boolean success;

    AsyncTable table = getDefaultAsyncTable(executor);
    Put someRandomPut =
        new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, otherQual, Bytes.toBytes(1l));

    long now = System.currentTimeMillis();
    Put put1 = new Put(rowKey, now - 10000).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      Bytes.toBytes(2000l));
    Put put2 = new Put(rowKey, now).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualToCheck,
      Bytes.toBytes(4000l));

    getDefaultTable().put(Arrays.asList(put1, put2));

    success = table.checkAndMutate(rowKey, SharedTestEnvRule.COLUMN_FAMILY)
        .qualifier(qualToCheck)
        .ifMatches(CompareOperator.GREATER, Bytes.toBytes(3000l))
        .thenPut(someRandomPut)
        .get();
    Assert.assertFalse("3000 > 4000 should fail", success);
  }
}
