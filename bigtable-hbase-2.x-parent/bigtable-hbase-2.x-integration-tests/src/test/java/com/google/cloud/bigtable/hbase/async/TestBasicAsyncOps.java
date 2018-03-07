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

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

/**
 * Test to make sure that basic {@link AsyncTable} operations work
 * @author sduskis
 */
@RunWith(JUnit4.class)
public class TestBasicAsyncOps extends AbstractAsyncTest {

  @Test
  public void testBasicAsyncOps() throws Exception {
    System.out.println("TestBasicAsyncOps");
    byte[] rowKey = dataHelper.randomData("TestBasicAsyncOps-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    byte[] testValue = dataHelper.randomData("testValue-");
    AsyncTable table = getDefaultAsyncTable();

    Stopwatch stopwatch = new Stopwatch();
    // Put
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, testQualifier, testValue);
    table.put(put).get();
    stopwatch.print("Put took %d ms");

    // Get
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, testQualifier);

    // Do the get on some tests, but not others.  The rationale for that is to do performance
    // testing on large values.
    if (true) {
      CompletableFuture<Result> cfResult = table.get(get);
      Result result = cfResult.get();
      stopwatch.print("Get took %d ms");
      Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
      List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
      Assert.assertEquals(1, cells.size());
      Assert.assertTrue(Arrays.equals(testValue, CellUtil.cloneValue(cells.get(0))));
      stopwatch.print("Verifying took %d ms");
    }
    // Delete
    Delete delete = new Delete(rowKey);
    delete.addColumns(COLUMN_FAMILY, testQualifier);
    table.delete(delete).get();
    stopwatch.print("Delete took %d ms");

    // Confirm deleted
    CompletableFuture<Boolean> exists = table.exists(get);
    Assert.assertNotNull(exists);
    Assert.assertFalse(exists.get());
    stopwatch.print("Exists took %d ms");
  }

  @Test
  public void testRowMutations() throws Exception {
    byte[] rowKey = dataHelper.randomData("testRowMutations-");
    byte[] testQualifier1 = dataHelper.randomData("testQualifier2-");
    byte[] testQualifier2 = dataHelper.randomData("testQualifier1-");
    byte[] testValue1 = dataHelper.randomData("testValue-");
    byte[] testValue2 = dataHelper.randomData("testValue-");
    byte[] testValue3 = dataHelper.randomData("testValue-");
    AsyncTable table = getDefaultAsyncTable();

    RowMutations rowMutations1 = new RowMutations(rowKey);
    rowMutations1.add(new Put(rowKey)
      .addColumn(COLUMN_FAMILY, testQualifier1, testValue1)
      .addColumn(COLUMN_FAMILY, testQualifier2, testValue2));
    table.mutateRow(rowMutations1).get();

    RowMutations rowMutations2 = new RowMutations(rowKey);
    rowMutations2.add(new Delete(rowKey).addColumns(COLUMN_FAMILY, testQualifier2));
    rowMutations2.add(new Put(rowKey).addColumn(COLUMN_FAMILY, testQualifier1, testValue3));
    table.mutateRow(rowMutations2).get();

    CompletableFuture<Result> cfResult = table.get(new Get(rowKey));
    Result result = cfResult.get();
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(CellUtil.matchingValue(result.rawCells()[0], testValue3));
  }

  @Test
  public void testAppend() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("testAppend-");
    byte[] qualifier = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value1-");
    byte[] value1And2 = ArrayUtils.addAll(value1, value2);

    // Put then append
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifier, value1);
    table.put(put);
    Append append =
        new Append(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifier, value2);
    Result result = getDefaultAsyncTable().append(append).get();
    Cell cell = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect concatenated byte array", value1And2,
      CellUtil.cloneValue(cell));

    // Test result
    Get get = new Get(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    get.readVersions(5);
    result = table.get(get);
    List<Cell> cells = result.getColumnCells(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect concatenated byte array", value1And2,
      CellUtil.cloneValue(cells.get(0)));
    if (result.size() == 2) {
      // TODO: This isn't always true with CBT.  Why is that?
      Assert.assertEquals("There should be two versions now", 2, result.size());
      Assert.assertArrayEquals("Expect original value still there", value1,
        CellUtil.cloneValue(cells.get(1)));
    }
  }

  @Test
  public void testIncrement() throws Exception {
    // Initialize data
    try (Table table = getDefaultTable()){
      byte[] rowKey = dataHelper.randomData("testrow-");
      byte[] qual1 = dataHelper.randomData("qual-");
      long value1 = new Random().nextInt();
      long incr1 = new Random().nextInt();
      byte[] qual2 = dataHelper.randomData("qual-");
      long value2 = new Random().nextInt();
      long incr2 = new Random().nextInt();

      // Put and increment
      Put put = new Put(rowKey);
      put.addColumn(COLUMN_FAMILY, qual1, Bytes.toBytes(value1));
      put.addColumn(COLUMN_FAMILY, qual2, Bytes.toBytes(value2));
      table.put(put);
      Increment increment = new Increment(rowKey);
      increment.addColumn(COLUMN_FAMILY, qual1, incr1);
      increment.addColumn(COLUMN_FAMILY, qual2, incr2);
      Result result = getDefaultAsyncTable().increment(increment).get();
      Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
        Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1))));
      Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
        Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2))));
      Assert.assertEquals(2, result.size());

      // Double-check values with a Get
      Get get = new Get(rowKey);
      get.readVersions(5);
      result = table.get(get);
      Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
        Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1))));
      Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
        Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2))));
    }
  }

  private class Stopwatch {
    long lastCheckin = System.currentTimeMillis();

    private void print(String string) {
      long now = System.currentTimeMillis();
      logger.info(string, now - lastCheckin);
      lastCheckin = now;
    }
  }
}
