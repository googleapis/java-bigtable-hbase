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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Test to make sure that basic {@link AsyncTable} operations work
 * @author sduskis
 */
@RunWith(JUnit4.class)
public class TestBasicAsyncOps extends AbstractAsyncTest {

  private static ExecutorService executor;

  @BeforeClass
  public static void setup() {
    executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("table-deleter").build());
  }

  @AfterClass
  public static void shutdown() {
    if (executor != null) {
      executor.shutdownNow();
    }
    executor = null;
  }

  @Test
  public void testBasicAsyncOps() throws Exception {
    System.out.println("TestBasicAsyncOps");
    byte[] rowKey = dataHelper.randomData("TestBasicAsyncOps-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    byte[] testValue = dataHelper.randomData("testValue-");
    testPutGetDelete(true, rowKey, testQualifier, testValue);
  }

  @Test
  public void testRowMutations() throws Exception {
    System.out.println("TestBasicAsyncOps");
    byte[] rowKey = dataHelper.randomData("testRowMutations-");
    byte[] testQualifier1 = dataHelper.randomData("testQualifier2-");
    byte[] testQualifier2 = dataHelper.randomData("testQualifier1-");
    byte[] testValue1 = dataHelper.randomData("testValue-");
    byte[] testValue2 = dataHelper.randomData("testValue-");
    byte[] testValue3 = dataHelper.randomData("testValue-");
    AsyncTable table = getAsyncConnection().getTable(sharedTestEnv.getDefaultTableName(), executor);

    RowMutations rowMutations1 = new RowMutations(rowKey);
    rowMutations1.add(new Put(rowKey)
      .addColumn(COLUMN_FAMILY, testQualifier1, testValue1)
      .addColumn(COLUMN_FAMILY, testQualifier2, testValue2));
    table.mutateRow(rowMutations1).get();

    RowMutations rowMutations2 = new RowMutations(rowKey);
    rowMutations2.add(new Delete(rowKey).addColumns(COLUMN_FAMILY, testQualifier2));
    rowMutations2.add(new Put(rowKey).addColumn(COLUMN_FAMILY, testQualifier1, testValue3));
    table.mutateRow(rowMutations2).get();

    Result result = table.get(new Get(rowKey)).get();
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(CellUtil.matchingValue(result.rawCells()[0], testValue3));
  }

  private void testPutGetDelete(boolean doGet, byte[] rowKey, byte[] testQualifier,
      byte[] testValue) throws IOException, InterruptedException, ExecutionException {
    AsyncTable table = getAsyncConnection().getTable(sharedTestEnv.getDefaultTableName(), executor);

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
    if (doGet) {
      Result result = table.get(get).get();
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

  private class Stopwatch {
    long lastCheckin = System.currentTimeMillis();

    private void print(String string) {
      long now = System.currentTimeMillis();
      logger.info(string, now - lastCheckin);
      lastCheckin = now;
    }
  }
}
