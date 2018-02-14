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
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
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
public class TestAsyncBatch extends AbstractAsyncTest {

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
    byte[][] rowKeys = dataHelper.randomData("TestBasicAsyncOps-", 5);
    byte[][] testQualifiers = dataHelper.randomData("testQualifier-", 5);
    byte[][] testValues = dataHelper.randomData("testValue-", 5);
    testPutsGetsDeletes(true, rowKeys, testQualifiers, testValues);
  }

  static <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(v -> futures.stream().map(f -> f.getNow(null)).collect(toList()));
  }

  private void testPutsGetsDeletes(boolean doGet, byte[][] rowKeys, byte[][] testQualifiers,
      byte[][] testValues) throws IOException, InterruptedException, ExecutionException {
    AsyncTable table = getAsyncConnection().getTable(sharedTestEnv.getDefaultTableName(), executor);

    // setup
    List<Get> gets = new ArrayList<>();
    List<Put> puts = new ArrayList<>();
    List<Delete> deletes = new ArrayList<>();
    for (int i = 0; i < rowKeys.length; i++) {
      puts.add(new Put(rowKeys[i])
        .addColumn(COLUMN_FAMILY, testQualifiers[i], testValues[i]));

      gets.add(new Get(rowKeys[i])
        .addColumn(COLUMN_FAMILY, testQualifiers[i]));

      deletes.add(new Delete(rowKeys[i]));
    }

    Stopwatch stopwatch = new Stopwatch();
    // Put
    allOf(table.put(puts)).get();
    stopwatch.print("Put took %d ms");

    // Get

    // Do the get on some tests, but not others.  The rationale for that is to do performance
    // testing on large values.
    if (doGet) {
      stopwatch.print("Get took %d ms");
      List<Result> results = allOf(table.get(gets)).get();
      Assert.assertEquals(rowKeys.length, results.size()); 
      for (int i = 0; i < rowKeys.length; i++) {
        Result result = results.get(i);
        Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifiers[i]));
        List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifiers[i]);
        Assert.assertEquals(1, cells.size());
        Assert.assertTrue(CellUtil.matchingValue(cells.get(0), testValues[i]));
      }
      stopwatch.print("Verifying took %d ms");
    }
    // Delete
    allOf(table.delete(deletes)).get();
    stopwatch.print("Delete took %d ms");

    // Confirm deleted
    List<Boolean> exists = table.existsAll(gets).get();
    Assert.assertNotNull(exists);
    Assert.assertEquals(rowKeys.length, exists.size());
    for (Boolean b : exists) {
      Assert.assertFalse(b);
    }
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
