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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.repackaged.com.google.common.util.concurrent.SettableFuture;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Test to make sure that basic {@link AsyncTable} operations work
 * @author sduskis
 */
@RunWith(JUnit4.class)
public class TestAsyncScan extends AbstractAsyncTest {

  private static ExecutorService executor;
  private static String prefix = "TestAsyncScan-";
  private static byte[][] rowKeys;
  private static byte[][] quals;
  private static byte[][] values;

  @BeforeClass
  public static void setup() throws IOException {
    Table table = SharedTestEnvRule.getInstance().getDefaultTable();
    rowKeys = dataHelper.randomData(prefix, 2);
    int numValues = 3;
    quals = dataHelper.randomData("qual-", numValues);
    values = dataHelper.randomData("value-", numValues);

    // Insert some columns
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < rowKeys.length; i++) {
      Put put = new Put(rowKeys[i]);
      for (int j = 0; j < numValues; j++) {
        put.addColumn(COLUMN_FAMILY, quals[j], values[j]);
      }
      puts.add(put);
    }
    table.put(puts);

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
  public void testScan() throws Exception {
    Scan scan = new Scan();
    scan.setRowPrefixFilter(Bytes.toBytes(prefix));

    ResultScanner resultScanner = getDefaultAsyncTable(executor).getScanner(scan);
    Result[] results = resultScanner.next(3);
    resultScanner.close();

    Assert.assertEquals(2, results.length);
    for (Result result : results) {
      verify(result);
    }
  }

  @Test
  public void testScanAll() throws Exception {
    Scan scan = new Scan();
    scan.setRowPrefixFilter(Bytes.toBytes(prefix));

    List<Result> results = getDefaultAsyncTable(executor)
        .scanAll(scan)
        .get(1, TimeUnit.MINUTES);

    Assert.assertEquals(2, results.size());
    for (Result result : results) {
      verify(result);
    }
  }

  @Test
  public void testScanConsumer() throws Exception {
    Scan scan = new Scan();
    scan.setRowPrefixFilter(Bytes.toBytes(prefix));

    SettableFuture<Integer> lock = SettableFuture.create();
    getDefaultAsyncTable(executor).scan(scan, new ScanResultConsumer() {
      int count = 0;
      @Override
      public boolean onNext(Result result) {
        try {
          verify(result);
          count++;
          return true;
        } catch (Exception e) {
          lock.setException(e);
          return false;
        }
      }

      @Override
      public void onError(Throwable e) {
        lock.setException(e);
      }

      @Override
      public void onComplete() {
        lock.set(count);
      }
    });

    Assert.assertEquals(2, lock.get().intValue());
  }

  private static void verify(Result result) {
    Assert.assertEquals(values.length, result.size());
    for (int i = 0; i < values.length; ++i) {
      Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, quals[i]));
      Assert.assertArrayEquals(values[i],
          CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[i])));
    }
  }

}
