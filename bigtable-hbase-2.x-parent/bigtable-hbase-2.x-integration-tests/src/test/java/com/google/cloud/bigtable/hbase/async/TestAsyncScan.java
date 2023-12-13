/*
 * Copyright 2018 Google LLC
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

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test to make sure that basic {@link AsyncTable} operations work
 *
 * @author sduskis
 */
@RunWith(JUnit4.class)
public class TestAsyncScan extends AbstractAsyncTest {

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
  }

  @Test
  public void testScan() throws Exception {
    Scan scan = new Scan();
    scan.setRowPrefixFilter(Bytes.toBytes(prefix));

    ResultScanner resultScanner = getDefaultAsyncTable().getScanner(scan);
    Result[] results = resultScanner.next(3);
    resultScanner.close();

    Assert.assertEquals(rowKeys.length, results.length);
    for (Result result : results) {
      verify(result);
    }
  }

  @Test
  public void testScanAll() throws Exception {
    Scan scan = new Scan();
    scan.setRowPrefixFilter(Bytes.toBytes(prefix));

    List<Result> results = getDefaultAsyncTable().scanAll(scan).get(1, TimeUnit.MINUTES);

    Assert.assertEquals(rowKeys.length, results.size());
    for (Result result : results) {
      verify(result);
    }
  }

  @Test
  public void testScanConsumer() throws Exception {
    Scan scan = new Scan();
    scan.setRowPrefixFilter(Bytes.toBytes(prefix));

    CompletableFuture<Integer> lock = new CompletableFuture<>();
    getDefaultAsyncTable()
        .scan(
            scan,
            new ScanResultConsumer() {
              int count = 0;

              @Override
              public boolean onNext(Result result) {
                try {
                  verify(result);
                  count++;
                  return true;
                } catch (Exception e) {
                  lock.completeExceptionally(e);
                  return false;
                }
              }

              @Override
              public void onError(Throwable e) {
                lock.completeExceptionally(e);
                ;
              }

              @Override
              public void onComplete() {
                lock.complete(count);
              }
            });

    Assert.assertEquals(rowKeys.length, lock.get().intValue());
  }

  @Test
  public void testNextAfterClose() throws IOException {
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");

    table.put(new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value));

    Scan scan = new Scan().withStartRow(rowKey).withStopRow(rowKey, true);
    ResultScanner resultScanner = table.getScanner(scan);

    Assert.assertNotNull(resultScanner.next());

    // The scanner should close here.
    Assert.assertNull(resultScanner.next());

    // This shouldn't throw an exception
    Assert.assertNull(resultScanner.next());

    resultScanner.close();

    // This shouldn't throw an exception
    Assert.assertNull(resultScanner.next());
  }

  private static void verify(Result result) {
    Assert.assertEquals(values.length, result.size());
    for (int i = 0; i < values.length; ++i) {
      Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, quals[i]));
      Assert.assertArrayEquals(
          values[i], CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[i])));
    }
  }
}
