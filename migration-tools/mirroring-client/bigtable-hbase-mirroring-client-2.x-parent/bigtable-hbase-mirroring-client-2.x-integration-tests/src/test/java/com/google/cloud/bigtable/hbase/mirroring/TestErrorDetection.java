/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring;

import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_READ_VERIFICATION_RATE_PERCENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.hbase.mirroring.utils.AsyncConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.PropagatingThread;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestMismatchDetectorCounter;
import com.google.cloud.bigtable.mirroring.core.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase2_x.MirroringAsyncConnection;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestErrorDetection {
  static final byte[] columnFamily1 = "cf1".getBytes();
  static final byte[] qualifier1 = "q1".getBytes();
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();

  @ClassRule
  public static AsyncConnectionRule asyncConnectionRule = new AsyncConnectionRule(connectionRule);

  @Rule public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  public DatabaseHelpers databaseHelpers = new DatabaseHelpers(connectionRule, executorServiceRule);

  public static Configuration config = ConfigurationHelper.newConfiguration();

  static {
    config.set(MIRRORING_READ_VERIFICATION_RATE_PERCENT, "100");
  }

  @Test(timeout = 10000)
  public void readsAndWritesArePerformed()
      throws IOException, ExecutionException, InterruptedException {
    final TableName tableName = connectionRule.createTable(columnFamily1);

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      asyncConnection
          .getTable(tableName)
          .put(Helpers.createPut("1".getBytes(), columnFamily1, qualifier1, "1".getBytes()))
          .get();
    }

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      Result result =
          asyncConnection
              .getTable(tableName)
              .get(Helpers.createGet("1".getBytes(), columnFamily1, qualifier1))
              .get();
      assertArrayEquals(result.getRow(), "1".getBytes());
      assertArrayEquals(result.getValue(columnFamily1, qualifier1), "1".getBytes());
      assertEquals(TestMismatchDetectorCounter.getInstance().getErrorCount(), 0);
    }
  }

  @Test(timeout = 10000)
  public void mismatchIsDetected() throws IOException, InterruptedException, ExecutionException {
    final TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      asyncConnection
          .getPrimaryConnection()
          .getTable(tableName)
          .put(Helpers.createPut("1".getBytes(), columnFamily1, qualifier1, "1".getBytes()))
          .get();
    }

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      asyncConnection
          .getSecondaryConnection()
          .getTable(tableName)
          .put(Helpers.createPut("1".getBytes(), columnFamily1, qualifier1, "2".getBytes()))
          .get();
    }

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      Result result =
          asyncConnection
              .getTable(tableName)
              .get(Helpers.createGet("1".getBytes(), columnFamily1, qualifier1))
              .get();
      // Data from primary is returned.
      assertArrayEquals(result.getRow(), "1".getBytes());
      assertArrayEquals(result.getValue(columnFamily1, qualifier1), "1".getBytes());
    }

    assertEquals(1, TestMismatchDetectorCounter.getInstance().getErrorCount());
  }

  @Ignore("Fails for unknown reasons")
  @Test(timeout = 10000)
  public void concurrentInsertionAndReadingInsertsWithScanner()
      throws IOException, InterruptedException, TimeoutException {

    class WorkerThread extends PropagatingThread {
      private final long workerId;
      private final long batchSize = 100;
      private final AsyncConnection connection;
      private final TableName tableName;
      private final long entriesPerWorker;
      private final long numberOfBatches;

      public WorkerThread(
          int workerId, AsyncConnection connection, TableName tableName, long numberOfBatches) {
        this.workerId = workerId;
        this.connection = connection;
        this.entriesPerWorker = numberOfBatches * batchSize;
        this.numberOfBatches = numberOfBatches;
        this.tableName = tableName;
      }

      @Override
      public void performTask() throws Throwable {
        AsyncTable<AdvancedScanResultConsumer> table = this.connection.getTable(tableName);
        for (long batchId = 0; batchId < this.numberOfBatches; batchId++) {
          List<Put> puts = new ArrayList<>();
          for (long batchEntryId = 0; batchEntryId < this.batchSize; batchEntryId++) {
            long putIndex =
                this.workerId * this.entriesPerWorker + batchId * this.batchSize + batchEntryId;
            long putTimestamp = putIndex + 1;
            byte[] putIndexBytes = Longs.toByteArray(putIndex);
            byte[] putValueBytes = ("value-" + putIndex).getBytes();
            puts.add(
                Helpers.createPut(
                    putIndexBytes, columnFamily1, qualifier1, putTimestamp, putValueBytes));
          }
          CompletableFuture.allOf(table.put(puts).toArray(new CompletableFuture[0])).get();
        }
      }
    }

    final int numberOfWorkers = 100;
    final int numberOfBatches = 100;

    final TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection connection = asyncConnectionRule.createAsyncConnection(config)) {
      List<PropagatingThread> workers = new ArrayList<>();
      for (int i = 0; i < numberOfWorkers; i++) {
        PropagatingThread worker = new WorkerThread(i, connection, tableName, numberOfBatches);
        worker.start();
        workers.add(worker);
      }

      for (PropagatingThread worker : workers) {
        worker.propagatingJoin(60000);
      }
    }

    try (MirroringAsyncConnection connection = asyncConnectionRule.createAsyncConnection(config)) {
      try (ResultScanner s = connection.getTable(tableName).getScanner(columnFamily1, qualifier1)) {
        long counter = 0;
        for (Result r : s) {
          long row = Longs.fromByteArray(r.getRow());
          byte[] value = r.getValue(columnFamily1, qualifier1);
          assertEquals(counter, row);
          assertEquals(("value-" + counter).getBytes(), value);
          counter += 1;
        }
      }
    }

    assertEquals(0, TestMismatchDetectorCounter.getInstance().getErrorCount());
  }

  @Test(timeout = 10000)
  public void conditionalMutationsPreserveConsistency() throws IOException, TimeoutException {
    // TODO(mwalkiewicz): fix BigtableToHBase2
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    final int numberOfOperations = 50;
    final int numberOfWorkers = 100;

    final byte[] canary = "canary-value".getBytes();

    class WorkerThread extends PropagatingThread {
      private final long workerId;
      private final AsyncConnection connection;
      private final TableName tableName;

      public WorkerThread(int workerId, AsyncConnection connection, TableName tableName) {
        this.workerId = workerId;
        this.connection = connection;
        this.tableName = tableName;
      }

      @Override
      public void performTask() throws Throwable {
        AsyncTable<AdvancedScanResultConsumer> table = this.connection.getTable(tableName);
        byte[] row = String.format("r%s", workerId).getBytes();
        table.put(Helpers.createPut(row, columnFamily1, qualifier1, 0, "0".getBytes()));
        for (int i = 0; i < numberOfOperations; i++) {
          byte[] currentValue = String.valueOf(i).getBytes();
          byte[] nextValue = String.valueOf(i + 1).getBytes();
          assertFalse(
              table
                  .checkAndMutate(row, columnFamily1)
                  .qualifier(qualifier1)
                  .ifMatches(CompareOperator.NOT_EQUAL, currentValue)
                  .thenPut(Helpers.createPut(row, columnFamily1, qualifier1, i, canary))
                  .get());
          assertTrue(
              table
                  .checkAndMutate(row, columnFamily1)
                  .qualifier(qualifier1)
                  .ifMatches(CompareOperator.EQUAL, currentValue)
                  .thenPut(Helpers.createPut(row, columnFamily1, qualifier1, i, nextValue))
                  .get());
        }
      }
    }

    final TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection connection = asyncConnectionRule.createAsyncConnection(config)) {
      List<PropagatingThread> workers = new ArrayList<>();
      for (int i = 0; i < numberOfWorkers; i++) {
        PropagatingThread worker = new WorkerThread(i, connection, tableName);
        worker.start();
        workers.add(worker);
      }

      for (PropagatingThread worker : workers) {
        worker.propagatingJoin(30000);
      }
    }

    try (Connection connection = databaseHelpers.createConnection()) {
      try (Table t = connection.getTable(tableName)) {
        try (ResultScanner s = t.getScanner(columnFamily1, qualifier1)) {
          int counter = 0;
          for (Result r : s) {
            assertEquals(
                new String(r.getRow(), Charset.defaultCharset()),
                String.valueOf(numberOfOperations),
                new String(r.getValue(columnFamily1, qualifier1), Charset.defaultCharset()));
            counter++;
          }
          assertEquals(numberOfWorkers, counter);
        }
      }
    }

    assertEquals(
        numberOfWorkers + 1, // because null returned from the scanner is also verified.
        TestMismatchDetectorCounter.getInstance().getVerificationsStartedCounter());
    assertEquals(
        numberOfWorkers + 1,
        TestMismatchDetectorCounter.getInstance().getVerificationsFinishedCounter());
    assertEquals(0, TestMismatchDetectorCounter.getInstance().getErrorCount());
  }
}
