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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.PropagatingThread;
import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestErrorDetection {
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();
  @Rule public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();
  private DatabaseHelpers databaseHelpers =
      new DatabaseHelpers(connectionRule, executorServiceRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  private static final byte[] columnFamily1 = "cf1".getBytes();
  private static final byte[] qualifier1 = "q1".getBytes();

  @Test
  public void readsAndWritesArePerformed() throws IOException {
    TableName tableName;

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      try (Table t1 = connection.getTable(tableName)) {
        t1.put(Helpers.createPut("1".getBytes(), columnFamily1, qualifier1, "1".getBytes()));
      }
    }

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t2 = connection.getTable(tableName)) {
        Result result = t2.get(Helpers.createGet("1".getBytes(), columnFamily1, qualifier1));
        assertArrayEquals(result.getRow(), "1".getBytes());
        assertArrayEquals(result.getValue(columnFamily1, qualifier1), "1".getBytes());
        assertEquals(MismatchDetectorCounter.getInstance().getErrorCount(), 0);
      }
    }
  }

  @Test
  public void mismatchIsDetected() throws IOException, InterruptedException {
    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      try (Table mirroredTable = connection.getTable(tableName)) {
        mirroredTable.put(
            Helpers.createPut("1".getBytes(), columnFamily1, qualifier1, "1".getBytes()));
      }
    }

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table secondaryTable = connection.getSecondaryConnection().getTable(tableName)) {
        secondaryTable.put(
            Helpers.createPut("1".getBytes(), columnFamily1, qualifier1, "2".getBytes()));
      }
    }

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table mirroredTable = connection.getTable(tableName)) {
        Result result =
            mirroredTable.get(Helpers.createGet("1".getBytes(), columnFamily1, qualifier1));
        // Data from primary is returned.
        assertArrayEquals(result.getRow(), "1".getBytes());
        assertArrayEquals(result.getValue(columnFamily1, qualifier1), "1".getBytes());
      }
    }

    assertEquals(1, MismatchDetectorCounter.getInstance().getErrorCount());
  }

  @Test
  public void concurrentInsertionAndReadingInsertsWithScanner()
      throws IOException, InterruptedException, TimeoutException {

    class WorkerThread extends PropagatingThread {
      private final long workerId;
      private final long batchSize = 100;
      private final Connection connection;
      private final TableName tableName;
      private final long entriesPerWorker;
      private final long numberOfBatches;

      public WorkerThread(
          int workerId, Connection connection, TableName tableName, long numberOfBatches) {
        this.workerId = workerId;
        this.connection = connection;
        this.entriesPerWorker = numberOfBatches * batchSize;
        this.numberOfBatches = numberOfBatches;
        this.tableName = tableName;
      }

      @Override
      public void performTask() throws Throwable {
        try (Table table = this.connection.getTable(tableName)) {
          for (long batchId = 0; batchId < this.numberOfBatches; batchId++) {
            List<Put> puts = new ArrayList<>();
            for (long batchEntryId = 0; batchEntryId < this.batchSize; batchEntryId++) {
              long putIndex =
                  this.workerId * this.entriesPerWorker + batchId * this.batchSize + batchEntryId;
              long putValue = putIndex + 1;
              byte[] putIndexBytes = Longs.toByteArray(putIndex);
              byte[] putValueBytes = Longs.toByteArray(putValue);
              puts.add(
                  Helpers.createPut(
                      putIndexBytes, columnFamily1, qualifier1, putValue, putValueBytes));
            }
            table.put(puts);
          }
        }
      }
    }

    final int numberOfWorkers = 100;
    final int numberOfBatches = 100;

    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);

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

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t = connection.getTable(tableName)) {
        try (ResultScanner s = t.getScanner(columnFamily1, qualifier1)) {
          long counter = 0;
          for (Result r : s) {
            long row = Longs.fromByteArray(r.getRow());
            long value = Longs.fromByteArray(r.getValue(columnFamily1, qualifier1));
            assertEquals(counter, row);
            assertEquals(counter + 1, value);
            counter += 1;
          }
        }
      }
    }

    assertEquals(
        MismatchDetectorCounter.getInstance().getErrorsAsString(),
        0,
        MismatchDetectorCounter.getInstance().getErrorCount());
  }

  @Test
  public void conditionalMutationsPreserveConsistency()
      throws IOException, InterruptedException, TimeoutException {
    final int numberOfOperations = 50;
    final int numberOfWorkers = 100;

    final byte[] canary = "canary-value".getBytes();

    class WorkerThread extends PropagatingThread {
      private final long workerId;
      private final Connection connection;
      private final TableName tableName;

      public WorkerThread(int workerId, Connection connection, TableName tableName) {
        this.workerId = workerId;
        this.connection = connection;
        this.tableName = tableName;
      }

      @Override
      public void performTask() throws Throwable {
        try (Table table = this.connection.getTable(tableName)) {
          byte[] row = String.format("r%s", workerId).getBytes();
          table.put(Helpers.createPut(row, columnFamily1, qualifier1, 0, "0".getBytes()));
          for (int i = 0; i < numberOfOperations; i++) {
            byte[] currentValue = String.valueOf(i).getBytes();
            byte[] nextValue = String.valueOf(i + 1).getBytes();
            assertFalse(
                table.checkAndPut(
                    row,
                    columnFamily1,
                    qualifier1,
                    CompareOp.NOT_EQUAL,
                    currentValue,
                    Helpers.createPut(row, columnFamily1, qualifier1, i, canary)));
            assertTrue(
                table.checkAndPut(
                    row,
                    columnFamily1,
                    qualifier1,
                    CompareOp.EQUAL,
                    currentValue,
                    Helpers.createPut(row, columnFamily1, qualifier1, i, nextValue)));
          }
        }
      }
    }

    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);
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

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
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
        MismatchDetectorCounter.getInstance().getVerificationsStartedCounter());
    assertEquals(
        numberOfWorkers + 1,
        MismatchDetectorCounter.getInstance().getVerificationsFinishedCounter());
    assertEquals(
        MismatchDetectorCounter.getInstance().getErrorsAsString(),
        0,
        MismatchDetectorCounter.getInstance().getErrorCount());
  }
}
