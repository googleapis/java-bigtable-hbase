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
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.PropagatingThread;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestMismatchDetectorCounter;
import com.google.cloud.bigtable.mirroring.core.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.core.MirroringConnection;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
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
  private static final byte[] row1 = "r1".getBytes();
  private static final byte[] value1 = "v1".getBytes();

  @Test
  public void readsAndWritesArePerformed() throws IOException {
    TableName tableName;

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      try (Table t1 = connection.getTable(tableName)) {
        t1.put(Helpers.createPut(row1, columnFamily1, qualifier1, value1));
      }
    }

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t2 = connection.getTable(tableName)) {
        Result result = t2.get(Helpers.createGet(row1, columnFamily1, qualifier1));
        assertArrayEquals(result.getRow(), row1);
        assertArrayEquals(result.getValue(columnFamily1, qualifier1), value1);
        assertEquals(TestMismatchDetectorCounter.getInstance().getErrorCount(), 0);
      }
    }
  }

  @Test
  public void mismatchIsDetected() throws IOException, InterruptedException {
    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      try (Table mirroredTable = connection.getTable(tableName)) {
        mirroredTable.put(Helpers.createPut(row1, columnFamily1, qualifier1, value1));
      }
    }

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table secondaryTable = connection.getSecondaryConnection().getTable(tableName)) {
        secondaryTable.put(Helpers.createPut(row1, columnFamily1, qualifier1, value1));
      }
    }

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table mirroredTable = connection.getTable(tableName)) {
        Result result = mirroredTable.get(Helpers.createGet(row1, columnFamily1, qualifier1));
        // Data from primary is returned.
        assertArrayEquals(result.getRow(), row1);
        assertArrayEquals(result.getValue(columnFamily1, qualifier1), value1);
      }
    }

    assertEquals(1, TestMismatchDetectorCounter.getInstance().getErrorCount());
  }

  @Test
  public void concurrentInsertionAndReadingInsertsWithScanner()
      throws IOException, TimeoutException {

    class WorkerThread extends PropagatingThread {
      private final long workerId;
      private final long batchSize;
      private final Connection connection;
      private final TableName tableName;
      private final long entriesPerWorker;
      private final long numberOfBatches;

      public WorkerThread(
          int workerId,
          Connection connection,
          TableName tableName,
          long numberOfBatches,
          long batchSize) {
        this.workerId = workerId;
        this.connection = connection;
        this.batchSize = batchSize;
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
              long putTimestamp = putIndex + 1;
              byte[] putIndexBytes = Longs.toByteArray(putIndex);
              byte[] putValueBytes = ("value-" + putIndex).getBytes();
              puts.add(
                  Helpers.createPut(
                      putIndexBytes, columnFamily1, qualifier1, putTimestamp, putValueBytes));
            }
            table.put(puts);
          }
        }
      }
    }

    final int numberOfWorkers = 100;
    final int numberOfBatches = 100;
    final long batchSize = 100;

    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);

      List<PropagatingThread> workers = new ArrayList<>();
      for (int i = 0; i < numberOfWorkers; i++) {
        PropagatingThread worker =
            new WorkerThread(i, connection, tableName, numberOfBatches, batchSize);
        worker.start();
        workers.add(worker);
      }

      for (PropagatingThread worker : workers) {
        worker.propagatingJoin(60000);
      }
    }

    Configuration configuration = ConfigurationHelper.newConfiguration();
    configuration.set(MIRRORING_READ_VERIFICATION_RATE_PERCENT, "100");

    try (MirroringConnection connection = databaseHelpers.createConnection(configuration)) {
      try (Table t = connection.getTable(tableName)) {
        try (ResultScanner s = t.getScanner(columnFamily1, qualifier1)) {
          long counter = 0;
          for (Result r : s) {
            long row = Longs.fromByteArray(r.getRow());
            byte[] value = r.getValue(columnFamily1, qualifier1);
            assertThat(counter).isEqualTo(row);
            assertThat(("value-" + counter).getBytes()).isEqualTo(value);
            counter += 1;
          }
        }
      }
    }

    assertEquals(0, TestMismatchDetectorCounter.getInstance().getErrorCount());
    // + 1 because we also verify the final `null` denoting end of results.
    assertEquals(
        numberOfWorkers * numberOfBatches * batchSize + 1,
        TestMismatchDetectorCounter.getInstance().getVerificationsFinishedCounter());
    assertEquals(
        numberOfWorkers * numberOfBatches * batchSize + 1,
        TestMismatchDetectorCounter.getInstance().getVerificationsStartedCounter());
  }
}
