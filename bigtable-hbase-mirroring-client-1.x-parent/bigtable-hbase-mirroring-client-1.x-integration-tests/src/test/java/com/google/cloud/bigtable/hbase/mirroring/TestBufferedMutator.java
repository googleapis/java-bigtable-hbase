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

import static com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration.MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.ExecutorServiceRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.PropagatingThread;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestWriteErrorConsumer;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegionRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBufferedMutator {
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();

  @Rule public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule(connectionRule);
  @Rule public FailingHBaseHRegionRule failingHBaseHRegionRule = new FailingHBaseHRegionRule();

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  static final byte[] columnFamily1 = "cf1".getBytes();
  static final byte[] columnQualifier1 = "cq1".getBytes();

  @Test
  public void testBufferedMutatorPerformsMutations() throws IOException, InterruptedException {
    final int numThreads = 10;
    final int numMutationsInBatch = 100;
    final int numBatchesPerThread = 1000;

    class WorkerThread extends PropagatingThread {
      final BufferedMutator bufferedMutator;
      final int threadId;

      WorkerThread(int threadId, BufferedMutator bufferedMutator) {
        this.threadId = threadId;
        this.bufferedMutator = bufferedMutator;
      }

      @Override
      public void performTask() throws Throwable {
        long intRowId = 0;
        for (int batchId = 0; batchId < numBatchesPerThread; batchId++) {
          List<Mutation> mutations = new ArrayList<>();
          long elementsPerThread = numMutationsInBatch * numBatchesPerThread;
          for (int mutationId = 0; mutationId < numMutationsInBatch; mutationId++) {
            byte[] rowId = Longs.toByteArray(intRowId);
            Put put = new Put(rowId, System.currentTimeMillis());
            put.addColumn(
                columnFamily1,
                Ints.toByteArray(threadId),
                Longs.toByteArray(threadId * elementsPerThread + intRowId));
            mutations.add(put);
            intRowId++;
          }
          bufferedMutator.mutate(mutations);
        }
      }
    }

    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "10000");

    TableName tableName;
    try (MirroringConnection connection = executorServiceRule.createConnection(config)) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      try (BufferedMutator bm = connection.getBufferedMutator(tableName)) {
        List<PropagatingThread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
          PropagatingThread propagatingThread = new WorkerThread(i, bm);
          propagatingThread.start();
          threads.add(propagatingThread);
        }
        for (PropagatingThread thread : threads) {
          thread.propagatingJoin();
        }
      }
    } // wait for secondary writes

    long readEntries = 0;
    try (MirroringConnection connection = executorServiceRule.createConnection()) {
      try (Table table = connection.getTable(tableName)) {
        try (ResultScanner scanner = table.getScanner(new Scan())) {
          Result[] results = scanner.next(100);
          while (results != null && results.length > 0) {
            for (final Result result : results) {
              readEntries++;
              verifyRowContents(numThreads, numMutationsInBatch, numBatchesPerThread, result);
            }
            results = scanner.next(100);
          }
        }
      }
    } // wait for secondary reads to finish
    assertEquals(numBatchesPerThread * numMutationsInBatch, readEntries);
    assertEquals(
        numBatchesPerThread * numMutationsInBatch / 100 + 1,
        MismatchDetectorCounter.getInstance().getVerificationsFinishedCounter());
    assertEquals(0, MismatchDetectorCounter.getInstance().getErrorCount());
  }

  private void verifyRowContents(
      int numThreads, int numMutationsInBatch, int numBatchesPerThread, Result result) {
    long rowId = Longs.fromByteArray(result.getRow());
    for (int columnId = 0; columnId < numThreads; columnId++) {
      Cell cell = result.getColumnLatestCell(columnFamily1, Ints.toByteArray(columnId));
      assertEquals(
          columnId * numMutationsInBatch * numBatchesPerThread + rowId,
          Longs.fromByteArray(CellUtil.cloneValue(cell)));
    }
  }

  @Test
  public void testBufferedMutatorReportsFailedSecondaryWrites() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    FailingHBaseHRegion.failMutation(Longs.toByteArray(3), "row-3-error");
    FailingHBaseHRegion.failMutation(Longs.toByteArray(7), "row-7-error");

    TestWriteErrorConsumer.clearErrors();
    Configuration configuration = ConfigurationHelper.newConfiguration();
    configuration.set(
        "google.bigtable.mirroring.write-error-consumer.impl",
        TestWriteErrorConsumer.class.getCanonicalName());

    TableName tableName;
    try (MirroringConnection connection = executorServiceRule.createConnection(configuration)) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      BufferedMutatorParams params = new BufferedMutatorParams(tableName);
      try (BufferedMutator bm = connection.getBufferedMutator(params)) {
        for (int intRowId = 0; intRowId < 10; intRowId++) {
          byte[] rowId = Longs.toByteArray(intRowId);
          Put put = new Put(rowId, System.currentTimeMillis());
          put.addColumn(columnFamily1, columnQualifier1, Longs.toByteArray(intRowId));
          bm.mutate(put);
        }
        bm.flush();
      }
    } // wait for secondary writes

    // Two failed writes should have been reported
    assertThat(TestWriteErrorConsumer.getErrorCount()).isEqualTo(2);

    try (MirroringConnection mirroringConnection = executorServiceRule.createConnection()) {
      try (Table table = mirroringConnection.getTable(tableName)) {
        try (ResultScanner scanner = table.getScanner(columnFamily1)) {
          List<Long> rows = new ArrayList<>();
          for (Result result : scanner) {
            rows.add(Longs.fromByteArray(result.getRow()));
          }
          assertThat(rows).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        }
      }
    }

    // First mismatch happens when primary returns 3 and secondary returns 4
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount()).isEqualTo(7);
  }

  @Test
  public void testBufferedMutatorSkipsFailedPrimaryWrites() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    FailingHBaseHRegion.failMutation(Longs.toByteArray(3), "row-3-error");
    FailingHBaseHRegion.failMutation(Longs.toByteArray(7), "row-7-error");

    final List<ByteBuffer> thrownException = new ArrayList<>();
    TableName tableName;
    try (MirroringConnection connection = executorServiceRule.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      BufferedMutatorParams params = new BufferedMutatorParams(tableName);
      params.listener(
          new ExceptionListener() {
            @Override
            public void onException(
                RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator)
                throws RetriesExhaustedWithDetailsException {
              for (int i = 0; i < e.getNumExceptions(); i++) {
                thrownException.add(ByteBuffer.wrap(e.getRow(i).getRow()));
              }
            }
          });
      try (BufferedMutator bm = connection.getBufferedMutator(params)) {
        for (int intRowId = 0; intRowId < 10; intRowId++) {
          byte[] rowId = Longs.toByteArray(intRowId);
          Put put = new Put(rowId, System.currentTimeMillis());
          put.addColumn(columnFamily1, columnQualifier1, Longs.toByteArray(intRowId));
          bm.mutate(put);
        }
        bm.flush();
      }
    } // wait for secondary writes

    try (MirroringConnection mirroringConnection = executorServiceRule.createConnection()) {
      Connection secondary = mirroringConnection.getSecondaryConnection();
      Table table = secondary.getTable(tableName);
      ResultScanner scanner = table.getScanner(columnFamily1);
      List<Long> rows = new ArrayList<>();
      for (Result result : scanner) {
        rows.add(Longs.fromByteArray(result.getRow()));
      }
      assertThat(rows).containsExactly(0L, 1L, 2L, 4L, 5L, 6L, 8L, 9L);
    }

    assertThat(thrownException).contains(ByteBuffer.wrap(Longs.toByteArray(3)));
    assertThat(thrownException).contains(ByteBuffer.wrap(Longs.toByteArray(7)));
  }
}
