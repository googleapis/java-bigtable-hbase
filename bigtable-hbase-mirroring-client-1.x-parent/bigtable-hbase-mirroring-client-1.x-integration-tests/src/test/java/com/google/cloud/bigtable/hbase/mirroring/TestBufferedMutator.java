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

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_CONCURRENT_WRITES;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_STRATEGY_MAX_OUTSTANDING_REQUESTS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SYNCHRONOUS_WRITES;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter.Mismatch;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.PropagatingThread;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestWriteErrorConsumer;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegionRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
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
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestBufferedMutator {
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();
  @Rule public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();
  private DatabaseHelpers databaseHelpers =
      new DatabaseHelpers(connectionRule, executorServiceRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  @Rule public FailingHBaseHRegionRule failingHBaseHRegionRule = new FailingHBaseHRegionRule();

  private static final byte[] columnFamily1 = "cf1".getBytes();
  private static final byte[] qualifier1 = "cq1".getBytes();

  @Parameterized.Parameters(name = "mutateConcurrently: {0}")
  public static Object[] data() {
    return new Object[] {false, true};
  }

  private final boolean mutateConcurrently;

  public TestBufferedMutator(boolean mutateConcurrently) {
    this.mutateConcurrently = mutateConcurrently;
  }

  private Configuration createConfiguration() {
    Configuration configuration = ConfigurationHelper.newConfiguration();
    // MirroringOptions constructor verifies that MIRRORING_SYNCHRONOUS_WRITES is true when
    // MIRRORING_CONCURRENT_WRITES is true (consult its constructor for more details).
    // We are keeping MIRRORING_SYNCHRONOUS_WRITES false if we do not write concurrently (we are not
    // testing the other case here anyways) and set it to true to meet the requirements otherwise.
    configuration.set(MIRRORING_CONCURRENT_WRITES, String.valueOf(this.mutateConcurrently));
    configuration.set(MIRRORING_SYNCHRONOUS_WRITES, String.valueOf(this.mutateConcurrently));
    return configuration;
  }

  @Test
  public void testBufferedMutatorPerformsMutations() throws IOException, InterruptedException {
    final int numThreads = 10;
    final int numMutationsInBatch = 100;
    final int numBatchesPerThread = 1000;

    // We will run `numThreads` threads, each performing `numBatchesPerThread` batches of mutations,
    // `numMutationsInBatch` each, using our MirroringClient. After all the operations are over we
    // will verify if primary and secondary databases have the same contents.

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

    Configuration config = this.createConfiguration();
    // Set flow controller requests limit to high value to increase concurrency.
    config.set(MIRRORING_FLOW_CONTROLLER_STRATEGY_MAX_OUTSTANDING_REQUESTS, "10000");

    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
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
    } // connection close will wait for secondary writes

    long readEntries = 0;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
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
  public void testBufferedMutatorSecondaryErrorHandling() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    FailingHBaseHRegion.failMutation(
        Longs.toByteArray(3), OperationStatusCode.SANITY_CHECK_FAILURE, "row-3-error");
    FailingHBaseHRegion.failMutation(
        Longs.toByteArray(7), OperationStatusCode.SANITY_CHECK_FAILURE, "row-7-error");

    TestWriteErrorConsumer.clearErrors();
    Configuration configuration = this.createConfiguration();
    configuration.set(
        "google.bigtable.mirroring.write-error-consumer.factory-impl",
        TestWriteErrorConsumer.Factory.class.getName());

    TableName tableName;
    List<Throwable> flushExceptions = null;
    try (MirroringConnection connection = databaseHelpers.createConnection(configuration)) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      BufferedMutatorParams params = new BufferedMutatorParams(tableName);
      try (BufferedMutator bm = connection.getBufferedMutator(params)) {
        for (int intRowId = 0; intRowId < 10; intRowId++) {
          byte[] rowId = Longs.toByteArray(intRowId);
          Put put = new Put(rowId, System.currentTimeMillis());
          put.addColumn(columnFamily1, qualifier1, Longs.toByteArray(intRowId));
          bm.mutate(put);
        }
        try {
          bm.flush();
        } catch (IOException e) {
          if (e instanceof RetriesExhaustedWithDetailsException) {
            flushExceptions = ((RetriesExhaustedWithDetailsException) e).getCauses();
          } else {
            fail();
          }
        }
      }
    } // connection close will wait for secondary writes

    if (this.mutateConcurrently) {
      // ConcurrentBufferedMutator does not report secondary write errors.
      assertThat(TestWriteErrorConsumer.getErrorCount()).isEqualTo(0);

      assertNotNull(flushExceptions);
      assertEquals(2, flushExceptions.size());

      for (Throwable e : flushExceptions) {
        Throwable cause = e.getCause();
        if (cause instanceof MirroringOperationException) {
          assertEquals(
              MirroringOperationException.DatabaseIdentifier.Secondary,
              ((MirroringOperationException) cause).databaseIdentifier);
        } else {
          fail();
        }
      }
    } else {
      // SequentialBufferedMutator should have reported two errors.
      assertEquals(2, TestWriteErrorConsumer.getErrorCount());

      assertNull(flushExceptions);
    }

    try (MirroringConnection mirroringConnection = databaseHelpers.createConnection()) {
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

    List<Mismatch> mismatches = MismatchDetectorCounter.getInstance().getMismatches();
    assertThat(mismatches.size()).isEqualTo(7);
    assertThat(mismatches).contains(scannerMismatch(3, 4));
    assertThat(mismatches).contains(scannerMismatch(4, 5));
    assertThat(mismatches).contains(scannerMismatch(5, 6));
    assertThat(mismatches).contains(scannerMismatch(6, 8));
    assertThat(mismatches).contains(scannerMismatch(7, 9));
    assertThat(mismatches).contains(scannerMismatch(8, null));
    assertThat(mismatches).contains(scannerMismatch(9, null));
  }

  private Mismatch scannerMismatch(int primary, Integer secondary) {
    return new Mismatch(
        HBaseOperation.NEXT,
        Longs.toByteArray(primary),
        secondary == null ? null : Longs.toByteArray(secondary));
  }

  @Test
  public void testBufferedMutatorPrimaryErrorHandling() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    FailingHBaseHRegion.failMutation(
        Longs.toByteArray(3), OperationStatusCode.SANITY_CHECK_FAILURE, "row-3-error");
    FailingHBaseHRegion.failMutation(
        Longs.toByteArray(7), OperationStatusCode.SANITY_CHECK_FAILURE, "row-7-error");

    Configuration configuration = this.createConfiguration();

    final Set<Throwable> exceptionsThrown = new HashSet<>();
    final List<ByteBuffer> exceptionRows = new ArrayList<>();
    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection(configuration)) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      BufferedMutatorParams params = new BufferedMutatorParams(tableName);
      params.listener(
          new ExceptionListener() {
            @Override
            public void onException(
                RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator)
                throws RetriesExhaustedWithDetailsException {
              for (int i = 0; i < e.getNumExceptions(); i++) {
                exceptionRows.add(ByteBuffer.wrap(e.getRow(i).getRow()));
                exceptionsThrown.addAll(e.getCauses());
              }
            }
          });
      try (BufferedMutator bm = connection.getBufferedMutator(params)) {
        for (int intRowId = 0; intRowId < 10; intRowId++) {
          byte[] rowId = Longs.toByteArray(intRowId);
          Put put = new Put(rowId, System.currentTimeMillis());
          put.addColumn(columnFamily1, qualifier1, Longs.toByteArray(intRowId));
          bm.mutate(put);
        }
        bm.flush();
      }
    } // connection close will wait for secondary writes

    List<Long> secondaryRows = new ArrayList<>();
    try (MirroringConnection mirroringConnection = databaseHelpers.createConnection()) {
      Connection secondary = mirroringConnection.getSecondaryConnection();
      Table table = secondary.getTable(tableName);
      ResultScanner scanner = table.getScanner(columnFamily1);
      for (Result result : scanner) {
        secondaryRows.add(Longs.fromByteArray(result.getRow()));
      }
    }

    assertEquals(2, exceptionRows.size());
    assertThat(exceptionRows).contains(ByteBuffer.wrap(Longs.toByteArray(3)));
    assertThat(exceptionRows).contains(ByteBuffer.wrap(Longs.toByteArray(7)));

    if (this.mutateConcurrently) {
      assertEquals(2, exceptionsThrown.size());
      for (Throwable e : exceptionsThrown) {
        Throwable cause = e.getCause();
        if (cause instanceof MirroringOperationException) {
          assertEquals(
              MirroringOperationException.DatabaseIdentifier.Primary,
              ((MirroringOperationException) cause).databaseIdentifier);
        } else {
          fail();
        }
      }
      assertThat(secondaryRows).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    } else {
      assertThat(secondaryRows).containsExactly(0L, 1L, 2L, 4L, 5L, 6L, 8L, 9L);
    }
  }
}
