/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.hbase.replication;

import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.PUTS_IN_FUTURE_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.CF1;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.ROW_KEY;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.TABLE_NAME_STRING;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.TABLE_NAME_STRING_2;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.TIMESTAMP;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.failedFuture;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.getRowKey;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.getValue;
import static org.apache.hadoop.hbase.KeyValue.Type.DeleteFamilyVersion;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicator.SharedResources;
import com.google.cloud.bigtable.hbase.replication.adapters.ApproximatingIncompatibleMutationAdapter;
import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapter;
import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class CloudBigtableReplicatorTest {

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();
  @Mock private ExecutorService mockExecutorService;
  @Mock private Connection mockConnection;
  @Mock private MetricsExporter mockMetricExporter;

  private Configuration conf = new Configuration(false);
  private SharedResources sharedResources;
  private IncompatibleMutationAdapter incompatibleMutationAdapter;

  @Before
  public void setUp() {
    sharedResources = new SharedResources(mockConnection, mockExecutorService);
    incompatibleMutationAdapter =
        new ApproximatingIncompatibleMutationAdapter(conf, mockMetricExporter, mockConnection);
  }

  @After
  public void tearDown() {
    Mockito.reset(mockExecutorService, mockConnection);
  }

  @Test
  public void testReplicateDryRun() {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 100, true);

    // Create WALs to replicate
    Cell cell = new KeyValue(ROW_KEY, CF1, null, TIMESTAMP, DeleteFamilyVersion);
    BigtableWALEntry walEntry =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell), TABLE_NAME_STRING);

    Map<String, List<BigtableWALEntry>> walToReplicate = new HashMap<>();
    walToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry));

    // Trigger replication
    assertTrue(replicator.replicate(walToReplicate));

    // Validate that CBT was not called and incompatibleAdapter was called for dry-run mode.
    // Called during the constructor
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    // Incremented due to incompatible DeleteFamilyVersion mutation
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 1);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);

    Mockito.verifyNoMoreInteractions(mockMetricExporter);
    // Calls to CBT happen via executor service. Make sure that executor service was not called.
    Mockito.verifyNoInteractions(mockConnection, mockExecutorService);
  }

  @Test
  public void testReplicateDoesNotSplitInBatches() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false);

    // Create WALs to replicate
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    Cell cell12 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(2));
    Cell cell13 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(3));
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11, cell12, cell13), TABLE_NAME_STRING);

    Cell cell21 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    Cell cell22 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(2));
    Cell cell23 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(3));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21, cell22, cell23), TABLE_NAME_STRING);
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2));

    // Everything succeeds.
    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    // Trigger replication
    assertTrue(replicator.replicate(walsToReplicate));

    // called during constructor
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    // Replicator should submit just 1 CloudBigtableReplicationTask for both WALEntries
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }

  @Test
  public void testReplicateSplitsBatchesOnRowBoundary() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(
        sharedResources, incompatibleMutationAdapter, 1 /*split into 1 byte batches*/, false);

    // Create WALs to replicate
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    Cell cell12 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(2));
    Cell cell13 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(3));
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11, cell12, cell13), TABLE_NAME_STRING);

    Cell cell21 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    Cell cell22 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(2));
    Cell cell23 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(3));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21, cell22, cell23), TABLE_NAME_STRING);

    Map<ByteRange, List<Cell>> expectedBatchOfWal1 = new HashMap<>();
    expectedBatchOfWal1.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    Map<ByteRange, List<Cell>> expectedBatchOfWal2 = new HashMap<>();
    expectedBatchOfWal2.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2));

    // Everything succeeds.
    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    // Trigger replication
    assertTrue(replicator.replicate(walsToReplicate));

    // called during constructor
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    // Replicator should split WALs into 2 CloudBigtableReplicationTask at row keys
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal1));
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal2));
    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }

  @Test
  public void testReplicateSplitsBatchesOnTableBoundary() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(
        sharedResources, incompatibleMutationAdapter, 1 /*split into 1 byte batches*/, false);

    // Create WALs to replicate
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    Cell cell12 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(2));
    Cell cell13 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(3));
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11, cell12, cell13), TABLE_NAME_STRING);

    Cell cell21 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    Cell cell22 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(2));
    Cell cell23 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(3));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21, cell22, cell23), TABLE_NAME_STRING_2);

    Map<ByteRange, List<Cell>> expectedBatchOfWal1 = new HashMap<>();
    expectedBatchOfWal1.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    Map<ByteRange, List<Cell>> expectedBatchOfWal2 = new HashMap<>();
    expectedBatchOfWal2.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1));
    walsToReplicate.put(TABLE_NAME_STRING_2, Arrays.asList(walEntry2));

    // Everything succeeds.
    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    // Trigger replication
    assertTrue(replicator.replicate(walsToReplicate));

    // called during constructor
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    // Replicator should split WALs into 2 CloudBigtableReplicationTask at table boundary
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal1));
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING_2, mockConnection, expectedBatchOfWal2));
    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }

  @Test
  public void testReplicateFailsOnAnyFailure() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(
        sharedResources, incompatibleMutationAdapter, 01 /*split into 1 byte batches*/, false);

    // Create WALs to replicate
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11), TABLE_NAME_STRING);

    Cell cell21 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21), TABLE_NAME_STRING);

    Map<ByteRange, List<Cell>> expectedBatchOfWal1 = new HashMap<>();
    expectedBatchOfWal1.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    Map<ByteRange, List<Cell>> expectedBatchOfWal2 = new HashMap<>();
    expectedBatchOfWal2.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2));

    // One task succeeds and other fails.
    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(false))
        .thenReturn(CompletableFuture.completedFuture(true));

    // Trigger replication, this should fail as 1 of the tasks failed
    assertFalse(replicator.replicate(walsToReplicate));

    // called during constructor
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    // Replicator should split WALs into 2 CloudBigtableReplicationTask at row keys
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal1));
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal2));
    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }

  @Test
  public void testReplicateFailsOnAnyFutureFailure() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(
        sharedResources, incompatibleMutationAdapter, 01 /*split into 1 byte batches*/, false);

    // Create WALs to replicate
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11), TABLE_NAME_STRING);

    Cell cell21 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21), TABLE_NAME_STRING);

    Map<ByteRange, List<Cell>> expectedBatchOfWal1 = new HashMap<>();
    expectedBatchOfWal1.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    Map<ByteRange, List<Cell>> expectedBatchOfWal2 = new HashMap<>();
    expectedBatchOfWal2.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2));

    // 1 task succeeds other fails with an exception.
    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(failedFuture(new RuntimeException("Failed Future.")))
        .thenReturn(CompletableFuture.completedFuture(true));

    // Trigger replication, this should fail as 1 of the tasks failed
    assertFalse(replicator.replicate(walsToReplicate));

    // called during constructor
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    // Replicator should split WALs into 2 CloudBigtableReplicationTask at row keys
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal1));
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal2));
    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }

  @Test
  public void testReplicateFailsToSubmitTask() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2, false);

    // Create WALs to replicate
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11), TABLE_NAME_STRING);

    Cell cell21 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21), TABLE_NAME_STRING);

    Map<ByteRange, List<Cell>> expectedBatchOfWal1 = new HashMap<>();
    expectedBatchOfWal1.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    Map<ByteRange, List<Cell>> expectedBatchOfWal2 = new HashMap<>();
    expectedBatchOfWal2.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2));

    // 1 submit fails and throws exceptions other succeeds.
    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));
    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenThrow(new RuntimeException("failed to submit"));

    // Replication failed as 1 task failed to submit
    assertFalse(replicator.replicate(walsToReplicate));

    // called during constructor
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    // Replicator should split WALs into 2 CloudBigtableReplicationTask at row keys
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal1));
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal2));
    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }
}
