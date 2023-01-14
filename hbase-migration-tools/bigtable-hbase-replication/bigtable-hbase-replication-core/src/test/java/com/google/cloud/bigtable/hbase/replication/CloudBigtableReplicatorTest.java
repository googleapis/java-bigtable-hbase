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

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_SOURCE_CBT_QUALIFIER;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_SOURCE_HBASE_QUALIFIER;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.ENABLE_TWO_WAY_REPLICATION_MODE_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.SOURCE_CBT_QUALIFIER_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.SOURCE_HBASE_QUALIFIER_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.SOURCE_CBT_DROPPED;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.SOURCE_HBASE_REPLICATED;
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

  /**
   * Two-way replication should add a special mutation on mutations its replicates.
   * This should be reflected in both metrics and the adapted WAL log.
   * @throws IOException
   */
  @Test
  public void testTwoWayReplicationAddsSpecialMutation() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false);

    // Enable two-way replication manually.
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(ENABLE_TWO_WAY_REPLICATION_MODE_KEY, true);
    replicator.maybeStartTwoWayReplication(replicationConf, mockMetricExporter);

    // Create cells to assemble WALs with.
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    Cell cell12 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(2));
    Cell cell13 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(3));
    // Create special mutation cell.
    Cell cell1s = new KeyValue(getRowKey(1),
        CF1, DEFAULT_SOURCE_HBASE_QUALIFIER.getBytes(), 0l, KeyValue.Type.Delete);

    // Create WAL without special mutation. Replicator will read from this WAL.
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11, cell12, cell13), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1));

    // Create WAL with special mutation. This reflects the expected state of what replicationTask
    // should see after Replicator adds special mutation to the original WAL.
    BigtableWALEntry walEntry1s = new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11,cell12,cell13,cell1s), TABLE_NAME_STRING);
    // The expected batch should include the special mutation.
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(1)), walEntry1s.getCells());

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
    // Metric reflects that one hbase row mutation was replicated.
    verify(mockMetricExporter).incCounters(SOURCE_HBASE_REPLICATED, 1);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }

  /**
   * Two-way replication should drop mutations that come from an external replicated source.
   * @throws IOException
   */
  @Test
  public void testTwoWayReplicationDropsReplicatedMutation() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false);

    // Enable two-way replication manually.
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(ENABLE_TWO_WAY_REPLICATION_MODE_KEY, true);
    replicator.maybeStartTwoWayReplication(replicationConf, mockMetricExporter);

    // Row key 1 WAL comes from CBT and should be filtered out.
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    // Special replicated mutation from CBT.
    Cell cell1s = new KeyValue(getRowKey(1),
        CF1, DEFAULT_SOURCE_CBT_QUALIFIER.getBytes(), 0l, KeyValue.Type.Delete);

    // Create replicator input WAL. Note that we don't have any expected replicator output.
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11, cell1s), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1));

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

    // WAL log is empty because all entries were dropped. Refer metrics for test success.
    // verify(mockExecutorService)
    //     .submit(
    //         new CloudBigtableReplicationTask(
    //             TABLE_NAME_STRING, mockConnection, new HashMap<>()));
    verify(mockMetricExporter).incCounters(SOURCE_CBT_DROPPED, 1);

    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }

  /**
   * Two-way replication combined test to check that replicator can drop the externally-replicated
   * mutation but keep and replicate its own mutation.
   * @throws IOException
   */
  @Test
  public void testTwoWayReplicationDropsOneReplicatesOther() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false);

    // Enable two-way replication manually.
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(ENABLE_TWO_WAY_REPLICATION_MODE_KEY, true);
    replicator.maybeStartTwoWayReplication(replicationConf, mockMetricExporter);

    // Row key 1 WAL comes from CBT and should be filtered out.
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    // Special replicated mutation from CBT.
    Cell cell1s = new KeyValue(getRowKey(1),
        CF1, DEFAULT_SOURCE_CBT_QUALIFIER.getBytes(), 0l, KeyValue.Type.Delete);

    // Row key 2 WAL comes from HBase and should be replicated.
    Cell cell21 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    Cell cell2s = new KeyValue(getRowKey(2),
        CF1, DEFAULT_SOURCE_HBASE_QUALIFIER.getBytes(), 0l, KeyValue.Type.Delete);

    // Create respective WAL entries.
    // Entry 1 will be filtered out.
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11, cell1s), TABLE_NAME_STRING);
    // Entry 2 should be replicated.
    BigtableWALEntry walEntry2 = new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21), TABLE_NAME_STRING);
    BigtableWALEntry walEntry2s = new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21, cell2s), TABLE_NAME_STRING);

    // Create WAL for replicator.
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2));

    // Create expected state of replicationTask input.
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(2)), walEntry2s.getCells());

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
    // Metrics reflect that one WAL entry is dropped and one passes through.
    verify(mockMetricExporter).incCounters(SOURCE_CBT_DROPPED, 1);
    verify(mockMetricExporter).incCounters(SOURCE_HBASE_REPLICATED, 1);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }

  /**
   * Two-way replication allows users to specify custom special column-qualifiers.
   * Both CBT and HBase qualifiers are tested on 2 rows.
   * One should be filtered out and the other gets replicated.
   * @throws IOException
   */
  @Test
  public void testTwoWayReplicationCustomSpecialColumnQualifier() throws IOException {

    String customSourceCBTQualifier = "cccbt";
    String customSourceHBaseQualifier = "hhhbase";

    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false);

    // Enable two-way replication manually.
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(ENABLE_TWO_WAY_REPLICATION_MODE_KEY, true);
    // Set custom special column qualifiers
    replicationConf.set(SOURCE_CBT_QUALIFIER_KEY, customSourceCBTQualifier);
    replicationConf.set(SOURCE_HBASE_QUALIFIER_KEY, customSourceHBaseQualifier);
    replicator.maybeStartTwoWayReplication(replicationConf, mockMetricExporter);

    // Row key 1 WAL comes from CBT and should be filtered out.
    Cell cell11 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    // Special replicated mutation from CBT.
    Cell cell1s = new KeyValue(getRowKey(1),
        CF1, customSourceCBTQualifier.getBytes(), 0l, KeyValue.Type.Delete);

    // Row key 2 WAL comes from HBase and should be replicated.
    Cell cell21 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    Cell cell2s = new KeyValue(getRowKey(2),
        CF1, customSourceHBaseQualifier.getBytes(), 0l, KeyValue.Type.Delete);

    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell11, cell1s), TABLE_NAME_STRING); // cell12, cell13,

    BigtableWALEntry walEntry2 = new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21), TABLE_NAME_STRING);
    BigtableWALEntry walEntry2s = new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell21, cell2s), TABLE_NAME_STRING);

    // Create WAL for replicator input.
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2));

    // Expect only one batch in replicator output.
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(2)), walEntry2s.getCells());

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
    // One batch dropped and one batch replicated.
    verify(mockMetricExporter).incCounters(SOURCE_CBT_DROPPED, 1);
    verify(mockMetricExporter).incCounters(SOURCE_HBASE_REPLICATED, 1);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    Mockito.verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    Mockito.verifyNoInteractions(mockConnection);
  }
}
