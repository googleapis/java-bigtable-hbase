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

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_CBT_QUALIFIER;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_CBT_QUALIFIER_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_HBASE_QUALIFIER;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_HBASE_QUALIFIER_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.TABLE_NAME_MAPPING_FILE_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.TABLE_NAME_MAPPING_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.PUTS_IN_FUTURE_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.CF1;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.COL_QUALIFIER;
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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicator.SharedResources;
import com.google.cloud.bigtable.hbase.replication.adapters.ApproximatingIncompatibleMutationAdapter;
import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapter;
import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    replicator.start(sharedResources, incompatibleMutationAdapter, 100, true, null, null);

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
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false, null, null);

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
        sharedResources,
        incompatibleMutationAdapter,
        1 /*split into 1 byte batches*/,
        false,
        null,
        null);

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
        sharedResources,
        incompatibleMutationAdapter,
        1 /*split into 1 byte batches*/,
        false,
        null,
        null);

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
        sharedResources,
        incompatibleMutationAdapter,
        01 /*split into 1 byte batches*/,
        false,
        null,
        null);

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
        sharedResources,
        incompatibleMutationAdapter,
        01 /*split into 1 byte batches*/,
        false,
        null,
        null);

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
    replicator.start(sharedResources, incompatibleMutationAdapter, 2, false, null, null);

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
   * Bidirectional replication should add a special mutation on mutations its replicates. This
   * should be reflected in both metrics and the adapted WAL log.
   *
   * @throws IOException
   */
  @Test
  public void testBidirectionalReplicationAddsSpecialMutation() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    // Enable bidirectional replication
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    Cell cell = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    // Special mutation with source-tagging
    Cell hbaseSourceTaggedCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_HBASE_QUALIFIER.getBytes(), 0l, KeyValue.Type.Delete);

    BigtableWALEntry inputWAL =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(inputWAL));

    BigtableWALEntry expectedWAL =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(cell, hbaseSourceTaggedCell), TABLE_NAME_STRING);
    // The expected batch should include the special mutation
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(1)), expectedWAL.getCells());

    // Everything succeeds
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
    // Metric reflects that one hbase row mutation was replicated
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, 1);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  /**
   * Bidirectional replication should drop mutations that come from an external replicated source.
   *
   * @throws IOException
   */
  @Test
  public void testBidirectionalReplicationDropsReplicatedMutation() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    // Enable bidirectional replication
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    Cell cell = new KeyValue(getRowKey(1), CF1, COL_QUALIFIER, TIMESTAMP, getValue(1));

    Cell cbtSourceTaggedCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_CBT_QUALIFIER.getBytes(), 0l, KeyValue.Type.Delete);
    BigtableWALEntry inputWAL =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(cell, cbtSourceTaggedCell), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(inputWAL));

    // Trigger replication
    assertTrue(replicator.replicate(walsToReplicate));

    // called during constructor
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    // Expect one dropped mutation
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY, 1);
    // Expect no executor interactions because the sole WAL entry was dropped
    verifyNoInteractions(mockExecutorService);
  }

  /**
   * Bidirectional replication combined test to check that replicator can drop the
   * externally-replicated mutation but keep and replicate source mutation.
   *
   * @throws IOException
   */
  @Test
  public void testBidirectionalReplicationDropsOneReplicatesOther() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    // Enable bidirectional replication manually.
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    Cell cellFromCbt = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    // Special replicated mutation from CBT.
    Cell cbtSourceTaggedCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_CBT_QUALIFIER.getBytes(), 0l, KeyValue.Type.Delete);

    Cell cellFromHbase = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    Cell hbaseSourceTaggedCell =
        new KeyValue(
            getRowKey(2), CF1, CBT_REPL_HBASE_QUALIFIER.getBytes(), 0l, KeyValue.Type.Delete);

    // Create respective WAL entries.
    // Entry 1 will be filtered out.
    BigtableWALEntry inputWALToBeDropped =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(cellFromCbt, cbtSourceTaggedCell), TABLE_NAME_STRING);
    // Entry 2 should be replicated.
    BigtableWALEntry inputWALToBeReplicated =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cellFromHbase), TABLE_NAME_STRING);
    BigtableWALEntry expectedWALToBeReplicated =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(cellFromHbase, hbaseSourceTaggedCell), TABLE_NAME_STRING);

    // Create WAL for replicator.
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(
        TABLE_NAME_STRING, Arrays.asList(inputWALToBeDropped, inputWALToBeReplicated));

    // Create expected state of replicationTask input.
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(2)), expectedWALToBeReplicated.getCells());

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
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    // Verify one cell was replicated.
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY, 1);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  /**
   * Bidirectional replication allows users to specify custom special column-qualifiers. Both CBT
   * and HBase qualifiers are tested on 2 rows. One should be filtered out and the other gets
   * replicated.
   *
   * @throws IOException
   */
  @Test
  public void testBidirectionalReplicationCustomSpecialColumnQualifier() throws IOException {

    String customSourceCBTQualifier = "cccbt";
    String customSourceHBaseQualifier = "hhhbase";

    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    // Enable bidirectional replication manually.
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    // Set custom special column qualifiers
    replicationConf.set(CBT_REPL_CBT_QUALIFIER_KEY, customSourceCBTQualifier);
    replicationConf.set(CBT_REPL_HBASE_QUALIFIER_KEY, customSourceHBaseQualifier);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    Cell cellFromCbt = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    // Special replicated mutation from CBT.
    Cell cbtSourceTaggedCell =
        new KeyValue(
            getRowKey(1), CF1, customSourceCBTQualifier.getBytes(), 0l, KeyValue.Type.Delete);

    Cell cellFromHbase = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(1));
    Cell hbaseSourceTaggedCell =
        new KeyValue(
            getRowKey(2), CF1, customSourceHBaseQualifier.getBytes(), 0l, KeyValue.Type.Delete);

    // Create respective WAL entries.
    // Entry 1 will be filtered out.
    BigtableWALEntry inputWALToBeDropped =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(cellFromCbt, cbtSourceTaggedCell), TABLE_NAME_STRING);
    // Entry 2 should be replicated.
    BigtableWALEntry inputWALToBeReplicated =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cellFromHbase), TABLE_NAME_STRING);
    BigtableWALEntry expectedWALToBeReplicated =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(cellFromHbase, hbaseSourceTaggedCell), TABLE_NAME_STRING);

    // Create WAL for replicator.
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(
        TABLE_NAME_STRING, Arrays.asList(inputWALToBeDropped, inputWALToBeReplicated));

    // Create expected state of replicationTask input.
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(2)), expectedWALToBeReplicated.getCells());

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
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    // Verify one cell was replicated.
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY, 1);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  /**
   * Test that multiple mutations are correctly processed in bidirectional replication.
   *
   * @throws IOException
   */
  @Test
  public void testBidirectionalReplicationMultipleMutations() throws IOException {
    // Create object to test
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    // Enable bidirectional replication
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    Cell cell = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    Cell cell2 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    Cell cell3 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    // Special mutation with source-tagging
    Cell hbaseSourceTaggedCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_HBASE_QUALIFIER.getBytes(), 0l, KeyValue.Type.Delete);

    BigtableWALEntry inputWAL =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell, cell2, cell3), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(inputWAL));

    BigtableWALEntry expectedWAL =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(cell, cell2, cell3, hbaseSourceTaggedCell), TABLE_NAME_STRING);
    // The expected batch should include the special mutation
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(1)), expectedWAL.getCells());

    // Everything succeeds
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
    // Metric reflects that one hbase row mutation was replicated
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, 3);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testReplicateAppliesTableNameMapping() throws IOException {
    // Map the namespaced HBase table to a flat Cloud Bigtable table ID, leave the other one alone.
    String namespacedTable = "my_namespace:" + TABLE_NAME_STRING;
    String mappedTable = "my_namespace_" + TABLE_NAME_STRING;
    Configuration mappingConf = new Configuration(false);
    mappingConf.set(
        TABLE_NAME_MAPPING_KEY, "{\"" + namespacedTable + "\": \"" + mappedTable + "\"}");

    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false, mappingConf, null);

    Cell cell1 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell1), namespacedTable);
    Cell cell2 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(2));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell2), TABLE_NAME_STRING_2);

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(namespacedTable, Arrays.asList(walEntry1));
    walsToReplicate.put(TABLE_NAME_STRING_2, Arrays.asList(walEntry2));

    Map<ByteRange, List<Cell>> expectedMappedBatch = new HashMap<>();
    expectedMappedBatch.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    Map<ByteRange, List<Cell>> expectedUnmappedBatch = new HashMap<>();
    expectedUnmappedBatch.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    // The namespaced table is written to the mapped Cloud Bigtable table...
    verify(mockExecutorService)
        .submit(new CloudBigtableReplicationTask(mappedTable, mockConnection, expectedMappedBatch));
    // ...and the unmapped table keeps its HBase name.
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING_2, mockConnection, expectedUnmappedBatch));
    verifyNoMoreInteractions(mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testStartFailsOnInvalidTableNameMapping() {
    Configuration mappingConf = new Configuration(false);
    // ':' is not a valid character in a Cloud Bigtable table ID.
    mappingConf.set(TABLE_NAME_MAPPING_KEY, "{\"ns:table\": \"ns:table\"}");

    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    expectThrows(
        IllegalArgumentException.class,
        () ->
            replicator.start(
                sharedResources, incompatibleMutationAdapter, 2000, false, mappingConf, null));
  }

  @Test
  public void testReplicateCoalescesSourceTablesMappingToSameBigtableTable() throws IOException {
    String mappedTable = "mapped_target";
    Configuration mappingConf = new Configuration(false);
    mappingConf.set(TABLE_NAME_MAPPING_KEY, "{\"table1\": \"" + mappedTable + "\"}");

    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false, mappingConf, null);

    Cell cell1 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry1 = new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell1), "table1");
    Cell cell2 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP + 1, getValue(2));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP + 1, Arrays.asList(cell2), "default:table1");

    Map<String, List<BigtableWALEntry>> walsToReplicate = new LinkedHashMap<>();
    walsToReplicate.put("table1", Arrays.asList(walEntry1));
    walsToReplicate.put("default:table1", Arrays.asList(walEntry2));

    // Both entries should be merged into a single batch and grouped under the same row key.
    Map<ByteRange, List<Cell>> expectedBatch = new HashMap<>();
    expectedBatch.put(new SimpleByteRange(getRowKey(1)), Arrays.asList(cell1, cell2));

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    // Verify only ONE task was submitted for mappedTable, containing both mutations.
    verify(mockExecutorService)
        .submit(new CloudBigtableReplicationTask(mappedTable, mockConnection, expectedBatch));
    verifyNoMoreInteractions(mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testReplicateCoalescesUnmappedDefaultNamespaceAliases() throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false, conf, null);

    Cell cell1 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell1), TABLE_NAME_STRING);
    Cell cell2 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP + 1, getValue(2));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP + 1, Arrays.asList(cell2), "default:" + TABLE_NAME_STRING);

    Map<String, List<BigtableWALEntry>> walsToReplicate = new LinkedHashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1));
    walsToReplicate.put("default:" + TABLE_NAME_STRING, Arrays.asList(walEntry2));

    Map<ByteRange, List<Cell>> expectedBatch = new HashMap<>();
    expectedBatch.put(new SimpleByteRange(getRowKey(1)), Arrays.asList(cell1, cell2));

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockExecutorService)
        .submit(new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch));
    verifyNoMoreInteractions(mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testStartFailsOnMissingTableNameMappingFile() {
    Configuration mappingConf = new Configuration(false);
    mappingConf.set(TABLE_NAME_MAPPING_FILE_KEY, "file:///definitely/not/a/real/mapping.json");

    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    expectThrows(
        IllegalStateException.class,
        () ->
            replicator.start(
                sharedResources, incompatibleMutationAdapter, 2000, false, mappingConf, null));
  }

  @Test
  public void testReplicateHandlesNullAndBlankHBaseTableNamesGracefully() throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false, conf, null);

    Cell cell = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell), TABLE_NAME_STRING);

    Map<String, List<BigtableWALEntry>> walsToReplicate = new LinkedHashMap<>();
    walsToReplicate.put(null, Arrays.asList(walEntry));
    walsToReplicate.put("   ", Arrays.asList(walEntry));
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry));

    Map<ByteRange, List<Cell>> expectedBatch = new HashMap<>();
    expectedBatch.put(new SimpleByteRange(getRowKey(1)), Arrays.asList(cell));

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    // Must not throw NullPointerException or IllegalArgumentException
    assertTrue(replicator.replicate(walsToReplicate));

    // Only the valid table entry is replicated
    verify(mockExecutorService)
        .submit(new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch));
    verifyNoMoreInteractions(mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testReplicateCatchesUnexpectedExceptionAndReturnsFalse() {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false, conf, null);

    Cell cell = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry walEntry =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell), TABLE_NAME_STRING);

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry));

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenThrow(new RuntimeException("Simulated unexpected thread pool failure"));

    // Must not propagate RuntimeException to caller, must return false
    assertFalse(replicator.replicate(walsToReplicate));
  }

  // --- Tests for Finding 1: Empty WALEntry / WALEdit poison pill fix ---

  @Test
  public void testBidirectionalReplicationSkipsEmptyWalEntryWithoutFailing() {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    BigtableWALEntry emptyEntry =
        new BigtableWALEntry(TIMESTAMP, Collections.<Cell>emptyList(), TABLE_NAME_STRING);

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(emptyEntry));

    // Must return true without throwing IndexOutOfBoundsException
    assertTrue(replicator.replicate(walsToReplicate));

    // Metric counters called during constructor only
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    // No replication tasks submitted
    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testBidirectionalReplicationSkipsNullCellsWalEntryWithoutFailing() {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    BigtableWALEntry nullCellsEntry = new BigtableWALEntry(TIMESTAMP, null, TABLE_NAME_STRING);

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(nullCellsEntry));

    // Must return true without throwing NullPointerException
    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testBidirectionalReplicationSkipsNullWalEntryWithoutFailing() {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Collections.singletonList((BigtableWALEntry) null));

    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testOneWayReplicationSkipsEmptyWalEntryWithoutIncrementingMetrics() {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(
        sharedResources, incompatibleMutationAdapter, 2000, false, conf, mockMetricExporter);

    BigtableWALEntry emptyEntry =
        new BigtableWALEntry(TIMESTAMP, Collections.<Cell>emptyList(), TABLE_NAME_STRING);

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(emptyEntry));

    assertTrue(replicator.replicate(walsToReplicate));

    // Verify ONE_WAY_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY was NOT incremented
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testReplicateFiltersEmptyWalEntriesAndReplicatesValidOnes() throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    BigtableWALEntry emptyEntry =
        new BigtableWALEntry(TIMESTAMP, Collections.<Cell>emptyList(), TABLE_NAME_STRING);
    Cell validCell = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry validEntry =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(validCell), TABLE_NAME_STRING);

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(emptyEntry, validEntry));

    Cell sourceMarker =
        new KeyValue(
            getRowKey(1),
            CF1,
            CBT_REPL_HBASE_QUALIFIER.getBytes(StandardCharsets.UTF_8),
            0L,
            KeyValue.Type.Delete);
    Map<ByteRange, List<Cell>> expectedBatch = new HashMap<>();
    expectedBatch.put(new SimpleByteRange(getRowKey(1)), Arrays.asList(validCell, sourceMarker));

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockExecutorService)
        .submit(new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch));
  }

  // --- Tests for Finding 2: Identity fallback collisions & cross-batch safety ---

  @Test
  public void testReplicateFailsOnUnmappedIdentityCollisionWithMappedTarget() {
    Configuration mappingConf = new Configuration(false);
    // Explicitly map ns:table_B to table_B
    mappingConf.set(TABLE_NAME_MAPPING_KEY, "{\"ns:table_B\": \"table_B\"}");

    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false, mappingConf, null);

    Cell cell = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    // An unmapped default-namespace table named table_B
    BigtableWALEntry unmappedEntry =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell), "table_B");

    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put("table_B", Arrays.asList(unmappedEntry));

    // TableNameMapper throws IllegalArgumentException on unmapped collision,
    // which replicate() must catch, log, and return false.
    assertFalse(replicator.replicate(walsToReplicate));
    verifyNoInteractions(mockExecutorService);
  }

  @Test
  public void testReplicateFailsWhenBatchContainsBothMappedAndUnmappedCollidingTables() {
    Configuration mappingConf = new Configuration(false);
    // Explicitly map ns:table_B to table_B
    mappingConf.set(TABLE_NAME_MAPPING_KEY, "{\"ns:table_B\": \"table_B\"}");

    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, 2000, false, mappingConf, null);

    Cell cell1 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    BigtableWALEntry mappedEntry =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell1), "ns:table_B");

    Cell cell2 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(2));
    BigtableWALEntry unmappedEntry =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell2), "table_B");

    Map<String, List<BigtableWALEntry>> walsToReplicate = new LinkedHashMap<>();
    walsToReplicate.put("ns:table_B", Arrays.asList(mappedEntry));
    walsToReplicate.put("table_B", Arrays.asList(unmappedEntry));

    assertFalse(replicator.replicate(walsToReplicate));
    verifyNoInteractions(mockExecutorService);
  }

  // --- Tests for Finding 3: UTF-8 qualifiers for bidirectional markers ---

  @Test
  public void testBidirectionalReplicationSupportsUtf8Qualifiers() throws IOException {
    String customCbtQualifier = "_source_cbt_日本語";
    String customHbaseQualifier = "_source_hbase_日本語";

    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicationConf.set(CBT_REPL_CBT_QUALIFIER_KEY, customCbtQualifier);
    replicationConf.set(CBT_REPL_HBASE_QUALIFIER_KEY, customHbaseQualifier);

    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    // 1. CBT marker with UTF-8 qualifier should be dropped
    Cell cbtMarkerCell =
        new KeyValue(
            getRowKey(1),
            CF1,
            customCbtQualifier.getBytes(StandardCharsets.UTF_8),
            0L,
            KeyValue.Type.Delete);
    BigtableWALEntry cbtEntry =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cbtMarkerCell), TABLE_NAME_STRING);

    assertTrue(
        replicator.replicate(Collections.singletonMap(TABLE_NAME_STRING, Arrays.asList(cbtEntry))));
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verifyNoInteractions(mockExecutorService);

    // 2. HBase mutation should be tagged with UTF-8 HBase qualifier
    Cell userCell = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(2));
    BigtableWALEntry hbaseEntry =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(userCell), TABLE_NAME_STRING);

    Cell expectedMarker =
        new KeyValue(
            getRowKey(2),
            CF1,
            customHbaseQualifier.getBytes(StandardCharsets.UTF_8),
            0L,
            KeyValue.Type.Delete);
    Map<ByteRange, List<Cell>> expectedBatch = new HashMap<>();
    expectedBatch.put(new SimpleByteRange(getRowKey(2)), Arrays.asList(userCell, expectedMarker));

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(
        replicator.replicate(
            Collections.singletonMap(TABLE_NAME_STRING, Arrays.asList(hbaseEntry))));

    verify(mockExecutorService)
        .submit(new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch));
  }

  // --- Tests for Round 3 Finding 1: Robust Bidirectional Marker Detection ---

  @Test
  public void testBidirectionalReplicationDetectsMarkerAtFirstPositionDropsEntry()
      throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    // CBT source marker delete placed as the FIRST cell (index 0), followed by a normal cell
    Cell cbtMarkerCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_CBT_QUALIFIER.getBytes(), 0L, KeyValue.Type.Delete);
    Cell userCell = new KeyValue(getRowKey(1), CF1, COL_QUALIFIER, TIMESTAMP, getValue(1));

    BigtableWALEntry inputWAL =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cbtMarkerCell, userCell), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(inputWAL));

    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    // Expect the WAL entry to be dropped because marker is detected at first position
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY, 1);
    verifyNoInteractions(mockExecutorService);
  }

  @Test
  public void testBidirectionalReplicationUserPutWithMarkerQualifierIsNotDropped()
      throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    // Normal user Put cell with qualifier equal to CBT_REPL_CBT_QUALIFIER and non-zero timestamp
    Cell userPutCell =
        new KeyValue(getRowKey(1), CF1, CBT_REPL_CBT_QUALIFIER.getBytes(), TIMESTAMP, getValue(1));

    BigtableWALEntry inputWAL =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(userPutCell), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(inputWAL));

    // When replicated, HBase source marker is appended to the row cells
    Cell hbaseSourceTaggedCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_HBASE_QUALIFIER.getBytes(), 0L, KeyValue.Type.Delete);
    BigtableWALEntry expectedWAL =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(userPutCell, hbaseSourceTaggedCell), TABLE_NAME_STRING);
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(1)), expectedWAL.getCells());

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    // Expect WAL entry NOT to be dropped; eligible metrics should be recorded
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, 1);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testBidirectionalReplicationDetectsMarkerInMiddlePositionDropsEntry()
      throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    // Cells sorted such that CBT marker delete is in the MIDDLE between user cells
    Cell userCell1 =
        new KeyValue(
            getRowKey(1), CF1, "col_a".getBytes(StandardCharsets.UTF_8), TIMESTAMP, getValue(1));
    Cell cbtMarkerCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_CBT_QUALIFIER.getBytes(), 0L, KeyValue.Type.Delete);
    Cell userCell2 =
        new KeyValue(
            getRowKey(1), CF1, "col_z".getBytes(StandardCharsets.UTF_8), TIMESTAMP, getValue(2));

    BigtableWALEntry inputWAL =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(userCell1, cbtMarkerCell, userCell2), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(inputWAL));

    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    // Entry dropped, 2 user mutations counted as ineligible (3 total - 1 marker)
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY, 2);
    verifyNoInteractions(mockExecutorService);
  }

  @Test
  public void testBidirectionalReplicationUserDeleteWithMarkerQualifierIsNotDropped()
      throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    // Normal user Delete cell with qualifier equal to CBT_REPL_CBT_QUALIFIER and non-zero timestamp
    Cell userDeleteCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_CBT_QUALIFIER.getBytes(), TIMESTAMP, KeyValue.Type.Delete);

    BigtableWALEntry inputWAL =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(userDeleteCell), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(inputWAL));

    Cell hbaseSourceTaggedCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_HBASE_QUALIFIER.getBytes(), 0L, KeyValue.Type.Delete);
    BigtableWALEntry expectedWAL =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(userDeleteCell, hbaseSourceTaggedCell), TABLE_NAME_STRING);
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(1)), expectedWAL.getCells());

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, 1);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testBidirectionalReplicationUserPutWithMarkerQualifierAtTimestampZeroIsNotDropped()
      throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    // Normal user Put cell with qualifier equal to CBT_REPL_CBT_QUALIFIER and timestamp 0L (Put,
    // not Delete)
    Cell userPutCell =
        new KeyValue(getRowKey(1), CF1, CBT_REPL_CBT_QUALIFIER.getBytes(), 0L, KeyValue.Type.Put);

    BigtableWALEntry inputWAL =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(userPutCell), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(inputWAL));

    Cell hbaseSourceTaggedCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_HBASE_QUALIFIER.getBytes(), 0L, KeyValue.Type.Delete);
    BigtableWALEntry expectedWAL =
        new BigtableWALEntry(
            TIMESTAMP, Arrays.asList(userPutCell, hbaseSourceTaggedCell), TABLE_NAME_STRING);
    Map<ByteRange, List<Cell>> expectedBatchOfWal = new HashMap<>();
    expectedBatchOfWal.put(new SimpleByteRange(getRowKey(1)), expectedWAL.getCells());

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, 1);

    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(
                TABLE_NAME_STRING, mockConnection, expectedBatchOfWal));
    verifyNoMoreInteractions(mockMetricExporter, mockExecutorService);
    verifyNoInteractions(mockConnection);
  }

  @Test
  public void testBidirectionalReplicationDetectsDeleteColumnMarkerDropsEntry() throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    Configuration replicationConf = new Configuration(false);
    replicationConf.setBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, true);
    replicator.start(
        sharedResources,
        incompatibleMutationAdapter,
        2000,
        false,
        replicationConf,
        mockMetricExporter);

    // CBT source marker delete using DeleteColumn type at timestamp 0L
    Cell cbtMarkerCell =
        new KeyValue(
            getRowKey(1), CF1, CBT_REPL_CBT_QUALIFIER.getBytes(), 0L, KeyValue.Type.DeleteColumn);
    Cell userCell = new KeyValue(getRowKey(1), CF1, COL_QUALIFIER, TIMESTAMP, getValue(1));

    BigtableWALEntry inputWAL =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cbtMarkerCell, userCell), TABLE_NAME_STRING);
    Map<String, List<BigtableWALEntry>> walsToReplicate = new HashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(inputWAL));

    assertTrue(replicator.replicate(walsToReplicate));

    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(mockMetricExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
    verify(mockMetricExporter).incCounters(BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY, 1);
    verifyNoInteractions(mockExecutorService);
  }

  // --- Tests for Finding 4: Pre-flush batch threshold check ---

  @Test
  public void testReplicatePreFlushesBatchBeforeAddingRowExceedingSizeThreshold()
      throws IOException {
    Cell cell1 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    int singleRowSize = (int) org.apache.hadoop.hbase.CellUtil.estimatedHeapSizeOf(cell1);

    // Set threshold so that 1 row fits, but 2 rows exceed it
    int threshold = (int) (singleRowSize * 1.5);

    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, threshold, false, null, null);

    Cell cell2 = new KeyValue(getRowKey(2), CF1, null, TIMESTAMP, getValue(2));
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell1), TABLE_NAME_STRING);
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell2), TABLE_NAME_STRING);

    Map<ByteRange, List<Cell>> expectedBatch1 = new HashMap<>();
    expectedBatch1.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    Map<ByteRange, List<Cell>> expectedBatch2 = new HashMap<>();
    expectedBatch2.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());

    Map<String, List<BigtableWALEntry>> walsToReplicate = new LinkedHashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2));

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    // Verify that the batch was pre-flushed before adding row 2, producing 2 distinct batches
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch1));
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch2));
  }

  @Test
  public void testReplicatePreFlushesBatchBeforeAddingRowExceedingCellCountThreshold()
      throws IOException {
    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    // High byte threshold so only cell count threshold (100,000) triggers
    replicator.start(
        sharedResources, incompatibleMutationAdapter, Integer.MAX_VALUE, false, null, null);

    // Row 1 with 60,000 cells
    Cell[] row1Cells = new Cell[60_000];
    for (int i = 0; i < 60_000; i++) {
      row1Cells[i] =
          new KeyValue(
              getRowKey(1),
              CF1,
              ("col" + i).getBytes(StandardCharsets.UTF_8),
              TIMESTAMP,
              getValue(1));
    }
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(row1Cells), TABLE_NAME_STRING);

    // Row 2 with 50,000 cells (60,000 + 50,000 = 110,000 >= 100,000)
    Cell[] row2Cells = new Cell[50_000];
    for (int i = 0; i < 50_000; i++) {
      row2Cells[i] =
          new KeyValue(
              getRowKey(2),
              CF1,
              ("col" + i).getBytes(StandardCharsets.UTF_8),
              TIMESTAMP,
              getValue(2));
    }
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(row2Cells), TABLE_NAME_STRING);

    Map<ByteRange, List<Cell>> expectedBatch1 = new HashMap<>();
    expectedBatch1.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    Map<ByteRange, List<Cell>> expectedBatch2 = new HashMap<>();
    expectedBatch2.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());

    Map<String, List<BigtableWALEntry>> walsToReplicate = new LinkedHashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2));

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    // Verify row 1 was pre-flushed before row 2 was added, resulting in 2 separate tasks
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch1));
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch2));
  }

  @Test
  public void testReplicatePreFlushesAndPostFlushesOversizedRow() throws IOException {
    Cell cell1 = new KeyValue(getRowKey(1), CF1, null, TIMESTAMP, getValue(1));
    int singleCellSize = (int) org.apache.hadoop.hbase.CellUtil.estimatedHeapSizeOf(cell1);

    // Threshold is 2 * singleCellSize
    int threshold = singleCellSize * 2;

    CloudBigtableReplicator replicator = new CloudBigtableReplicator();
    replicator.start(sharedResources, incompatibleMutationAdapter, threshold, false, null, null);

    // Row 1: 1 cell (fits)
    BigtableWALEntry walEntry1 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell1), TABLE_NAME_STRING);

    // Row 2: 3 cells (3 * singleCellSize > threshold, oversized row)
    Cell cell2a =
        new KeyValue(
            getRowKey(2), CF1, "c1".getBytes(StandardCharsets.UTF_8), TIMESTAMP, getValue(2));
    Cell cell2b =
        new KeyValue(
            getRowKey(2), CF1, "c2".getBytes(StandardCharsets.UTF_8), TIMESTAMP, getValue(2));
    Cell cell2c =
        new KeyValue(
            getRowKey(2), CF1, "c3".getBytes(StandardCharsets.UTF_8), TIMESTAMP, getValue(2));
    BigtableWALEntry walEntry2 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell2a, cell2b, cell2c), TABLE_NAME_STRING);

    // Row 3: 1 cell (fits)
    Cell cell3 = new KeyValue(getRowKey(3), CF1, null, TIMESTAMP, getValue(3));
    BigtableWALEntry walEntry3 =
        new BigtableWALEntry(TIMESTAMP, Arrays.asList(cell3), TABLE_NAME_STRING);

    Map<ByteRange, List<Cell>> expectedBatch1 = new HashMap<>();
    expectedBatch1.put(new SimpleByteRange(getRowKey(1)), walEntry1.getCells());
    Map<ByteRange, List<Cell>> expectedBatch2 = new HashMap<>();
    expectedBatch2.put(new SimpleByteRange(getRowKey(2)), walEntry2.getCells());
    Map<ByteRange, List<Cell>> expectedBatch3 = new HashMap<>();
    expectedBatch3.put(new SimpleByteRange(getRowKey(3)), walEntry3.getCells());

    Map<String, List<BigtableWALEntry>> walsToReplicate = new LinkedHashMap<>();
    walsToReplicate.put(TABLE_NAME_STRING, Arrays.asList(walEntry1, walEntry2, walEntry3));

    when(mockExecutorService.submit((Callable<Object>) any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertTrue(replicator.replicate(walsToReplicate));

    // Verify 3 distinct batches: Row 1 pre-flushed before Row 2, Row 2 post-flushed immediately,
    // Row 3 flushed at end.
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch1));
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch2));
    verify(mockExecutorService)
        .submit(
            new CloudBigtableReplicationTask(TABLE_NAME_STRING, mockConnection, expectedBatch3));
  }

  /** JUnit 4.12 (pulled in transitively by hbase-client) does not have assertThrows. */
  private static void expectThrows(Class<? extends Throwable> expected, ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Throwable t) {
      if (expected.isInstance(t)) {
        return;
      }
      throw new AssertionError(
          "Expected " + expected.getName() + " but got " + t.getClass().getName(), t);
    }
    fail("Expected " + expected.getName() + " to be thrown.");
  }

  private interface ThrowingRunnable {
    void run() throws Throwable;
  }
}
