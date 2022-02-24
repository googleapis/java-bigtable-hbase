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

package com.google.cloud.bigtable.hbase.replication.adapters;

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DELETE_FAMILY_WRITE_THRESHOLD_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_METRIC_KEY;
import static org.apache.hadoop.hbase.HConstants.LATEST_TIMESTAMP;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class ApproximatingIncompatibleMutationAdapterTest {

  private static final byte[] rowKey = "rowKey".getBytes(StandardCharsets.UTF_8);
  private static final byte[] cf = "family".getBytes(StandardCharsets.UTF_8);
  private static final byte[] qual = "qual".getBytes(StandardCharsets.UTF_8);
  private static final byte[] val = "value".getBytes(StandardCharsets.UTF_8);

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private Configuration conf;

  @Mock
  Connection connection;

  @Mock
  MetricsExporter metricsExporter;

  @Mock
  BigtableWALEntry mockWalEntry;

  @InjectMocks
  EnvironmentEdgeManager environmentEdgeManager;

  @Mock
  EnvironmentEdge mockEnvironmentEdge;

  ApproximatingIncompatibleMutationAdapter incompatibleMutationAdapter;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration(false);
    conf.setInt(DELETE_FAMILY_WRITE_THRESHOLD_KEY, 10);
    when(mockWalEntry.getWalWriteTime()).thenReturn(1005L);
    // Expectations on Conf should be set before this point.
    incompatibleMutationAdapter =
        new ApproximatingIncompatibleMutationAdapter(conf, metricsExporter, connection);
  }

  @After
  public void tearDown() throws Exception {
    verify(mockWalEntry, atLeast(2)).getCells();
    verify(mockWalEntry, atLeastOnce()).getWalWriteTime();
    verifyNoInteractions(connection);
    reset(mockWalEntry, connection, metricsExporter);
  }

  @Test
  public void testDeletesAreAdapted() {
    Cell delete = new KeyValue(rowKey, cf, null, 1000, KeyValue.Type.DeleteFamily);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    walEntryCells.add(put);
    walEntryCells.add(delete);
    when(mockWalEntry.getCells()).thenReturn(walEntryCells);
    Cell expectedDelete =
        new KeyValue(rowKey, cf, null, LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily);

    Assert.assertEquals(
        Arrays.asList(put, expectedDelete),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
  }

  @Test
  public void testIncompatibleDeletesAreDropped() {
    Cell deleteFamilyVersion = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.DeleteFamilyVersion);
    // Cell timestamp > WAL time, should be rejected.
    Cell deleteFamilyAfterWAL =
        new KeyValue(rowKey, cf, qual, 2000, KeyValue.Type.DeleteFamilyVersion);
    // The WAL entry is written at 1000 with write threshold of 10, anything before 990 is rejected
    Cell deleteFamilyBeforeThreshold =
        new KeyValue(rowKey, cf, qual, 500, KeyValue.Type.DeleteFamily);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    walEntryCells.add(deleteFamilyVersion);
    walEntryCells.add(deleteFamilyAfterWAL);
    walEntryCells.add(deleteFamilyBeforeThreshold);
    walEntryCells.add(put);
    when(mockWalEntry.getWalWriteTime()).thenReturn(1000L);
    when(mockWalEntry.getCells()).thenReturn(walEntryCells);

    Assert.assertEquals(
        Arrays.asList(put), incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter, times(3)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(3)).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }

  @Test
  public void testFutureDeletesAreDropped() {
    final long now = EnvironmentEdgeManager.currentTime();
    when(mockEnvironmentEdge.currentTime()).thenReturn(now);
    Cell put = new KeyValue(rowKey, cf, qual, now * 2, KeyValue.Type.Put, val);
    Cell delete1 = new KeyValue(rowKey, cf, null, now * 2, KeyValue.Type.DeleteFamily);
    Cell delete2 = new KeyValue(rowKey, cf, null, now - 1, KeyValue.Type.DeleteFamily);

    ArrayList<Cell> walEntryCells = new ArrayList<>();
    walEntryCells.add(put);
    walEntryCells.add(delete1);
    walEntryCells.add(delete2);
    when(mockWalEntry.getCells()).thenReturn(walEntryCells);
    when(mockWalEntry.getWalWriteTime()).thenReturn(now - 1);
    Cell expectedDelete =
        new KeyValue(rowKey, cf, null, LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily);
    Assert.assertEquals(
        Arrays.asList(put, expectedDelete),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(metricsExporter, times(2)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }
}
