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

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_FILTER_LARGE_CELLS;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.FILTER_LARGE_CELLS_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_CELL_SIZE_EXCEEDED_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.PUTS_IN_FUTURE_METRIC_KEY;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class IncompatibleMutationAdapterTest {

  static class TestIncompatibleMutationAdapter extends IncompatibleMutationAdapter {

    /**
     * The output for AdaptImpl based on index in the walEntry.getEdit.getCells() collection based
     * on index in the walEntry.getEdit.getCells() collection.
     */
    Map<Integer, List<Cell>> adaptedEntryMap = new HashMap<>();

    /**
     * The set of indexes where mutations can't be adapted. This set takes precedence over
     * adaptedEntryMap when same index is set on both.
     */
    Set<Integer> incompatibleMutations = new HashSet<>();

    // Configuration
    private Configuration conf;

    /**
     * Creates an IncompatibleMutationAdapter with HBase configuration, MetricSource, and CBT Table.
     *
     * <p>All subclasses must expose this constructor.
     *
     * @param conf HBase configuration. All the configurations required by subclases should come
     *     from here.
     * @param metricsExporter Interface for exposing Hadoop metric source.
     * @param connection CBT table taht is destination of the replicated edits. This
     */
    public TestIncompatibleMutationAdapter(
        Configuration conf, MetricsExporter metricsExporter, Connection connection) {
      super(conf, metricsExporter, connection);
      this.conf = conf;
    }

    @Override
    protected List<Cell> adaptIncompatibleMutation(BigtableWALEntry walEntry, int index) {
      if (incompatibleMutations.contains(index)) {
        throw new UnsupportedOperationException();
      }
      if (!adaptedEntryMap.containsKey(index)) {
        throw new IllegalStateException("Expected value to be set for index " + index);
      }
      return adaptedEntryMap.get(index);
    }

    public void reset() {
      incompatibleMutations.clear();
      adaptedEntryMap.clear();
    }
  }

  private static final byte[] rowKey = "rowKey".getBytes(StandardCharsets.UTF_8);
  private static final byte[] cf = "family".getBytes(StandardCharsets.UTF_8);
  private static final byte[] qual = "qual".getBytes(StandardCharsets.UTF_8);
  private static final byte[] val = "value".getBytes(StandardCharsets.UTF_8);
  private static final String tableName = "test-table";

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private final Configuration conf = new Configuration(false);

  @Mock Connection connection;

  @Mock MetricsExporter metricsExporter;

  TestIncompatibleMutationAdapter incompatibleMutationAdapter;

  @Before
  public void setUp() throws Exception {
    incompatibleMutationAdapter =
        new TestIncompatibleMutationAdapter(conf, metricsExporter, connection);
  }

  @After
  public void tearDown() throws Exception {
    verifyNoInteractions(connection);
    reset(connection, metricsExporter);
    incompatibleMutationAdapter.reset();
  }

  @Test
  public void testCompatibleMutationsAreNotChanged() {
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    Cell put2 = new KeyValue(rowKey, cf, qual, 10, KeyValue.Type.Put, val);
    Cell compatibleDelete = new KeyValue(rowKey, cf, qual, 10, KeyValue.Type.Delete);
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    walEntryCells.add(put);
    walEntryCells.add(put2);
    walEntryCells.add(compatibleDelete);

    BigtableWALEntry walEntry =
        new BigtableWALEntry(System.currentTimeMillis(), walEntryCells, tableName);

    Assert.assertEquals(
        walEntryCells, incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verifyNoMoreInteractions(metricsExporter);
  }

  @Test
  public void testDeletesAreAdapted() {
    ArrayList<Cell> walEdit = new ArrayList<>();
    Cell delete = new KeyValue(rowKey, cf, null, 0, KeyValue.Type.DeleteFamilyVersion);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    walEdit.add(put);
    walEdit.add(delete);
    BigtableWALEntry walEntry =
        new BigtableWALEntry(System.currentTimeMillis(), walEdit, tableName);
    Cell expectedDelete = new KeyValue(rowKey, cf, null, 0, KeyValue.Type.DeleteFamily);
    incompatibleMutationAdapter.adaptedEntryMap.put(1, Arrays.asList(expectedDelete));

    Assert.assertEquals(
        Arrays.asList(put, expectedDelete),
        incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verifyNoMoreInteractions(metricsExporter);
  }

  @Test
  public void testDeleteCanCreateManyDeletes() {
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    Cell delete = new KeyValue(rowKey, cf, null, 1000, KeyValue.Type.DeleteFamily);
    walEntryCells.add(delete);
    BigtableWALEntry walEntry =
        new BigtableWALEntry(System.currentTimeMillis(), walEntryCells, tableName);

    // A single deleteFamily becomes 2 delete cells. This can happen when we call CBT and find out
    // there were 2 cells in the family before timestamp 100
    List<Cell> expectedDeletes =
        Arrays.asList(
            new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Delete),
            new KeyValue(rowKey, cf, qual, 10, KeyValue.Type.Delete));
    incompatibleMutationAdapter.adaptedEntryMap.put(0, expectedDeletes);

    Assert.assertEquals(
        expectedDeletes, incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verifyNoMoreInteractions(metricsExporter);
  }

  @Test
  public void testUnknownMutationTypesAreDropped() {
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    Cell incompatible = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Maximum);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    walEntryCells.add(incompatible);
    walEntryCells.add(put);
    BigtableWALEntry walEntry =
        new BigtableWALEntry(System.currentTimeMillis(), walEntryCells, tableName);

    Assert.assertEquals(
        Arrays.asList(put), incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verifyNoMoreInteractions(metricsExporter);
  }

  @Test
  public void testIncompatibleDeletesAreDropped() {
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    Cell incompatible = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.DeleteFamilyVersion);
    walEntryCells.add(put);
    walEntryCells.add(incompatible);
    BigtableWALEntry walEntry =
        new BigtableWALEntry(System.currentTimeMillis(), walEntryCells, tableName);
    incompatibleMutationAdapter.incompatibleMutations.add(1);

    Assert.assertEquals(
        Arrays.asList(put), incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verifyNoMoreInteractions(metricsExporter);
  }

  @Test
  public void testTimestampsOverflowMutations() {
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    Cell put1 = new KeyValue(rowKey, cf, qual, Long.MAX_VALUE - 1, KeyValue.Type.Put, val);
    Cell put2 = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    // make sure this is not flagged as overflow.
    Cell delete =
        new KeyValue(rowKey, cf, qual, HConstants.LATEST_TIMESTAMP, Type.DeleteFamily, val);
    walEntryCells.add(put1);
    walEntryCells.add(put2);
    walEntryCells.add(delete);
    BigtableWALEntry walEntry = new BigtableWALEntry(100L, walEntryCells, tableName);

    Assert.assertEquals(
        Arrays.asList(put1, put2, delete),
        incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(metricsExporter, times(1))
        .incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 1);

    verifyNoMoreInteractions(metricsExporter);
  }

  @Test
  public void testFuturePutAreFlagged() {
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    Cell put1 = new KeyValue(rowKey, cf, qual, 900, Type.Put);
    Cell put2 = new KeyValue(rowKey, cf, qual, 1005L, Type.Put);
    walEntryCells.add(put1);
    walEntryCells.add(put2);
    BigtableWALEntry walEntry = new BigtableWALEntry(1000L, walEntryCells, tableName);

    Assert.assertEquals(
        Arrays.asList(put1, put2),
        incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 1);
    verifyNoMoreInteractions(metricsExporter);
  }

  @Test
  public void testFilterLargeCellsFlag() {
    int numBytes = 150 * 1024 * 1024;
    byte[] largeCellValBytes = new byte[numBytes];
    new Random().nextBytes(largeCellValBytes);

    ArrayList<Cell> walEntryCells = new ArrayList<>();
    Cell incompatibleLargePut =
        new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, largeCellValBytes);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    walEntryCells.add(put);
    walEntryCells.add(incompatibleLargePut);
    BigtableWALEntry walEntry =
        new BigtableWALEntry(System.currentTimeMillis(), walEntryCells, tableName);

    Assert.assertEquals(
        conf.getBoolean(FILTER_LARGE_CELLS_KEY, DEFAULT_FILTER_LARGE_CELLS),
        DEFAULT_FILTER_LARGE_CELLS);
    Assert.assertEquals(
        Arrays.asList(put, incompatibleLargePut),
        incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));
  }

  @Test
  public void testFilterLargeCellsDefaultSize() {
    conf.set(FILTER_LARGE_CELLS_KEY, "true");

    ArrayList<Cell> walEntryCells = new ArrayList<>();

    int numBytes = 100 * 1024 * 1024 - 1;
    byte[] b = new byte[numBytes];
    new Random().nextBytes(b);
    Cell putSmallerSize = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, b);

    numBytes = 100 * 1024 * 1024;
    b = new byte[numBytes];
    new Random().nextBytes(b);
    Cell putEqualSize = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, b);

    numBytes = 100 * 1024 * 1024 + 1;
    b = new byte[numBytes];
    new Random().nextBytes(b);
    Cell putLargerSize = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, b);

    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);

    walEntryCells.add(put);
    walEntryCells.add(putSmallerSize);
    walEntryCells.add(putEqualSize);
    walEntryCells.add(putLargerSize);

    BigtableWALEntry walEntry =
        new BigtableWALEntry(System.currentTimeMillis(), walEntryCells, tableName);

    Assert.assertEquals(conf.getBoolean(FILTER_LARGE_CELLS_KEY, DEFAULT_FILTER_LARGE_CELLS), true);
    Assert.assertEquals(
        Arrays.asList(put, putSmallerSize, putEqualSize),
        incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_CELL_SIZE_EXCEEDED_KEY, 0);
    verify(metricsExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);

    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1))
        .incCounters(DROPPED_INCOMPATIBLE_MUTATION_CELL_SIZE_EXCEEDED_KEY, 1);
    verifyNoMoreInteractions(metricsExporter);
  }

  @Test
  public void testFilterLargeCellsCustomSize() {
    conf.set(FILTER_LARGE_CELLS_KEY, "true");
    conf.setInt(FILTER_LARGE_CELLS_THRESHOLD_IN_BYTES_KEY, 10 * 1024 * 1024);

    ArrayList<Cell> walEntryCells = new ArrayList<>();

    int numBytes = 10 * 1024 * 1024 - 1;
    byte[] b = new byte[numBytes];
    new Random().nextBytes(b);
    Cell putSmallerSize = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, b);

    numBytes = 10 * 1024 * 1024;
    b = new byte[numBytes];
    new Random().nextBytes(b);
    Cell putEqualSize = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, b);

    numBytes = 10 * 1024 * 1024 + 1;
    b = new byte[numBytes];
    new Random().nextBytes(b);
    Cell putLargerSize = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, b);

    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);

    walEntryCells.add(put);
    walEntryCells.add(putSmallerSize);
    walEntryCells.add(putEqualSize);
    walEntryCells.add(putLargerSize);

    BigtableWALEntry walEntry =
        new BigtableWALEntry(System.currentTimeMillis(), walEntryCells, tableName);

    Assert.assertEquals(conf.getBoolean(FILTER_LARGE_CELLS_KEY, DEFAULT_FILTER_LARGE_CELLS), true);
    Assert.assertEquals(
        Arrays.asList(put, putSmallerSize, putEqualSize),
        incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry));

    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_DELETES_METRICS_KEY, 0);
    verify(metricsExporter).incCounters(INCOMPATIBLE_MUTATION_TIMESTAMP_OVERFLOW_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(DROPPED_INCOMPATIBLE_MUTATION_CELL_SIZE_EXCEEDED_KEY, 0);
    verify(metricsExporter).incCounters(PUTS_IN_FUTURE_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1))
        .incCounters(DROPPED_INCOMPATIBLE_MUTATION_CELL_SIZE_EXCEEDED_KEY, 1);
    verifyNoMoreInteractions(metricsExporter);
  }
}
