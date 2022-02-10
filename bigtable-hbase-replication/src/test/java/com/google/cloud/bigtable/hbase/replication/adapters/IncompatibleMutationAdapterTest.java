package com.google.cloud.bigtable.hbase.replication.adapters;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
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

  class TestIncompatibleMutationAdapter extends IncompatibleMutationAdapter {

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

    /**
     * Creates an IncompatibleMutationAdapter with HBase configuration, MetricSource, and CBT
     * Table.
     *
     * All subclasses must expose this constructor.
     *  @param conf HBase configuration. All the configurations required by subclases should come
     * from here.
     * @param metricsExporter Interface for exposing Hadoop metric source.
     * @param connection CBT table taht is destination of the replicated edits. This
     */
    public TestIncompatibleMutationAdapter(Configuration conf,
        MetricsExporter metricsExporter,
        Connection connection) {
      super(conf, metricsExporter, connection);
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

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  Configuration conf;

  @Mock
  Connection connection;

  @Mock
  MetricsExporter metricsExporter;

  @Mock
  BigtableWALEntry mockWalEntry;


  TestIncompatibleMutationAdapter incompatibleMutationAdapter;

  @Before
  public void setUp() throws Exception {
    incompatibleMutationAdapter = new TestIncompatibleMutationAdapter(conf, metricsExporter,
        connection);
  }

  @After
  public void tearDown() throws Exception {
    verifyNoInteractions(connection, conf);
    reset(mockWalEntry, conf, connection, metricsExporter);
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
    when(mockWalEntry.getCells()).thenReturn(walEntryCells);

    Assert.assertEquals(walEntryCells,
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getCells();
    verify(metricsExporter).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
  }

  @Test
  public void testDeletesAreAdapted() {
    ArrayList<Cell> walEdit = new ArrayList<>();
    Cell delete = new KeyValue(rowKey, cf, null, 0, KeyValue.Type.DeleteFamilyVersion);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    walEdit.add(put);
    walEdit.add(delete);
    when(mockWalEntry.getCells()).thenReturn(walEdit);
    Cell expectedDelete = new KeyValue(rowKey, cf, null, 0, KeyValue.Type.DeleteFamily);
    incompatibleMutationAdapter.adaptedEntryMap.put(1, Arrays.asList(expectedDelete));

    Assert.assertEquals(Arrays.asList(put, expectedDelete),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getCells();
    verify(metricsExporter).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
  }

  @Test
  public void testDeleteCanCreateManyDeletes() {
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    Cell delete = new KeyValue(rowKey, cf, null, 1000, KeyValue.Type.DeleteFamily);
    walEntryCells.add(delete);
    when(mockWalEntry.getCells()).thenReturn(walEntryCells);

    // A single deleteFamily becomes 2 delete cells. This can happen when we call CBT and find out
    // there were 2 cells in the family before timestamp 100
    List<Cell> expectedDeletes = Arrays.asList(
        new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Delete),
        new KeyValue(rowKey, cf, qual, 10, KeyValue.Type.Delete)
    );
    incompatibleMutationAdapter.adaptedEntryMap.put(0, expectedDeletes);

    Assert.assertEquals(expectedDeletes,
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getCells();
    verify(metricsExporter).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
  }

  @Test
  public void testUnknownMutationTypesAreDropped() {
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    Cell incompatible = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Maximum);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    walEntryCells.add(incompatible);
    walEntryCells.add(put);
    when(mockWalEntry.getCells()).thenReturn(walEntryCells);

    Assert.assertEquals(Arrays.asList(put),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getCells();
    verify(metricsExporter).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }

  @Test
  public void testIncompatibleDeletesAreDropped() {
    ArrayList<Cell> walEntryCells = new ArrayList<>();
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    Cell incompatible = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.DeleteFamilyVersion);
    walEntryCells.add(put);
    walEntryCells.add(incompatible);
    when(mockWalEntry.getCells()).thenReturn(walEntryCells);
    incompatibleMutationAdapter.incompatibleMutations.add(1);

    Assert.assertEquals(Arrays.asList(put),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getCells();
    verify(metricsExporter).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsExporter, times(1)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsExporter, times(1)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }


}