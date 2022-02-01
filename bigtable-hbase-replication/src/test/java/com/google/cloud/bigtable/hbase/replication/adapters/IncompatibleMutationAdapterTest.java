package com.google.cloud.bigtable.hbase.replication.adapters;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
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
     * @param metricsSource Hadoop metric source exposed by HBase Replication Endpoint.
     * @param connection CBT table taht is destination of the replicated edits. This
     */
    public TestIncompatibleMutationAdapter(Configuration conf,
        MetricsSource metricsSource,
        Connection connection) {
      super(conf, metricsSource, connection);
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
  MetricsSource metricsSource;

  @Mock
  BigtableWALEntry mockWalEntry;


  TestIncompatibleMutationAdapter incompatibleMutationAdapter;

  @Before
  public void setUp() throws Exception {
    incompatibleMutationAdapter = new TestIncompatibleMutationAdapter(conf, metricsSource,
        connection);
  }

  @After
  public void tearDown() throws Exception {
    verifyNoInteractions(connection, conf);
    reset(mockWalEntry, conf, connection, metricsSource);
    incompatibleMutationAdapter.reset();
  }

  @Test
  public void testCompatibleMutationsAreNotChanged() {
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    Cell put2 = new KeyValue(rowKey, cf, qual, 10, KeyValue.Type.Put, val);
    Cell compatibleDelete = new KeyValue(rowKey, cf, qual, 10, KeyValue.Type.Delete);
    ArrayList<Cell> walEdit = new ArrayList<>();
    walEdit.add(put);
    walEdit.add(put2);
    walEdit.add(compatibleDelete);
    when(mockWalEntry.getWalEdit()).thenReturn(walEdit);

    Assert.assertEquals(walEdit,
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getWalEdit();
    verify(metricsSource).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsSource).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
  }

  @Test
  public void testDeletesAreAdapted() {
    ArrayList<Cell> walEdit = new ArrayList<>();
    Cell delete = new KeyValue(rowKey, cf, null, 0, KeyValue.Type.DeleteFamilyVersion);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    walEdit.add(put);
    walEdit.add(delete);
    when(mockWalEntry.getWalEdit()).thenReturn(walEdit);
    Cell expectedDelete = new KeyValue(rowKey, cf, null, 0, KeyValue.Type.DeleteFamily);
    incompatibleMutationAdapter.adaptedEntryMap.put(1, Arrays.asList(expectedDelete));

    Assert.assertEquals(Arrays.asList(put, expectedDelete),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getWalEdit();
    verify(metricsSource).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
  }

  @Test
  public void testDeleteCanCreateManyDeletes() {
    ArrayList<Cell> walEdit = new ArrayList<>();
    Cell delete = new KeyValue(rowKey, cf, null, 1000, KeyValue.Type.DeleteFamily);
    walEdit.add(delete);
    when(mockWalEntry.getWalEdit()).thenReturn(walEdit);

    // A single deleteFamily becomes 2 delete cells. This can happen when we call CBT and find out
    // there were 2 cells in the family before timestamp 100
    List<Cell> expectedDeletes = Arrays.asList(
        new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Delete),
        new KeyValue(rowKey, cf, qual, 10, KeyValue.Type.Delete)
    );
    incompatibleMutationAdapter.adaptedEntryMap.put(0, expectedDeletes);

    Assert.assertEquals(expectedDeletes,
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getWalEdit();
    verify(metricsSource).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
  }

  @Test
  public void testUnknownMutationTypesAreDropped() {
    ArrayList<Cell> walEdit = new ArrayList<>();
    Cell incompatible = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Maximum);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    walEdit.add(incompatible);
    walEdit.add(put);
    when(mockWalEntry.getWalEdit()).thenReturn(walEdit);

    Assert.assertEquals(Arrays.asList(put),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getWalEdit();
    verify(metricsSource).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }

  @Test
  public void testIncompatibleDeletesAreDropped() {
    ArrayList<Cell> walEdit = new ArrayList<>();
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    Cell incompatible = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.DeleteFamilyVersion);
    walEdit.add(put);
    walEdit.add(incompatible);
    when(mockWalEntry.getWalEdit()).thenReturn(walEdit);
    incompatibleMutationAdapter.incompatibleMutations.add(1);

    Assert.assertEquals(Arrays.asList(put),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(mockWalEntry).getWalEdit();
    verify(metricsSource).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }


}