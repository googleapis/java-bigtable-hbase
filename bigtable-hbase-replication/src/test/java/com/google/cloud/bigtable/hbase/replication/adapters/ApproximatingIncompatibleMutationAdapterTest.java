package com.google.cloud.bigtable.hbase.replication.adapters;

import static com.google.cloud.bigtable.hbase.replication.adapters.ApproximatingIncompatibleMutationAdapter.DELETE_FAMILY_WRITE_THRESHOLD_KEY;
import static org.apache.hadoop.hbase.HConstants.LATEST_TIMESTAMP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
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
public class ApproximatingIncompatibleMutationAdapterTest {

  private static final byte[] rowKey = "rowKey".getBytes(StandardCharsets.UTF_8);
  private static final byte[] cf = "family".getBytes(StandardCharsets.UTF_8);
  private static final byte[] qual = "qual".getBytes(StandardCharsets.UTF_8);
  private static final byte[] val = "value".getBytes(StandardCharsets.UTF_8);

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  Configuration conf;

  @Mock
  Table table;

  @Mock
  MetricsSource metricsSource;

  @Mock
  WAL.Entry mockWalEntry;

  WALKey walKey=  new WALKey(new byte[0], TableName.valueOf("test-table"), 1005);
  ApproximatingIncompatibleMutationAdapter incompatibleMutationAdapter;

  @Before
  public void setUp() throws Exception {
    when(conf.getInt(anyString(), anyInt())).thenReturn(10);
    when(mockWalEntry.getKey()).thenReturn(walKey);
    // Expectations on Conf should be set before this point.
    incompatibleMutationAdapter = new ApproximatingIncompatibleMutationAdapter(conf, metricsSource, table);
  }

  @After
  public void tearDown() throws Exception {
    verify(mockWalEntry, atLeast(2)).getEdit();
    verify(mockWalEntry, atLeastOnce()).getKey();
    verify(conf, atLeastOnce()).getInt(eq(DELETE_FAMILY_WRITE_THRESHOLD_KEY), anyInt());
    verifyNoInteractions(table);
    reset(mockWalEntry, conf, table, metricsSource);
  }

 @Test
  public void testDeletesAreAdapted() {
    WALEdit walEdit = new WALEdit();
    Cell delete = new KeyValue(rowKey, cf, null, 1000, KeyValue.Type.DeleteFamily);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    walEdit.add(delete);
    walEdit.add(put);
    when(mockWalEntry.getEdit()).thenReturn(walEdit);
    Cell expectedDelete = new KeyValue(rowKey, cf, null, LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily);

    Assert.assertEquals(Arrays.asList(expectedDelete, put),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(metricsSource).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsSource, times(1)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
  }

  @Test
  public void testIncompatibleDeletesAreDropped() {
    WALEdit walEdit = new WALEdit();
    Cell deleteFamilyVersion = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.DeleteFamilyVersion);
    // Cell timestamp > WAL time, should be rejected.
    Cell deleteFamilyAfterWAL = new KeyValue(rowKey, cf, qual, 2000, KeyValue.Type.DeleteFamilyVersion);
    // The WAL entry is written at 1000 with write threshold of 10, anything before 990 is rejected
    Cell deleteFamilyBeforeThreshold =
        new KeyValue(rowKey, cf, qual, 500, KeyValue.Type.DeleteFamily);
    Cell put = new KeyValue(rowKey, cf, qual, 0, KeyValue.Type.Put, val);
    walEdit.add(deleteFamilyVersion);
    walEdit.add(deleteFamilyAfterWAL);
    walEdit.add(deleteFamilyBeforeThreshold);
    walEdit.add(put);
    when(mockWalEntry.getKey()).thenReturn(walKey);
    when(mockWalEntry.getEdit()).thenReturn(walEdit);

    Assert.assertEquals(Arrays.asList(put),
        incompatibleMutationAdapter.adaptIncompatibleMutations(mockWalEntry));

    verify(metricsSource).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 0);
    verify(metricsSource, times(3)).incCounters(
        IncompatibleMutationAdapter.INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
    verify(metricsSource, times(3)).incCounters(
        IncompatibleMutationAdapter.DROPPED_INCOMPATIBLE_MUTATION_METRIC_KEY, 1);
  }
}