/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Tests the {@link CreateBigtableMutationsFn} functionality. */
@RunWith(JUnit4.class)
public class CreateBigtableMutationsFnTest {

  private SnapshotConfig snapshotConfig;
  private SnapshotKey snapshotKey;
  private Result result;
  private DoFn.OutputReceiver<KV<String, Iterable<Mutation>>> receiver;

  @Before
  public void setUp() {
    snapshotConfig =
        SnapshotConfig.builder()
            .setProjectId("project")
            .setSourceLocation("source")
            .setSnapshotName("my-snapshot")
            .setTableName("my-table")
            .setRestoreLocation("restore")
            .setConfigurationDetails(Collections.emptyMap())
            .build();
    snapshotKey = SnapshotKey.create("my-snapshot", "my-table");
    receiver = mock(DoFn.OutputReceiver.class);
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} successfully converts a simple HBase
   * result to Bigtable mutations.
   */
  @Test
  public void testProcessElement() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell = new KeyValue(rowKey, "cf".getBytes(), "qual".getBytes(), "val".getBytes());

    Result localResult = Result.create(Collections.singletonList(cell));

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(100, false, 0L, false, 0, false, 0, false);

    fn.processElement(KV.of(snapshotKey, localResult), receiver);
    ArgumentCaptor<KV<String, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);
    verify(receiver, times(1)).output(captor.capture());
    KV<String, Iterable<Mutation>> kv = captor.getValue();
    assertEquals("my-table", kv.getKey());
    Iterator<Mutation> mutationIt = kv.getValue().iterator();
    assertEquals(true, mutationIt.hasNext());
    Mutation mutation = mutationIt.next();
    assertEquals("row-key", new String(mutation.getRow()));
    assertEquals(false, mutationIt.hasNext());
  }

  /** Tests that {@link CreateBigtableMutations#processElement} skips results with no cells. */
  @Test
  public void testProcessElement_emptyCells() throws Exception {
    Result localResult = Result.create(Collections.<Cell>emptyList());

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(100, false, 0L, false, 0, false, 0, false);

    fn.processElement(KV.of(snapshotKey, localResult), receiver);
    verify(receiver, times(0)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} filters out cells that exceed the
   * configured size threshold.
   */
  @Test
  public void testProcessElement_filterLargeCell() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    byte[] largeValue = new byte[1000];
    Cell cell = new KeyValue(rowKey, "cf".getBytes(), "qual".getBytes(), largeValue);

    result = Result.create(Collections.singletonList(cell));

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(
            100, false, 0L, true, 500, // filterLargeCells enabled, threshold 500
            false, 0, false);

    fn.processElement(KV.of(snapshotKey, result), receiver);

    verify(receiver, times(0)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} drops the row if the total size of
   * its cells exceeds the configured threshold.
   */
  @Test
  public void testProcessElement_filterLargeRow() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell = new KeyValue(rowKey, "cf".getBytes(), "qual".getBytes(), "val".getBytes());

    result = Result.create(Collections.singletonList(cell));

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(
            100, true, 10L, false, 0, false, 0, false // filterLargeRows enabled, threshold 10 bytes
            );

    fn.processElement(KV.of(snapshotKey, result), receiver);

    verify(receiver, times(0)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} drops the row if the row key length
   * exceeds the configured threshold.
   */
  @Test
  public void testProcessElement_filterLargeRowKey() throws Exception {
    byte[] rowKey = "row-key".getBytes(); // 7 bytes
    Cell cell = new KeyValue(rowKey, "cf".getBytes(), "qual".getBytes(), "val".getBytes());

    result = Result.create(Collections.singletonList(cell));

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(
            100,
            false,
            0L,
            false,
            0,
            true,
            5,
            false // filterLargeRowKeys enabled, threshold 5 bytes
            );

    fn.processElement(KV.of(snapshotKey, result), receiver);

    verify(receiver, times(0)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} splits mutations into multiple Put
   * requests if the number of cells exceeds the threshold.
   */
  @Test
  public void testProcessElement_splitMutations() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell1 = new KeyValue(rowKey, "cf".getBytes(), "qual1".getBytes(), "val1".getBytes());
    Cell cell2 = new KeyValue(rowKey, "cf".getBytes(), "qual2".getBytes(), "val2".getBytes());

    List<Cell> cells = new ArrayList<>();
    cells.add(cell1);
    cells.add(cell2);

    result = Result.create(cells);

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(
            1, // maxMutationsPerRequestThreshold = 1
            false, 0L, false, 0, false, 0, false);

    fn.processElement(KV.of(snapshotKey, result), receiver);

    ArgumentCaptor<KV<String, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);

    verify(receiver, times(1)).output(captor.capture());

    KV<String, Iterable<Mutation>> output = captor.getValue();
    assertNotNull(output);

    Iterator<Mutation> iterator = output.getValue().iterator();
    assertEquals(true, iterator.hasNext());
    Mutation mutation1 = iterator.next();
    assertEquals("row-key", new String(mutation1.getRow()));
    // Validate cells in mutation1
    List<Cell> cells1 = mutation1.getFamilyCellMap().values().iterator().next();
    assertEquals(1, cells1.size());
    Cell c1 = cells1.get(0);
    assertEquals(
        "qual1",
        new String(c1.getQualifierArray(), c1.getQualifierOffset(), c1.getQualifierLength()));

    assertEquals(true, iterator.hasNext());
    Mutation mutation2 = iterator.next();
    assertEquals("row-key", new String(mutation2.getRow()));
    // Validate cells in mutation2
    List<Cell> cells2 = mutation2.getFamilyCellMap().values().iterator().next();
    assertEquals(1, cells2.size());
    Cell c2 = cells2.get(0);
    assertEquals(
        "qual2",
        new String(c2.getQualifierArray(), c2.getQualifierOffset(), c2.getQualifierLength()));

    assertEquals(false, iterator.hasNext());
  }

  /**
   * Tests that {@link CreateBigtableMutationsFn#processElement} drops wide rows if filterWideRows
   * is enabled and cell count exceeds threshold.
   */
  @Test
  public void testProcessElement_filterWideRows_exceedsThreshold() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell1 = new KeyValue(rowKey, "cf".getBytes(), "qual1".getBytes(), "val1".getBytes());
    Cell cell2 = new KeyValue(rowKey, "cf".getBytes(), "qual2".getBytes(), "val2".getBytes());

    List<Cell> cells = new ArrayList<>();
    cells.add(cell1);
    cells.add(cell2);

    result = Result.create(cells);

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(
            1, // maxMutationsPerRequestThreshold = 1
            false, 0L, false, 0, false, 0, true); // filterWideRows enabled

    fn.processElement(KV.of(snapshotKey, result), receiver);

    verify(receiver, times(0)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutationsFn#processElement} does NOT drop wide rows if
   * filterWideRows is enabled but cell count is exactly at threshold.
   */
  @Test
  public void testProcessElement_filterWideRows_exactThreshold() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell1 = new KeyValue(rowKey, "cf".getBytes(), "qual1".getBytes(), "val1".getBytes());
    Cell cell2 = new KeyValue(rowKey, "cf".getBytes(), "qual2".getBytes(), "val2".getBytes());

    List<Cell> cells = new ArrayList<>();
    cells.add(cell1);
    cells.add(cell2);

    result = Result.create(cells);

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(
            2, // maxMutationsPerRequestThreshold = 2
            false, 0L, false, 0, false, 0, true); // filterWideRows enabled

    fn.processElement(KV.of(snapshotKey, result), receiver);

    verify(receiver, times(1)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutationsFn#processElement} does NOT drop the row if a large
   * cell is filtered out first, and the remaining cells are within the row size threshold.
   */
  @Test
  public void testProcessElement_largeCellFiltered_rowNotDropped() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell1 = new KeyValue(rowKey, "cf".getBytes(), "qual1".getBytes(), "val1".getBytes());
    byte[] largeValue = new byte[1000];
    Cell cell2 = new KeyValue(rowKey, "cf".getBytes(), "qual2".getBytes(), largeValue);

    List<Cell> cells = new ArrayList<>();
    cells.add(cell1);
    cells.add(cell2);

    result = Result.create(cells);

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(
            100, true,
            200L, // filterLargeRows enabled, threshold 200 bytes (cell1 fits, cell1+cell2 doesn't)
            true, 500, // filterLargeCells enabled, threshold 500 bytes (cell2 is filtered)
            false, 0, false);

    fn.processElement(KV.of(snapshotKey, result), receiver);

    ArgumentCaptor<KV<String, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);
    verify(receiver, times(1)).output(captor.capture());

    KV<String, Iterable<Mutation>> output = captor.getValue();
    Iterator<Mutation> iterator = output.getValue().iterator();
    Mutation mutation = iterator.next();
    List<Cell> outCells = mutation.getFamilyCellMap().values().iterator().next();
    assertEquals(1, outCells.size());
    assertEquals(
        "qual1",
        new String(
            outCells.get(0).getQualifierArray(),
            outCells.get(0).getQualifierOffset(),
            outCells.get(0).getQualifierLength()));
  }

  /**
   * Tests that {@link CreateBigtableMutationsFn#processElement} drops the row if total size exceeds
   * threshold, even if it would have been split due to cell count.
   */
  @Test
  public void testProcessElement_splitMutations_butRowTooLarge() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell1 = new KeyValue(rowKey, "cf".getBytes(), "qual1".getBytes(), "val1".getBytes());
    Cell cell2 = new KeyValue(rowKey, "cf".getBytes(), "qual2".getBytes(), "val2".getBytes());

    List<Cell> cells = new ArrayList<>();
    cells.add(cell1);
    cells.add(cell2);

    result = Result.create(cells);

    long cell1Size = CellUtil.estimatedSerializedSizeOf(cell1);

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(
            1, // maxMutationsPerRequestThreshold = 1 (forces split)
            true,
            cell1Size + 10, // filterLargeRows enabled, fails on cell2
            false,
            0,
            false,
            0,
            false);

    fn.processElement(KV.of(snapshotKey, result), receiver);

    verify(receiver, times(0)).output(Mockito.any()); // Should be dropped!
  }

  /**
   * Tests that {@link CreateBigtableMutationsFn#processElement} filters out multiple large cells in
   * the same row, and only logs once.
   */
  @Test
  public void testProcessElement_filterMultipleLargeCells() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    byte[] largeValue = new byte[1000];
    Cell cell1 = new KeyValue(rowKey, "cf".getBytes(), "qual1".getBytes(), largeValue);
    Cell cell2 = new KeyValue(rowKey, "cf".getBytes(), "qual2".getBytes(), largeValue);
    Cell cell3 =
        new KeyValue(rowKey, "cf".getBytes(), "qual3".getBytes(), "val".getBytes()); // Normal cell

    List<Cell> cells = new ArrayList<>();
    cells.add(cell1);
    cells.add(cell2);
    cells.add(cell3);

    result = Result.create(cells);

    CreateBigtableMutationsFn fn =
        new CreateBigtableMutationsFn(
            100, false, 0L, true, 500, // filterLargeCells enabled, threshold 500
            false, 0, false);

    fn.processElement(KV.of(snapshotKey, result), receiver);

    ArgumentCaptor<KV<String, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);
    verify(receiver, times(1)).output(captor.capture());

    KV<String, Iterable<Mutation>> output = captor.getValue();
    Iterator<Mutation> iterator = output.getValue().iterator();
    Mutation mutation = iterator.next();
    List<Cell> outCells = mutation.getFamilyCellMap().values().iterator().next();
    assertEquals(1, outCells.size()); // Only cell3 should remain!
    assertEquals(
        "qual3",
        new String(
            outCells.get(0).getQualifierArray(),
            outCells.get(0).getQualifierOffset(),
            outCells.get(0).getQualifierLength()));
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} increments droppedRows counter when
   * every cell in the row is filtered out due to exceeding the large cell threshold.
   */
  @Test
  public void testProcessElement_filterLargeCell_incrementsDroppedRows() throws Exception {
    MetricsContainer mockContainer = mock(MetricsContainer.class);
    org.apache.beam.sdk.metrics.Counter mockCounter =
        mock(org.apache.beam.sdk.metrics.Counter.class);
    Mockito.when(mockContainer.getCounter(Mockito.any(MetricName.class))).thenReturn(mockCounter);
    MetricsEnvironment.setCurrentContainer(mockContainer);

    try {
      byte[] rowKey = "row-key".getBytes();
      byte[] largeValue = new byte[1000];
      Cell cell = new KeyValue(rowKey, "cf".getBytes(), "qual".getBytes(), largeValue);

      result = Result.create(Collections.singletonList(cell));

      CreateBigtableMutationsFn fn =
          new CreateBigtableMutationsFn(
              100, false, 0L, true, 500, // filterLargeCells enabled, threshold 500
              false, 0, false);

      fn.processElement(KV.of(snapshotKey, result), receiver);

      // Verify that both droppedCells and droppedRows were requested and incremented
      ArgumentCaptor<MetricName> captor = ArgumentCaptor.forClass(MetricName.class);
      verify(mockContainer, times(2)).getCounter(captor.capture());

      List<MetricName> requestedMetrics = captor.getAllValues();
      assertEquals(2, requestedMetrics.size());

      assertEquals("droppedCells", requestedMetrics.get(0).getName());
      assertEquals("droppedRows", requestedMetrics.get(1).getName());

      verify(mockCounter, times(2)).inc(1L);
    } finally {
      MetricsEnvironment.setCurrentContainer(null);
    }
  }
}
