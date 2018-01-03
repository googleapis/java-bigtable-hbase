/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.beam.sequencefiles;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.google.cloud.bigtable.beam.sequencefiles.testing.HBaseCellUtils;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

/**
 * Unit tests for {@link HBaseResultToMutationFn}.
 */
@RunWith(JUnit4.class)
public class HBaseResultToMutationFnTest {
  private static final byte[] ROW_KEY = Bytes.toBytes("row_key");
  private static final byte[] ROW_KEY_2 = Bytes.toBytes("row_key_2");
  private static final byte[] CF = Bytes.toBytes("column_family");
  private static final byte[] CF2 = Bytes.toBytes("column_family_2");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] QUALIFIER_2 = Bytes.toBytes("qualifier_2");
  private static final byte[] VALUE = Bytes.toBytes("value");

  HBaseResultToMutationFn doFn;

  @Mock private Logger logger;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    doFn = new HBaseResultToMutationFn();
    HBaseResultToMutationFn.setLogger(logger);
  }

  /**
   * Verifies that when {@link HBaseResultToMutationFn} is called on a {@link Result}
   * with a single {@link Cell}, that cell is passed to the output, and
   * the logger is not invoked.
   */
  @Test
  public void testResultToMutation() throws Exception {
    DoFnTester<KV<ImmutableBytesWritable, Result>, Mutation> doFnTester = DoFnTester.of(doFn);
    Cell[] expected = new Cell[] { new KeyValue(ROW_KEY, CF, QUALIFIER, 1L, VALUE)};
    List<Mutation> outputs = doFnTester.processBundle(
        KV.of(new ImmutableBytesWritable(ROW_KEY), Result.create(expected)));
    assertEquals("Cells", Sets.newHashSet(expected),
        Sets.newHashSet(Iterables.getOnlyElement(outputs).getFamilyCellMap().get(CF)));
    verifyZeroInteractions(logger);
  }

  /**
   * Verifies that malformed {@link Result}s with null cell-array cause one warning in the log.
   */
  @Test
  public void testResultToMutation_nullCellsWarnOnce() throws Exception {
    DoFnTester<KV<ImmutableBytesWritable, Result>, Mutation> doFnTester = DoFnTester.of(doFn);
    Result resultWithNullCells = new Result();
    assertNull(resultWithNullCells.rawCells());
    List<Mutation> outputs = doFnTester.processBundle(
        KV.of(new ImmutableBytesWritable(ROW_KEY), resultWithNullCells),
        KV.of(new ImmutableBytesWritable(ROW_KEY_2), resultWithNullCells));
    assertTrue(outputs == null || outputs.isEmpty());
    verify(logger, times(1)).warn(anyString());
    verifyNoMoreInteractions(logger);
  }

  /**
   * Verifies that malformed {@link Result}s with empty cell-array cause one warning in the log.
   */
  @Test
  public void testResultToMutation_emptyCellsWarnOnce() throws Exception {
    DoFnTester<KV<ImmutableBytesWritable, Result>, Mutation> doFnTester = DoFnTester.of(doFn);
    Result resultWithEmptyCells = Result.create(Collections.<Cell> emptyList());
    List<Mutation> outputs = doFnTester.processBundle(
        KV.of(new ImmutableBytesWritable(ROW_KEY), resultWithEmptyCells),
        KV.of(new ImmutableBytesWritable(ROW_KEY_2), resultWithEmptyCells));
    assertTrue(outputs == null || outputs.isEmpty());
    verify(logger, times(1)).warn(anyString());
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void testResultToMutation_withDeleteMarkers() throws Exception {
    Cell[] inputCells = new Cell[] {
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER, 1000),
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER, 1010),
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER, 1020),
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER, 1030),
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER, 1040),
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER, 1050), // 5th
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER, 990),
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER, 980),
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER, 970),
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER_2, 100),
        HBaseCellUtils.createDataCell(ROW_KEY, CF, QUALIFIER_2, 1000), // 10th
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER, 990),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER_2, 990),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER, 1000),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER_2, 1000),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER, 1010), // 15th
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER_2, 1010),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER, 1020),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER_2, 1020),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER, 1030),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER_2, 1030), // 20th
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER, 1040),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER_2, 1040),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER, 1050),
        HBaseCellUtils.createDataCell(ROW_KEY, CF2, QUALIFIER_2, 1050),

        // No effect -- nonexisting timestamp 1045:
        HBaseCellUtils.deleteMarkerForOneCellWithExactTimestamp(
            ROW_KEY, CF, QUALIFIER, 1045),
        // Removes one cell: <CF, QUALIFIER, 1040> (4th with 0-based indexing).
        HBaseCellUtils.deleteMarkerForOneCellWithExactTimestamp(
            ROW_KEY, CF, QUALIFIER, 1040),
        // Removes all <CF, QUALIFIER> cells whose ts <= 1000:
        // <CF, QUALIFIER, 1000> (0th), <CF, QUALIFIER, 990> (6th), <CF, QUALIFIER, 980> (7th)
        // <CF, QUALIFIER, 970> (8th)
        HBaseCellUtils.deleteMarkerForCellsWithLowerOrEqualTimestamp(
            ROW_KEY, CF, QUALIFIER, 1000),
        // No effect -- nonexisting timestamp: 1045
        HBaseCellUtils.deleteMarkerForAllCellsInFamilyWithExactTimestamp(
            ROW_KEY, CF, 1045),
        // Removes two cells in CF2 with ts == 1040:
        // <CF2, QUALIFIER, 1040> (21st), <CF2, QUALIFIER_2, 1040> (22nd)
        HBaseCellUtils.deleteMarkerForAllCellsInFamilyWithExactTimestamp(
            ROW_KEY, CF2, 1040),
        // All cells in CF2 with ts <= 1000:
        // <CF2, QUALIFIER, 990> (11th), <CF2, QUALIFIER_2, 990> (12th)
        // <CF2, QUALIFIER, 1000> (13th), <CF2, QUALIFIER_2, 1000> (14th)
        HBaseCellUtils.deleteMarkerForAllCellsInFamilyWithLowerOrEqualTimestamp(
            ROW_KEY, CF2, 1000),
    };
    // Cells in column family 'CF' that are expected to pass:
    Cell[] expectedInCF = new Cell[] {
        inputCells[1], // <CF, QUALIFIER_2, 1010>
        inputCells[2], // <CF, QUALIFIER_2, 1020>
        inputCells[3], // <CF, QUALIFIER_2, 1030>
        inputCells[5], // <CF, QUALIFIER_2, 1050>
        inputCells[9],  // <CF, QUALIFIER_2, 100>
        inputCells[10]  // <CF, QUALIFIER_2, 1000>
    };
    // Cells in column family 'CF2' that are expected to pass:
    Cell[] expectedInCF2 = new Cell[] {
        inputCells[15], inputCells[16], // <CF2, QUALIFIER, 1010>,  <CF2, QUALIFIER_2, 1010>,
        inputCells[17], inputCells[18], // <CF2, QUALIFIER, 1020>,  <CF2, QUALIFIER_2, 1020>,
        inputCells[19], inputCells[20], // <CF2, QUALIFIER, 1030>,  <CF2, QUALIFIER_2, 1030>,
        inputCells[23], inputCells[24], // <CF2, QUALIFIER, 1050>,  <CF2, QUALIFIER_2, 1050>,
    };
    DoFnTester<KV<ImmutableBytesWritable, Result>, Mutation> doFnTester = DoFnTester.of(doFn);
    Result result = Result.create(inputCells);
    List<Mutation> outputs =
        doFnTester.processBundle(KV.of(new ImmutableBytesWritable(ROW_KEY), result));
    assertEquals(Sets.newHashSet(expectedInCF),
        Sets.newHashSet(Iterables.getOnlyElement(outputs).getFamilyCellMap().get(CF)));
    assertEquals(Sets.newHashSet(expectedInCF2),
        Sets.newHashSet(Iterables.getOnlyElement(outputs).getFamilyCellMap().get(CF2)));
  }
}
