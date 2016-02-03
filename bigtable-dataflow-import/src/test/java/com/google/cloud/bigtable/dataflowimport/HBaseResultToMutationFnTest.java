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
package com.google.cloud.bigtable.dataflowimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

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

import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;

/**
 * Unit tests for {@link HBaseResultToMutationFn}.
 */
@RunWith(JUnit4.class)
public class HBaseResultToMutationFnTest {
  private static final byte[] ROW_KEY = Bytes.toBytes("row_key");
  private static final byte[] ROW_KEY_2 = Bytes.toBytes("row_key_2");
  private static final byte[] CF = Bytes.toBytes("column_family");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
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
    List<Mutation> outputs = doFnTester.processBatch(
        KV.of(new ImmutableBytesWritable(ROW_KEY), Result.create(expected)));
    verifyMutationCells(expected, Iterables.getOnlyElement(outputs));
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
    List<Mutation> outputs = doFnTester.processBatch(
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
    Result resultWithEmptyCells = Result.create(Collections.EMPTY_LIST);
    List<Mutation> outputs = doFnTester.processBatch(
        KV.of(new ImmutableBytesWritable(ROW_KEY), resultWithEmptyCells),
        KV.of(new ImmutableBytesWritable(ROW_KEY_2), resultWithEmptyCells));
    assertTrue(outputs == null || outputs.isEmpty());
    verify(logger, times(1)).warn(anyString());
    verifyNoMoreInteractions(logger);
  }

  private void verifyMutationCells(Cell[] expected, Mutation mutation) throws Exception {
    NavigableMap<byte[], List<Cell>> map = mutation.getFamilyCellMap();
    assertEquals("Cells", Sets.newHashSet(expected), Sets.newHashSet(map.get(CF)));
  }
}
