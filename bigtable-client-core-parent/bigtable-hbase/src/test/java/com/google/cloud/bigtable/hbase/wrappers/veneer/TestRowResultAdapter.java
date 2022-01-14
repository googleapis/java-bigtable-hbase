/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestRowResultAdapter {

  private static final ByteString ROW_KEY = ByteString.copyFromUtf8("one");
  private static final String COL_FAMILY = "cf";
  private static final ByteString QUAL_ONE = ByteString.copyFromUtf8("q1");
  private static final ByteString QUAL_TWO = ByteString.copyFromUtf8("q2");
  private static final List<String> LABELS = ImmutableList.of("label-a", "label-b");
  private static final ByteString VALUE = ByteString.copyFromUtf8("value");

  private RowResultAdapter underTest = new RowResultAdapter();

  @Test
  public void testWithSingleCellRow() {
    RowAdapter.RowBuilder<Result> rowBuilder = underTest.createRowBuilder();

    rowBuilder.startRow(ROW_KEY);
    rowBuilder.startCell(
        COL_FAMILY, QUAL_ONE, 10000L, Collections.<String>emptyList(), VALUE.size());
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();
    rowBuilder.startCell(COL_FAMILY, QUAL_TWO, 20000L, LABELS, -1);
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    Result expected =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    ROW_KEY.toByteArray(),
                    COL_FAMILY.getBytes(),
                    QUAL_ONE.toByteArray(),
                    10L,
                    VALUE.toByteArray()),
                new RowCell(
                    ROW_KEY.toByteArray(),
                    COL_FAMILY.getBytes(),
                    QUAL_TWO.toByteArray(),
                    20L,
                    VALUE.toByteArray(),
                    LABELS)));
    assertResult(expected, rowBuilder.finishRow());
    assertEquals(ROW_KEY, underTest.getKey(expected));
  }

  @Test
  public void testWhenSplitCell() {
    ByteString valuePart1 = ByteString.copyFromUtf8("part-1");
    ByteString valuePart2 = ByteString.copyFromUtf8("part-2");
    ByteString valuePart3 = ByteString.copyFromUtf8("part-3");

    RowAdapter.RowBuilder<Result> rowBuilder = underTest.createRowBuilder();
    rowBuilder.startRow(ROW_KEY);
    rowBuilder.startCell(COL_FAMILY, QUAL_ONE, 10000L, Collections.<String>emptyList(), 18);
    rowBuilder.cellValue(valuePart1);
    rowBuilder.cellValue(valuePart2);
    rowBuilder.cellValue(valuePart3);
    rowBuilder.finishCell();

    Result actualResult = rowBuilder.finishRow();
    assertArrayEquals(
        valuePart1.concat(valuePart2).concat(valuePart3).toByteArray(),
        actualResult.getValue(COL_FAMILY.getBytes(), QUAL_ONE.toByteArray()));
  }

  @Test
  public void testWhenRowKeyIsNotSet() {
    RowAdapter.RowBuilder<Result> rowBuilder = underTest.createRowBuilder();
    rowBuilder.startCell(COL_FAMILY, QUAL_ONE, 30000L, LABELS, VALUE.size());
    rowBuilder.cellValue(VALUE);
    try {
      rowBuilder.finishCell();
      Assert.fail("should not accept null rowKey");
    } catch (NullPointerException expected) {
      assertEquals("row key cannot be null", expected.getMessage());
    }
  }

  @Test
  public void testOnlyValueIsDifferent() {
    ByteString valuePart1 = ByteString.copyFromUtf8("part-1");
    RowAdapter.RowBuilder<Result> rowBuilder = underTest.createRowBuilder();
    rowBuilder.startRow(ROW_KEY);
    rowBuilder.startCell(COL_FAMILY, QUAL_ONE, 30000L, LABELS, VALUE.size());
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    // started cell with same qualifier but different value
    rowBuilder.startCell(COL_FAMILY, QUAL_ONE, 30000L, LABELS, valuePart1.size());
    rowBuilder.cellValue(valuePart1);
    rowBuilder.finishCell();

    Result actual = rowBuilder.finishRow();
    assertEquals(2, actual.size());

    Result expected =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    ROW_KEY.toByteArray(),
                    COL_FAMILY.getBytes(),
                    QUAL_ONE.toByteArray(),
                    30L,
                    VALUE.toByteArray(),
                    LABELS),
                new RowCell(
                    ROW_KEY.toByteArray(),
                    COL_FAMILY.getBytes(),
                    QUAL_ONE.toByteArray(),
                    30L,
                    valuePart1.toByteArray(),
                    LABELS)));
    assertResult(expected, actual);
  }

  @Test
  public void testDeduplicationLogic() {
    List<String> EMPTY_LABEL = ImmutableList.of();
    ByteString valueForNewQual = ByteString.copyFromUtf8("value for new qualifier");
    ByteString valueForCellWithoutLabel = ByteString.copyFromUtf8("value for Cell without label");
    ByteString valueForCellWithLabel = ByteString.copyFromUtf8("value for cell with labels");
    RowAdapter.RowBuilder<Result> rowBuilder = underTest.createRowBuilder();
    rowBuilder.startRow(ROW_KEY);

    // new qualifier
    rowBuilder.startCell(COL_FAMILY, QUAL_ONE, 20000L, EMPTY_LABEL, valueForNewQual.size());
    rowBuilder.cellValue(valueForNewQual);
    rowBuilder.finishCell();

    // same qualifier, same timestamp, without label
    rowBuilder.startCell(COL_FAMILY, QUAL_ONE, 20000L, EMPTY_LABEL, -1);
    rowBuilder.cellValue(valueForCellWithoutLabel);
    rowBuilder.finishCell();

    // same qualifier, same timestamp, with label
    rowBuilder.startCell(COL_FAMILY, QUAL_ONE, 20000L, LABELS, -1);
    rowBuilder.cellValue(valueForCellWithLabel);
    rowBuilder.finishCell();

    // same qualifier, different timestamp
    rowBuilder.startCell(COL_FAMILY, QUAL_ONE, 30000L, EMPTY_LABEL, VALUE.size());
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    Result actual = rowBuilder.finishRow();
    assertEquals(3, actual.size());

    Result expected =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    ROW_KEY.toByteArray(),
                    COL_FAMILY.getBytes(),
                    QUAL_ONE.toByteArray(),
                    20L,
                    valueForNewQual.toByteArray(),
                    EMPTY_LABEL),
                new RowCell(
                    ROW_KEY.toByteArray(),
                    COL_FAMILY.getBytes(),
                    QUAL_ONE.toByteArray(),
                    20L,
                    valueForCellWithLabel.toByteArray(),
                    LABELS),
                new RowCell(
                    ROW_KEY.toByteArray(),
                    COL_FAMILY.getBytes(),
                    QUAL_ONE.toByteArray(),
                    30L,
                    VALUE.toByteArray(),
                    EMPTY_LABEL)));
    assertResult(expected, actual);
  }

  @Test
  public void testFamilyOrdering() {
    RowAdapter.RowBuilder<Result> rowBuilder = underTest.createRowBuilder();

    rowBuilder.startRow(ROW_KEY);
    rowBuilder.startCell("cc", QUAL_ONE, 20000L, LABELS, -1);
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    rowBuilder.startCell("bb", QUAL_TWO, 40000L, LABELS, VALUE.size() * 2);
    rowBuilder.cellValue(VALUE);
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    rowBuilder.startCell("aa", QUAL_ONE, 20000L, LABELS, VALUE.size());
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    rowBuilder.startCell("zz", QUAL_ONE, 80000L, LABELS, VALUE.size());
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    rowBuilder.startCell("b", QUAL_ONE, 10000L, LABELS, VALUE.size());
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    Result actualResult = rowBuilder.finishRow();

    List<String> colFamilyInActualOrder = new ArrayList<>(5);
    for (Cell cell : actualResult.listCells()) {
      colFamilyInActualOrder.add(Bytes.toString(CellUtil.cloneFamily(cell)));
    }
    assertEquals(ImmutableList.of("aa", "b", "bb", "cc", "zz"), colFamilyInActualOrder);
  }

  @Test
  public void testWithMarkerRow() {
    Result markerRow = new RowResultAdapter.RowResultBuilder().createScanMarkerRow(ROW_KEY);
    assertTrue(underTest.isScanMarkerRow(markerRow));
    assertEquals(ROW_KEY, underTest.getKey(markerRow));

    Result resultWithOneCell =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    ROW_KEY.toByteArray(),
                    COL_FAMILY.getBytes(),
                    QUAL_ONE.toByteArray(),
                    10L,
                    VALUE.toByteArray())));
    assertFalse(underTest.isScanMarkerRow(resultWithOneCell));
  }

  @Test
  public void testReset() {
    RowAdapter.RowBuilder<Result> rowBuilder = underTest.createRowBuilder();

    rowBuilder.startRow(ROW_KEY);
    rowBuilder.startCell(
        COL_FAMILY, QUAL_ONE, 10000L, Collections.<String>emptyList(), VALUE.size());
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    rowBuilder.reset();
    ByteString anotherKey = ByteString.copyFromUtf8("another-rowKey");
    rowBuilder.startRow(anotherKey);
    rowBuilder.startCell(COL_FAMILY, QUAL_TWO, 40000L, LABELS, VALUE.size());
    rowBuilder.cellValue(VALUE);
    rowBuilder.finishCell();

    Result actual = rowBuilder.finishRow();
    assertResult(
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    anotherKey.toByteArray(),
                    COL_FAMILY.getBytes(),
                    QUAL_TWO.toByteArray(),
                    40L,
                    VALUE.toByteArray()))),
        actual);
  }

  private void assertResult(Result expected, Result actual) {
    try {
      if (actual.isEmpty()) {
        Assert.fail("Result does not have any rows");
      }
      Result.compareResults(expected, actual);
    } catch (Throwable throwable) {
      throw new AssertionError("Result did not match", throwable);
    }
  }
}
