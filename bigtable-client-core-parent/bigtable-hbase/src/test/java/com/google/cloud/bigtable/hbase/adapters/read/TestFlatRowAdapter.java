/*
 * Copyright 2016 Google LLC
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
package com.google.cloud.bigtable.hbase.adapters.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for the {@link RowAdapter}. */
@RunWith(JUnit4.class)
public class TestFlatRowAdapter {

  private FlatRowAdapter instance = new FlatRowAdapter();

  @Test
  public void adaptResponse_null() {
    assertNull(instance.adaptResponse(null).rawCells());
  }

  @Test
  public void adaptResponse_emptyRow() {
    FlatRow row = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("key")).build();
    Result result = instance.adaptResponse(row);
    assertEquals(0, result.rawCells().length);

    // The rowKey is defined based on the cells, and in this case there are no cells, so there isn't
    // a key.
    assertNull(transformToFlatRow(result));
  }

  @Test
  public void adaptResponse_oneRow() {
    String family1 = "family1";
    String family2 = "family2";
    byte[] qualifier1 = "qualifier1".getBytes();
    byte[] qualifier2 = "qualifier2".getBytes();
    byte[] value1 = "value1".getBytes();
    byte[] value2 = "value2".getBytes();
    byte[] value3 = "value3".getBytes();
    byte[] value4 = "value4".getBytes();
    byte[] value5 = "value5".getBytes();
    byte[] value6 = "value6".getBytes();

    FlatRow row =
        FlatRow.newBuilder()
            .withRowKey(ByteString.copyFromUtf8("key"))
            // First cell.
            .addCell(family1, ByteString.copyFrom(qualifier1), 54321L, ByteString.copyFrom(value1))
            // Same family, same column, but different timestamps.
            .addCell(family1, ByteString.copyFrom(qualifier1), 12345L, ByteString.copyFrom(value2))
            // With label
            .addCell(
                family1,
                ByteString.copyFrom(qualifier1),
                12345L,
                ByteString.copyFrom(value6),
                Collections.singletonList("label"))
            // Same family, same timestamp, but different column.
            .addCell(family1, ByteString.copyFrom(qualifier2), 54321L, ByteString.copyFrom(value3))
            // Same column, same timestamp, but different family.
            .addCell(family2, ByteString.copyFrom(qualifier1), 54321L, ByteString.copyFrom(value4))
            // Same timestamp, but different family qualifier2 column.
            .addCell(family2, ByteString.copyFrom(qualifier2), 54321L, ByteString.copyFrom(value5))
            .build();

    Result result = instance.adaptResponse(row);
    assertEquals(6, result.rawCells().length);

    List<org.apache.hadoop.hbase.Cell> cells1 =
        result.getColumnCells(family1.getBytes(), qualifier1);
    assertEquals(3, cells1.size());
    assertEquals(Bytes.toString(value1), Bytes.toString(CellUtil.cloneValue(cells1.get(0))));
    assertEquals(Bytes.toString(value2), Bytes.toString(CellUtil.cloneValue(cells1.get(1))));
    assertEquals(Bytes.toString(value6), Bytes.toString(CellUtil.cloneValue(cells1.get(2))));
    List<org.apache.hadoop.hbase.Cell> cells2 =
        result.getColumnCells(family1.getBytes(), qualifier2);
    assertEquals(1, cells2.size());
    assertEquals(Bytes.toString(value3), Bytes.toString(CellUtil.cloneValue(cells2.get(0))));

    List<org.apache.hadoop.hbase.Cell> cells3 =
        result.getColumnCells(family2.getBytes(), qualifier1);
    assertEquals(1, cells3.size());
    assertEquals(Bytes.toString(value4), Bytes.toString(CellUtil.cloneValue(cells3.get(0))));

    List<org.apache.hadoop.hbase.Cell> cells4 =
        result.getColumnCells(family2.getBytes(), qualifier2);
    assertEquals(1, cells4.size());
    assertEquals(Bytes.toString(value5), Bytes.toString(CellUtil.cloneValue(cells4.get(0))));

    // The duplicate row and label cells have been removed. The timestamp micros get converted to
    // millisecond accuracy.
    FlatRow expected =
        FlatRow.newBuilder()
            .withRowKey(ByteString.copyFromUtf8("key"))
            // First cell.
            .addCell(family1, ByteString.copyFrom(qualifier1), 54000L, ByteString.copyFrom(value1))
            // Same family, same column, but different timestamps.
            .addCell(family1, ByteString.copyFrom(qualifier1), 12000L, ByteString.copyFrom(value2))
            // With label
            .addCell(
                family1,
                ByteString.copyFrom(qualifier1),
                12000L,
                ByteString.copyFrom(value6),
                Collections.singletonList("label"))
            // Same family, same timestamp, but different column.
            .addCell(family1, ByteString.copyFrom(qualifier2), 54000L, ByteString.copyFrom(value3))
            // Same column, same timestamp, but different family.
            .addCell(family2, ByteString.copyFrom(qualifier1), 54000L, ByteString.copyFrom(value4))
            // Same timestamp, but different family and column.
            .addCell(family2, ByteString.copyFrom(qualifier2), 54000L, ByteString.copyFrom(value5))
            .build();
    assertEquals(expected, transformToFlatRow(result));
  }

  @Test
  public void adaptToRow_oneRow() {
    Cell inputKeyValue =
        CellUtil.createCell(
            "key".getBytes(),
            "family".getBytes(),
            "qualifier".getBytes(),
            1200,
            Type.Put.getCode(),
            "value".getBytes());
    Result inputResult = Result.create(new Cell[] {inputKeyValue});

    FlatRow outputRow = transformToFlatRow(inputResult);
    assertEquals("output doesn't have the same number of cells", 1, outputRow.getCells().size());
    FlatRow.Cell outputCell = outputRow.getCells().get(0);

    assertEquals("key", outputRow.getRowKey().toStringUtf8());
    assertEquals("family", outputCell.getFamily());
    assertEquals("qualifier", outputCell.getQualifier().toStringUtf8());
    // bigtable has a higher resolution
    assertEquals(1200 * 1000, outputCell.getTimestamp());
    assertEquals("value", outputCell.getValue().toStringUtf8());
  }

  @Test
  public void resultRoundTrip() {
    Cell inputKeyValue =
        CellUtil.createCell(
            "key".getBytes(),
            "family".getBytes(),
            "qualifier".getBytes(),
            1200,
            Type.Put.getCode(),
            "value".getBytes());
    Result inputResult = Result.create(new Cell[] {inputKeyValue});

    FlatRow intermediateRow = transformToFlatRow(inputResult);

    Result outputRow = instance.adaptResponse(intermediateRow);
    Cell outputCell = outputRow.listCells().get(0);

    assertTrue(CellComparator.equals(inputKeyValue, outputCell));
  }

  /**
   * Convert a {@link org.apache.hadoop.hbase.client.Result} to a {@link FlatRow}.
   *
   * @param result a {@link org.apache.hadoop.hbase.client.Result} object.
   * @return a {@link FlatRow} object.
   */
  private static FlatRow transformToFlatRow(Result result) {
    // Result.getRow() is derived from its cells.  If the cells are empty, the row will be null.
    if (result.getRow() == null) {
      return null;
    }

    FlatRow.Builder rowBuilder =
        FlatRow.newBuilder().withRowKey(ByteStringer.wrap(result.getRow()));

    final Cell[] rawCells = result.rawCells();
    if (rawCells != null && rawCells.length > 0) {
      for (Cell rawCell : rawCells) {
        FlatRow.Cell.Builder flatRowCell = FlatRow.Cell.newBuilder();

        flatRowCell
            .withFamily(
                Bytes.toString(
                    rawCell.getFamilyArray(), rawCell.getFamilyOffset(), rawCell.getFamilyLength()))
            .withQualifier(
                ByteStringer.wrap(
                    rawCell.getQualifierArray(),
                    rawCell.getQualifierOffset(),
                    rawCell.getQualifierLength()))
            .withTimestamp(TimestampConverter.hbase2bigtable(rawCell.getTimestamp()))
            .withValue(
                ByteStringer.wrap(
                    rawCell.getValueArray(), rawCell.getValueOffset(), rawCell.getValueLength()));

        if (rawCell instanceof RowCell) {
          flatRowCell.withLabels(((RowCell) rawCell).getLabels());
        }
        rowBuilder.addCell(flatRowCell.build());
      }
    }

    return rowBuilder.build();
  }
}
