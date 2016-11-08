/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.protobuf.ByteString;

/**
 * Unit tests for the {@link RowAdapter}.
 */
@RunWith(JUnit4.class)
public class TestFlatRowAdapter {
  
  private FlatRowAdapter instance = new FlatRowAdapter();

  @Test
  public void adaptResponse_null() {
    assertNull(instance.adaptResponse(null).rawCells());
  }

  @Test
  public void adaptResponse_emptyRow() {
    FlatRow row = FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8("key"))
        .build();
    Result result = instance.adaptResponse(row);
    assertEquals(0, result.rawCells().length);

    // The rowKey is defined based on the cells, and in this case there are no cells, so there isn't
    // a key.
    assertEquals(FlatRow.newBuilder().build(), instance.adaptToRow(result));
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
 
    FlatRow row = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("key"))
        // First cell.
        .addCell(family1, ByteString.copyFrom(qualifier1), 54321L, ByteString.copyFrom(value1))
        // Duplicate cell.
        .addCell(family1, ByteString.copyFrom(qualifier1), 54321L, ByteString.copyFrom(value1))
        // Same family, same column, but different timestamps.
        .addCell(family1, ByteString.copyFrom(qualifier1), 12345L, ByteString.copyFrom(value2))
        // With label
        .addCell(family1, ByteString.copyFrom(qualifier1), 12345L, ByteString.copyFrom(value2),
          Arrays.asList("label"))
        // Same family, same timestamp, but different column.
        .addCell(family1, ByteString.copyFrom(qualifier2), 54321L, ByteString.copyFrom(value3))
        // Same column, same timestamp, but different family.
        .addCell(family2, ByteString.copyFrom(qualifier1), 54321L, ByteString.copyFrom(value4))
        // Same timestamp, but different family qualifier2 column.
        .addCell(family2, ByteString.copyFrom(qualifier2), 54321L, ByteString.copyFrom(value5))
        .build();

    Result result = instance.adaptResponse(row);
    assertEquals(5, result.rawCells().length);

    List<org.apache.hadoop.hbase.Cell> cells1 =
        result.getColumnCells(family1.getBytes(), qualifier1);
    assertEquals(2, cells1.size());
    assertEquals(Bytes.toString(value1), Bytes.toString(CellUtil.cloneValue(cells1.get(0))));
    assertEquals(Bytes.toString(value2), Bytes.toString(CellUtil.cloneValue(cells1.get(1))));

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
    FlatRow expected = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("key"))
        // First cell.
        .addCell(family1, ByteString.copyFrom(qualifier1), 54000L, ByteString.copyFrom(value1))
        // Same family, same column, but different timestamps.
        .addCell(family1, ByteString.copyFrom(qualifier1), 12000L, ByteString.copyFrom(value2))
        // Same family, same timestamp, but different column.
        .addCell(family1, ByteString.copyFrom(qualifier2), 54000L, ByteString.copyFrom(value3))
        // Same column, same timestamp, but different family.
        .addCell(family2, ByteString.copyFrom(qualifier1), 54000L, ByteString.copyFrom(value4))
        // Same timestamp, but different family and column.
        .addCell(family2, ByteString.copyFrom(qualifier2), 54000L, ByteString.copyFrom(value5))
        .build();
    assertEquals(expected, instance.adaptToRow(result));
  }
}
