/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the {@link RowAdapter}.
 */
@RunWith(JUnit4.class)
public class TestRowAdapter {
  
  private RowAdapter instance = new RowAdapter();

  @Test
  public void adaptResponse_null() {
    assertNull(instance.adaptResponse(null).rawCells());
  }

  @Test
  public void adaptResponse_emptyRow() {
    Row row = Row.newBuilder()
        .setKey(ByteString.copyFromUtf8("key"))
        .build();
    Result result = instance.adaptResponse(row);
    assertEquals(0, result.rawCells().length);
  }

  @Test
  public void adaptResponse_oneRow() {
    String family1 = "family1";
    byte[] family1Bytes = Bytes.toBytes(family1);
    String family2 = "family2";
    byte[] family2Bytes = Bytes.toBytes(family2);
    byte[] qualifier1 = "qualifier1".getBytes();
    byte[] qualifier2 = "qualifier2".getBytes();
    byte[] value1 = "value1".getBytes();
    byte[] value2 = "value2".getBytes();
    byte[] value3 = "value3".getBytes();
    byte[] value4 = "value4".getBytes();
    byte[] value5 = "value5".getBytes();
 
    ByteString key = ByteString.copyFromUtf8("key");
    final long ts1Micros = 54321L;
    final long ts2Micros = 12345L;
    final long ts1Millis = ts1Micros / 1000;
    final long ts2Millis = ts2Micros / 1000;
    Row row = Row.newBuilder()
        .setKey(key)
        .addFamilies(Family.newBuilder()
            .setName(family1)
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFrom(qualifier1))
                .addCells(Cell.newBuilder() // First cell.
                    .setTimestampMicros(ts1Micros)
                    .setValue(ByteString.copyFrom(value1)))
                .addCells(Cell.newBuilder() // Same family, same column, but different timestamps.
                  .setTimestampMicros(ts2Micros)
                  .setValue(ByteString.copyFrom(value2)))
                .addCells(Cell.newBuilder() // With label
                    .setValue(ByteString.copyFromUtf8("withLabel"))
                    .addLabels("label")))
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFrom(qualifier2))
                .addCells(Cell.newBuilder() // Same family, same timestamp, but different column.
                    .setTimestampMicros(ts1Micros)
                    .setValue(ByteString.copyFrom(value3)))))
        .addFamilies(Family.newBuilder()
            .setName(family2)
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFrom(qualifier1))
                .addCells(Cell.newBuilder() // Same column, same timestamp, but different family.
                    .setTimestampMicros(ts1Micros)
                    .setValue(ByteString.copyFrom(value4))))
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFrom(qualifier2))
                .addCells(Cell.newBuilder() // Same timestamp, but different family and column.
                    .setTimestampMicros(ts1Micros)
                    .setValue(ByteString.copyFrom(value5)))))
        .build();

    Result result = instance.adaptResponse(row);
    assertEquals(5, result.rawCells().length);

    // The duplicate row and label cells have been removed. The timestamp micros get converted to
    // millisecond accuracy.
    byte[] keyArray = ByteStringer.extract(key);
    org.apache.hadoop.hbase.Cell[] expectedCells = new org.apache.hadoop.hbase.Cell[] {
        new RowCell(keyArray, family1Bytes, qualifier1, ts1Millis, value1),
        new RowCell(keyArray, family1Bytes, qualifier1, ts2Millis, value2),
        new RowCell(keyArray, family1Bytes, qualifier2, ts1Millis, value3),
        new RowCell(keyArray, family2Bytes, qualifier1, ts1Millis, value4),
        new RowCell(keyArray, family2Bytes, qualifier2, ts1Millis, value5)
    };
    assertArrayEquals(expectedCells, result.rawCells());
  }
}
