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

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
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
    Row row = Row.create(ByteString.copyFromUtf8("key"), Collections.<RowCell>emptyList());
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
    List<String> emptyLabelList = Collections.emptyList();

    ImmutableList<RowCell> rowCells =  ImmutableList.of(
        // First cell.
        RowCell.create(family1, ByteString.copyFrom(qualifier1), ts1Micros, emptyLabelList,
            ByteString.copyFrom(value1)),
        // Same family, same column, but different timestamps.
        RowCell.create(family1, ByteString.copyFrom(qualifier1), ts2Micros, emptyLabelList,
          ByteString.copyFrom(value2)),
        // Same family, same timestamp, but different column.
        RowCell.create(family1, ByteString.copyFrom(qualifier2), ts1Micros, emptyLabelList,
          ByteString.copyFrom(value3)),
        // Same column, same timestamp, but different family.
        RowCell.create(family2, ByteString.copyFrom(qualifier1), ts1Micros, emptyLabelList,
            ByteString.copyFrom(value4)),
        // Same timestamp, but different family and column.
        RowCell.create(family2, ByteString.copyFrom(qualifier2), ts1Micros, emptyLabelList,
            ByteString.copyFrom(value5)),
        //Contains label, should be ignored.
        RowCell.create(family1, ByteString.copyFrom(qualifier1), ts2Micros,
            Collections.singletonList("label"), ByteString.copyFrom(value1))
    );

    Row row = Row.create(key, rowCells);
    Result result = instance.adaptResponse(row);
    assertEquals(5, result.rawCells().length);

    // The duplicate row and label cells have been removed. The timestamp micros get converted to
    // millisecond accuracy.
    byte[] keyArray = ByteStringer.extract(key);
    org.apache.hadoop.hbase.Cell[] expectedCells = new org.apache.hadoop.hbase.Cell[] {
        new com.google.cloud.bigtable.hbase.adapters.read.RowCell
            (keyArray, family1Bytes, qualifier1, ts1Millis, value1),
        new com.google.cloud.bigtable.hbase.adapters.read.RowCell
            (keyArray, family1Bytes, qualifier1, ts2Millis, value2),
        new com.google.cloud.bigtable.hbase.adapters.read.RowCell
            (keyArray, family1Bytes, qualifier2, ts1Millis, value3),
        new com.google.cloud.bigtable.hbase.adapters.read.RowCell
            (keyArray, family2Bytes, qualifier1, ts1Millis, value4),
        new com.google.cloud.bigtable.hbase.adapters.read.RowCell
            (keyArray, family2Bytes, qualifier2, ts1Millis, value5)
    };
    assertArrayEquals(expectedCells, result.rawCells());
  }
}
