/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc.scanner;

import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.common.collect.ImmutableList;

import com.google.protobuf.ByteString;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFlatRowModelAdapter {

  private final FlatRowModelAdapter adapter = new FlatRowModelAdapter();
  private RowAdapter.RowBuilder<FlatRow> rowBuilder;

  @Before
  public void setUp() {
    rowBuilder = adapter.createRowBuilder();
  }

  @Test
  public void testWithSingleCellRow() {
    ByteString value = ByteString.copyFromUtf8("my-value");
    rowBuilder.startRow(ByteString.copyFromUtf8("my-key"));
    rowBuilder.startCell(
        "my-family",
        ByteString.copyFromUtf8("my-qualifier"),
        100,
        ImmutableList.of("my-label"),
        value.size());
    rowBuilder.cellValue(value);
    rowBuilder.finishCell();

    FlatRow expected = FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8("my-key"))
        .addCell("my-family",
            ByteString.copyFromUtf8("my-qualifier"),
            100,
            value,
            ImmutableList.of("my-label")).build();

    Assert.assertEquals(expected, rowBuilder.finishRow());
  }

  @Test
  public void testWithMultiCell() {
    FlatRow.Builder builder = FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8("my-key"));

    rowBuilder.startRow(ByteString.copyFromUtf8("my-key"));

    for (int i = 0; i < 10; i++) {
      ByteString value = ByteString.copyFromUtf8("value-" + i);
      ByteString qualifier = ByteString.copyFromUtf8("qualifier-" + i);
      rowBuilder.startCell("family", qualifier, 1000, ImmutableList.of("my-label"), value.size());
      rowBuilder.cellValue(value);
      rowBuilder.finishCell();

      builder.addCell(
          new FlatRow.Cell("family", qualifier, 1000, value, ImmutableList.of("my-label")));
    }

    Assert.assertEquals(builder.build(), rowBuilder.finishRow());
  }

  @Test
  public void testWhenSplitCell() {
    ByteString part1 = ByteString.copyFromUtf8("part1");
    ByteString part2 = ByteString.copyFromUtf8("part2");

    rowBuilder.startRow(ByteString.copyFromUtf8("my-key"));
    rowBuilder.startCell(
        "family",
        ByteString.copyFromUtf8("qualifier"),
        1000,
        ImmutableList.of("my-label"),
        part1.size() + part2.size());
    rowBuilder.cellValue(part1);
    rowBuilder.cellValue(part2);
    rowBuilder.finishCell();

    FlatRow expected = FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8("my-key"))
        .addCell("family",
            ByteString.copyFromUtf8("qualifier"),
            1000,
            ByteString.copyFromUtf8("part1part2"),
            ImmutableList.of("my-label")).build();

    Assert.assertEquals(expected, rowBuilder.finishRow());
  }

  @Test
  public void testWithMarkerRow() {
    FlatRow markerRow = rowBuilder.createScanMarkerRow(ByteString.copyFromUtf8("key"));
    Assert.assertTrue(adapter.isScanMarkerRow(markerRow));

    ByteString value = ByteString.copyFromUtf8("value");
    rowBuilder.startRow(ByteString.copyFromUtf8("key"));
    rowBuilder.startCell(
        "family", ByteString.EMPTY, 1000, ImmutableList.<String>of(), value.size());
    rowBuilder.cellValue(value);
    rowBuilder.finishCell();

    Assert.assertFalse(adapter.isScanMarkerRow(rowBuilder.finishRow()));
  }
}
