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
import java.util.List;

/**
 * Adapter for {@link RowAdapter} that uses {@link FlatRow}'s to represent logical rows.
 */
public class FlatRowModelAdapter implements RowAdapter<FlatRow> {

  @Override
  public RowBuilder<FlatRow> createRowBuilder() {
    return new FlatRowModelAdapter.FlatRowBuilder();
  }

  public class FlatRowBuilder implements RowBuilder<FlatRow> {
    private ByteString currentKey;
    private ImmutableList.Builder<FlatRow.Cell> cells;
    private String family;
    private ByteString qualifier;
    private List<String> labels;
    private long timestamp;
    private ByteString value;

    public FlatRowBuilder() {
    }

    @Override
    public void startRow(ByteString rowKey) {
      this.currentKey = rowKey;
      this.cells = ImmutableList.builder();
    }

    @Override
    public void startCell(String family, ByteString qualifier, long timestamp, List<String> labels,
        long size) {
      this.family = family;
      this.qualifier = qualifier;
      this.timestamp = timestamp;
      this.labels = labels;
      this.value = ByteString.EMPTY;
    }

    @Override
    public void cellValue(ByteString value) {
      this.value = this.value.concat(value);
    }

    @Override
    public void finishCell() {
      this.cells.add(
          new FlatRow.Cell(this.family, this.qualifier, this.timestamp, this.value, this.labels));
    }

    @Override
    public FlatRow finishRow() {
      return new FlatRow(this.currentKey, this.cells.build());
    }

    @Override
    public void reset() {
      this.currentKey = null;
      this.cells = null;
      this.family = null;
      this.qualifier = null;
      this.labels = null;
      this.timestamp = 0L;
      this.value = null;
    }

    @Override
    public FlatRow createScanMarkerRow(ByteString rowKey) {
      return new FlatRow(rowKey, ImmutableList.<FlatRow.Cell>of());
    }
  }

  @Override
  public boolean isScanMarkerRow(FlatRow row) {
    return row.getCells().isEmpty();
  }

  @Override
  public ByteString getKey(FlatRow row) {
    return row.getRowKey();
  }
}
