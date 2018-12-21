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
import com.google.cloud.bigtable.grpc.scanner.FlatRow.Cell;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Adapter for {@link RowAdapter} that uses {@link FlatRow}'s to represent logical rows.
 */
public class FlatRowAdapter implements RowAdapter<FlatRow> {

  @Override
  public RowBuilder<FlatRow> createRowBuilder() {
    return new FlatRowAdapter.FlatRowBuilder();
  }

  public class FlatRowBuilder implements RowBuilder<FlatRow> {
    private ByteString currentKey;
    private Map<String, List<Cell>> cells = new TreeMap<>();
    private List<Cell> familyCells = null;

    private String family;
    private ByteString qualifier;
    private List<String> labels;
    private long timestamp;
    private ByteString value;

    private String previousFamily;
    private int totalCellCount = 0;

    public FlatRowBuilder() {
    }

    @Override
    public void startRow(ByteString rowKey) {
      this.currentKey = rowKey;
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

    /**
     * Adds a Cell to {@link Cell}'s map which is ordered by family. cells received from
     * {@link RowBuilder} has ordering as:
     *   <ul>
     *     <li>family names clustered, but not sorted</li>
     *     <li>qualifiers in each family cluster is sorted lexicographically</li>
     *     <li>then descending by timestamp</li>
     *   </ul>
     * The end result will be that {@link Cell} are ordered as:
     *    <ul>
     *      <li>lexicographical by family</li>
     *      <li>then lexicographical by qualifier</li>
     *      <li>then descending by timestamp</li>
     *    </ul>
     * A flattened version of the {@link Cell} map will be sorted correctly.
     */
    @Override
    public void finishCell() {
      if (!Objects.equal(this.family, this.previousFamily)) {
        previousFamily = this.family;
        familyCells = new ArrayList<>();
        cells.put(this.family, this.familyCells);
      }

      FlatRow.Cell cell  = new FlatRow.Cell(this.family, this.qualifier, this.timestamp,
                    this.value, this.labels);
      this.familyCells.add(cell);
      totalCellCount++;
  }

    /**
     * This method flattens the {@link #cells} which has a map of Lists keyed by family name.
     * The {@link #cells} TreeMap is sorted lexicographically, and each List is sorted by
     * qualifier in lexicographically ascending order, and timestamp in descending order.
     *
     * @return an object of HBase {@link FlatRow}.
     */
    @Override
    public FlatRow finishRow() {
      ImmutableList.Builder<FlatRow.Cell> combined = ImmutableList.builderWithExpectedSize(totalCellCount);
      for (List<FlatRow.Cell> familyCellList : cells.values()) {
        combined.addAll(familyCellList);
      }

      return new FlatRow(this.currentKey, combined.build());
    }

    @Override
    public void reset() {
      this.currentKey = null;
      this.cells = new TreeMap<>();
      this.family = null;
      this.qualifier = null;
      this.labels = null;
      this.timestamp = 0L;
      this.value = null;
      this.previousFamily = null;
      this.totalCellCount = 0;
    }

    @Override
    public FlatRow createScanMarkerRow(ByteString rowKey) {
      return new FlatRow(rowKey, ImmutableList.<Cell>of());
    }
  }

  @Override
  public boolean isScanMarkerRow(FlatRow row) {
    return row.getCells() == null || row.getCells().isEmpty();
  }

  @Override
  public ByteString getKey(FlatRow row) {
    return row.getRowKey();
  }
}
