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
package com.google.cloud.bigtable.hbase.adapters.read;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

/**
 * Adapter for {@link RowAdapter} that uses {@link Result} to represent logical rows.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ResultRowAdapter implements RowAdapter<Result> {

  @Override
  public RowBuilder<Result> createRowBuilder() {
    return new ResultRowBuilder();
  }

  @Override
  public boolean isScanMarkerRow(Result result) {
    return result.isEmpty();
  }

  @Override
  public ByteString getKey(Result result) {
    return ByteStringer.wrap(result.getRow());
  }

  public static class ResultRowBuilder implements RowBuilder<Result> {
    private byte[] currentKey;
    private String family;
    private ByteString qualifier;
    private List<String> labels;
    private long timestamp;
    private ByteString value;

    private Map<String, List<RowCell>> cells = new TreeMap<>();
    private List<RowCell> currentFamilyCells = null;
    private String previousFamily;
    private int totalCellCount = 0;

    @Override
    public void startRow(ByteString rowKey) {
      this.currentKey = ByteStringer.extract(rowKey);
    }

    @Override
    public void startCell(
        String family, ByteString qualifier, long timestamp, List<String> labels, long size) {
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
     * Adds a Cell to {@link RowCell}'s map which is ordered by family. cells received from {@link
     * RowBuilder} has ordering as:
     *
     * <ul>
     *   <li>family names clustered, but not sorted
     *   <li>qualifiers in each family cluster is sorted lexicographically
     *   <li>then descending by timestamp
     * </ul>
     *
     * The end result will be that {@link RowCell} are ordered as:
     *
     * <ul>
     *   <li>lexicographical by family
     *   <li>then lexicographical by qualifier
     *   <li>then descending by timestamp
     * </ul>
     *
     * A flattened version of the {@link RowCell} map will be sorted correctly.
     */
    @Override
    public void finishCell() {
      if (!Objects.equals(this.family, this.previousFamily)) {
        previousFamily = this.family;
        currentFamilyCells = new ArrayList<>();
        cells.put(this.family, this.currentFamilyCells);
      }

      RowCell rowCell =
          new RowCell(
              this.currentKey,
              this.family.getBytes(),
              ByteStringer.extract(this.qualifier),
              TimestampConverter.bigtable2hbase(this.timestamp),
              ByteStringer.extract(this.value),
              this.labels);
      this.currentFamilyCells.add(rowCell);
      totalCellCount++;
    }

    /**
     * This method flattens the {@code cells} which has a map of Lists keyed by family name. The
     * {@code cells} treeMap is sorted lexicographically, and each List is sorted by qualifier in
     * lexicographically ascending order, and timestamp in descending order.
     *
     * @return an object of HBase {@link Result}.
     */
    @Override
    public Result finishRow() {
      if (totalCellCount == 0) {
        return Result.EMPTY_RESULT;
      }

      ImmutableList.Builder<Cell> combined = ImmutableList.builder();
      for (List<RowCell> familyCellList : cells.values()) {

        RowCell previous = null;
        for (RowCell rowCell : familyCellList) {
          if (previous == null || !rowCell.getLabels().isEmpty() || !keysMatch(rowCell, previous)) {
            combined.add(rowCell);
          }
          previous = rowCell;
        }
      }

      return Result.create(combined.build());
    }

    private boolean keysMatch(RowCell current, RowCell previous) {
      return current.getTimestamp() == previous.getTimestamp()
          && Arrays.equals(current.getQualifierArray(), previous.getQualifierArray());
    }

    @Override
    public void reset() {
      this.currentKey = null;
      this.family = null;
      this.qualifier = null;
      this.labels = null;
      this.timestamp = 0L;
      this.value = null;
      this.cells = new TreeMap<>();
      this.currentFamilyCells = null;
      this.previousFamily = null;
      this.totalCellCount = 0;
    }

    @Override
    public Result createScanMarkerRow(ByteString rowKey) {
      return Result.EMPTY_RESULT;
    }
  }
}
