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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
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
public class RowResultAdapter implements RowAdapter<Result> {

  private static final byte[] EMPTY_VALUE = new byte[0];

  @Override
  public RowBuilder<Result> createRowBuilder() {
    return new RowResultBuilder();
  }

  @Override
  public boolean isScanMarkerRow(Result result) {
    if (result instanceof RowResult) {
      return ((RowResult) result).isMarkerRow();
    }
    // TODO: This may never be executed still is it ok to fallback on this?
    return result.isEmpty();
  }

  @Override
  public ByteString getKey(Result result) {
    if (result instanceof RowResult) {
      return ((RowResult) result).getKey();
    }
    return ByteStringer.wrap(result.getRow());
  }

  static class RowResult extends Result {
    private final ByteString rowKey;
    private final boolean isMarkerRow;

    static RowResult create(ByteString rowKey, List<Cell> cells) {
      return new RowResult(rowKey, cells);
    }

    static RowResult createMarker(ByteString rowKey) {
      return new RowResult(rowKey, ImmutableList.<Cell>of());
    }

    private RowResult(ByteString rowKey, List<Cell> cells) {
      this.rowKey = rowKey;
      this.isMarkerRow = cells == null || cells.isEmpty();

      // all except default ctor of Result are private, So instantiating cells through copyFrom()
      // because value(), size(), isEmpty() rawCells() etc. are directly using Result's cells field.
      this.copyFrom(Result.create(cells));
    }

    ByteString getKey() {
      return rowKey;
    }

    boolean isMarkerRow() {
      return isMarkerRow;
    }
  }

  static class RowResultBuilder implements RowBuilder<Result> {
    private ByteString currentKey;
    private String family;
    private byte[] qualifier;
    private List<String> labels;
    private long timestamp;
    private byte[] value;

    private Map<String, List<RowCell>> cells = new TreeMap<>();
    private List<RowCell> currentFamilyCells = null;
    private String previousFamily;
    private int nextValueIndex;

    @Override
    public void startRow(ByteString rowKey) {
      this.currentKey = rowKey;
    }

    @Override
    public void startCell(
        String family, ByteString qualifier, long timestamp, List<String> labels, long size) {
      this.family = family;
      this.qualifier = ByteStringer.extract(qualifier);
      this.timestamp = timestamp;
      this.labels = labels;
      if (size > 0) {
        this.value = new byte[(int) size];
        this.nextValueIndex = 0;
      } else {
        this.value = EMPTY_VALUE;
        this.nextValueIndex = -1;
      }
    }

    @Override
    public void cellValue(ByteString newValue) {
      // Optimize unsplit cells by avoiding a copy
      if (nextValueIndex == -1) {
        this.value = ByteStringer.extract(newValue);
        nextValueIndex = newValue.size();
        return;
      }

      Preconditions.checkState(
          nextValueIndex + newValue.size() <= value.length, "Cell value is larger than expected");

      newValue.copyTo(this.value, this.nextValueIndex);
      nextValueIndex += newValue.size();
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
      Preconditions.checkNotNull(currentKey, "row key cannot be null");
      Preconditions.checkState(nextValueIndex == value.length, "Cell value too short");

      if (!Objects.equals(this.family, this.previousFamily)) {
        previousFamily = this.family;
        currentFamilyCells = new ArrayList<>();
        cells.put(this.family, this.currentFamilyCells);
      }

      RowCell rowCell =
          new RowCell(
              ByteStringer.extract(this.currentKey),
              this.family.getBytes(),
              this.qualifier,
              TimestampConverter.bigtable2hbase(this.timestamp),
              this.value,
              this.labels);
      this.currentFamilyCells.add(rowCell);
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
      ImmutableList.Builder<Cell> combined = ImmutableList.builder();
      for (List<RowCell> familyCellList : cells.values()) {
        combined.addAll(familyCellList);
      }

      return RowResult.create(currentKey, combined.build());
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
    }

    @Override
    public Result createScanMarkerRow(ByteString rowKey) {
      return RowResult.createMarker(rowKey);
    }
  }
}
