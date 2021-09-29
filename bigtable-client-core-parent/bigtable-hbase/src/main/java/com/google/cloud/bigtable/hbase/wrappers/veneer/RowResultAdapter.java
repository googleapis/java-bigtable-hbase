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
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.cloud.bigtable.data.v2.models.Filters.InterleaveFilter;
import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.common.base.Preconditions;
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
import org.apache.hadoop.hbase.filter.FilterList.Operator;

/**
 * Adapter for {@link RowAdapter} that uses {@link Result} to represent logical rows.
 *
 * <p>This adapter is responsible for cell deduplication. bigtable-hbase will convert a {@link
 * Operator#MUST_PASS_ONE} filter into an {@link Interleave}. Unfortunately there is a bit of a
 * mismatch between the 2 filters: MUST_PASS_ONE will not duplicate the cell if it matches multiple
 * branches of the MUST_PASS_ONE, but Interleave will.This adapter will pave over the difference by
 * removing the duplicate cells while building the Result. However, HBase's WhileMatchFilter depends
 * on duplicate labelled cells for its implementation. So this adapter will not deduplicate labelled
 * cells.
 *
 * <p>This adapter will also return and check for scan marker rows, which will be an empty row with
 * the scan marker row label.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class RowResultAdapter implements RowAdapter<Result> {

  private static final byte[] EMPTY_VALUE = new byte[0];
  private static final String SCAN_MARKER_ROW_LABEL = "bigtable-scan-marker-row";

  @Override
  public RowBuilder<Result> createRowBuilder() {
    return new RowResultBuilder();
  }

  /**
   * Checks if the result is a scan marker row. Returns true if the row's family, qualifier, and
   * value are empty, and only has a scan marker row label.
   */
  @Override
  public boolean isScanMarkerRow(Result result) {
    if (result.rawCells().length != 1 || !(result.rawCells()[0] instanceof RowCell)) {
      return false;
    }

    RowCell cell = (RowCell) result.rawCells()[0];
    return cell.getLabels().size() == 1
        && cell.getLabels().get(0).equals(SCAN_MARKER_ROW_LABEL)
        && cell.getValueArray().length == 0
        && cell.getFamilyArray().length == 0
        && cell.getQualifierArray().length == 0;
  }

  @Override
  public ByteString getKey(Result result) {
    Cell cell = result.rawCells()[0];
    return ByteStringer.wrap(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  static class RowResultBuilder implements RowBuilder<Result> {
    private ByteString currentKey;
    private String family;
    private byte[] qualifier;
    private List<String> labels;
    private long timestamp;
    private byte[] value;
    private RowCell previousNoLabelCell;

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
     *
     * <p>Applying {@link InterleaveFilter} may result in a row to contain duplicated cells, where
     * duplicates are grouped in sequences.
     */
    @Override
    public void finishCell() {
      Preconditions.checkNotNull(currentKey, "row key cannot be null");
      Preconditions.checkState(nextValueIndex == value.length, "Cell value too short");

      if (!Objects.equals(this.family, this.previousFamily)) {
        previousFamily = this.family;
        currentFamilyCells = new ArrayList<>();
        cells.put(this.family, this.currentFamilyCells);
        previousNoLabelCell = null;
      }

      RowCell rowCell =
          new RowCell(
              ByteStringer.extract(this.currentKey),
              this.family.getBytes(),
              this.qualifier,
              TimestampConverter.bigtable2hbase(this.timestamp),
              this.value,
              this.labels);

      // dedupe user visible cells. Please see class javadoc for details
      if (!this.labels.isEmpty()) {
        this.currentFamilyCells.add(rowCell);
      } else if (!keysMatch(previousNoLabelCell, rowCell)) {
        this.currentFamilyCells.add(rowCell);
        this.previousNoLabelCell = rowCell;
      }
    }

    private static boolean keysMatch(RowCell previousNoLabelCell, RowCell current) {
      return previousNoLabelCell != null
          && previousNoLabelCell.getTimestamp() == current.getTimestamp()
          && Arrays.equals(previousNoLabelCell.getQualifierArray(), current.getQualifierArray());
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

      return Result.create(combined.build());
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
      this.previousNoLabelCell = null;
    }

    /**
     * Creates a marker row with rowKey with a scan marker row label and empty family, qualifier and
     * value.
     */
    @Override
    public Result createScanMarkerRow(ByteString rowKey) {
      return Result.create(
          ImmutableList.<Cell>of(
              new RowCell(
                  ByteStringer.extract(rowKey),
                  EMPTY_VALUE,
                  EMPTY_VALUE,
                  0l,
                  EMPTY_VALUE,
                  ImmutableList.<String>of(SCAN_MARKER_ROW_LABEL))));
    }
  }
}
