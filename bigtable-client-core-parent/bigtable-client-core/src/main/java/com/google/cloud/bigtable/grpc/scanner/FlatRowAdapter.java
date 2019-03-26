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
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Adapter for {@link RowAdapter} that uses {@link FlatRow}'s to represent logical rows.
 */
public class FlatRowAdapter implements RowAdapter<FlatRow> {

  /** {@inheritDoc} */
  @Override
  public RowBuilder<FlatRow> createRowBuilder() {
    return new FlatRowAdapter.FlatRowBuilder();
  }

  public class FlatRowBuilder implements RowBuilder<FlatRow> {
    private ByteString currentKey;
    private String family;
    private ByteString qualifier;
    private List<String> labels;
    private long timestamp;
    private ByteString value;

    /*
     * cells contains list of {@link Cell} for all the families.
     */
    private Map<String, List<Cell>> cells = new TreeMap<>();

    /*
     * currentFamilyCells is buffered with current family's {@link Cell}s.
     */
    private List<Cell> currentFamilyCells = null;
    private String previousFamily;
    private int totalCellCount = 0;

    public FlatRowBuilder() {
    }

    /** {@inheritDoc} */
    @Override
    public void startRow(ByteString rowKey) {
      this.currentKey = rowKey;
    }

    /** {@inheritDoc} */
    @Override
    public void startCell(String family, ByteString qualifier, long timestamp, List<String> labels,
        long size) {
      this.family = family;
      this.qualifier = qualifier;
      this.timestamp = timestamp;
      this.labels = labels;
      this.value = ByteString.EMPTY;
    }

    /** {@inheritDoc} */
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
      if (!Objects.equals(this.family, this.previousFamily)) {
        previousFamily = this.family;
        currentFamilyCells = new ArrayList<>();
        cells.put(this.family, this.currentFamilyCells);
      }

      FlatRow.Cell cell  = new FlatRow.Cell(this.family, this.qualifier, this.timestamp,
                    this.value, this.labels);
      this.currentFamilyCells.add(cell);
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
      FlatRow.Cell previous = null;
      for (List<FlatRow.Cell> familyCellList : cells.values()) {
        for(FlatRow.Cell c : familyCellList) {
          if(previous == null || !c.getLabels().isEmpty() || !keysMatch(c, previous)){
            combined.add(c);
          }
          previous = c;
        }
      }

      return new FlatRow(this.currentKey, combined.build());
    }

    private boolean keysMatch(Cell c, Cell previous) {
      return c.equals(previous);
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public FlatRow createScanMarkerRow(ByteString rowKey) {
      return new FlatRow(rowKey, ImmutableList.<Cell>of());
    }
  }

  /**
   * A comparator that compares the cells by Bigtable native ordering:
   *
   * <ul>
   *   <li>Family lexicographically ascending
   *   <li>Qualifier lexicographically ascending
   *   <li>Timestamp in reverse chronological order
   *   <li>Labels are also included as label with "-in" & "-out" are needed for
   *   WhileMatchFilter
   * </ul>
   *
   * <p>Values are not included in the comparison.
   */
//  private static Comparator<FlatRow.Cell> compareCells() {
//    return new Comparator<FlatRow.Cell>() {
//      @Override
//      public int compare(FlatRow.Cell c1, FlatRow.Cell c2) {
//        return ComparisonChain.start()
//            .compare(c1.getFamily(), c2.getFamily())
//            .compare(c1.getQualifier(), c2.getQualifier(), ByteStringComparator.INSTANCE)
//            .compare(c2.getTimestamp(), c1.getTimestamp())
//            .compare(c1.getLabels(), c2.getLabels(), compareLables())
//            .result();
//      }
//    };
//  }
//
//  private static Comparator<List<String>> compareLables(){
//    return new Comparator<List<String>>() {
//      @Override
//      public int compare(List<String> o1, List<String> o2) {
//        if(o1.size() == o2.size() && o1.containsAll(o2))
//          return 0;
//        return o1.size() < o2.size()?-1:1;
//      }
//    };
//  }

  /** {@inheritDoc} */
  @Override
  public boolean isScanMarkerRow(FlatRow row) {
    return row.getCells().isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public ByteString getKey(FlatRow row) {
    return row.getRowKey();
  }
}
