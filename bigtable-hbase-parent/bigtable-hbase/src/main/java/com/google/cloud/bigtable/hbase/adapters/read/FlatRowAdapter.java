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

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Adapt between a {@link FlatRow} and an hbase client {@link org.apache.hadoop.hbase.client.Result}.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class FlatRowAdapter implements ResponseAdapter<FlatRow, Result> {
  // This only works because BIGTABLE_TIMEUNIT is smaller than HBASE_TIMEUNIT, otherwise we will get
  // 0.
  static final long TIME_CONVERSION_UNIT = BigtableConstants.BIGTABLE_TIMEUNIT.convert(1,
    BigtableConstants.HBASE_TIMEUNIT);


  /**
   * Converts all of the {@link FlatRow.Cell}s into HBase {@link RowCell}s.
   * <p>
   * All @link FlatRow.Cell}s with labels are discarded, since those are used only for
   * {@link WhileMatchFilter}
   * <p>
   * Some additional deduping and sorting is also required. Cloud Bigtable returns values sorted by
   * family (by internal id, not lexicographically), a lexicographically ascending ordering of
   * qualifiers, and finally by timestamp descending. Each cell can appear more than once, if there
   * are {@link Interleave}s in the {@link ReadRowsRequest#getFilter()}, but the duplicates will
   * appear one after the other. An {@link Interleave}s can originate from a {@link FilterList} with
   * a {@link FilterList.Operator#MUST_PASS_ONE} operator or a {@link WhileMatchFilter}.
   */
  private static class CellSorter {
    private final byte[] rowKey;
    private final FlatRow flatRow;

    private FlatRow.Cell previousCell = null;
    private byte[] previousFamily = null;

    private final Map<String, List<RowCell>> familyMap = new TreeMap<>();
    private List<RowCell> currentFamilyRowCells = null;

    public CellSorter(FlatRow flatRow) {
      this.flatRow = flatRow;
      this.rowKey = ByteStringer.extract(flatRow.getRowKey());
    }

    public Cell[] getSortedAndDedupedHBaseCells() {
      for (FlatRow.Cell cell : flatRow.getCells()) {
        // Cells with labels are for internal use, do not return them.
        // TODO(kevinsi4508): Filter out targeted {@link WhileMatchFilter} labels.
        if (cell.getLabels().isEmpty()) {
          processCell(cell);
        }
      }
      return flattenFamilyMap();
    }

    /**
     * Processes the next {@link FlatRow.Cell}. Duplicate cells are removed, and new cells are added
     * to {@link #currentFamilyRowCells}. If this cell has a new family, create a new List for
     * {@link #currentFamilyRowCells}, and add the new List to {@link #familyMap}
     * @param currentCell
     */
    private void processCell(FlatRow.Cell currentCell) {
      byte[] familyBytes;
      String familyString = currentCell.getFamily();

      if (!isSameFamily(familyString)) {
        // Found a new family, which means all of the previous family's cells were processed.
        addFamily(familyString);
        familyBytes = Bytes.toBytes(familyString);

      } else if (isSameTimestampAndQualifier(currentCell)) {
        // Dedup cells where the family, qualifier and timestamp are the same.
        return;

      } else {
        // This is the same family, but a new qualifier or timestamp.
        // Converting a String to byte[] is expensive. Use a cached value for familyBytes to avoid
        // the conversion cost.
        familyBytes = previousFamily;
      }

      // Add the currentCell to the list of Cells for the current family.
      addCell(currentCell, familyBytes);
    }

    /**
     * @param familyString A String representation of the family of a new cell.
     * @return true if the previous cell's family and the new family are equal.
     */
    private boolean isSameFamily(String familyString) {
      return previousCell != null && familyString.equals(previousCell.getFamily());
    }

    /**
     * Checks to see if the current {@link FlatRow.Cell}'s qualifier and timestamp are equal to the
     * previous {@link FlatRow.Cell}'s. This method assumes that {@link #isSameFamily(String)} was
     * called, and the {@link #previousCell} is not null and has the same family as the new cell.
     * @param the new {@link FlatRow.Cell}
     * @return true if the new cell and old cell have logical equivalency.
     */
    private boolean isSameTimestampAndQualifier(FlatRow.Cell cell) {
      return cell.getTimestamp() == previousCell.getTimestamp()
          && Objects.equal(previousCell.getQualifier(), cell.getQualifier());
    }

    /**
     * A new family was found. Create a new entry in the {@link familyMap} for this family.
     *
     * @param family The String representation of the new family.
     */
    private void addFamily(String family) {
      currentFamilyRowCells = new ArrayList<>();
      familyMap.put(family, currentFamilyRowCells);
    }

    private void addCell(FlatRow.Cell currentCell, byte[] family) {
      currentFamilyRowCells.add(toRowCell(rowKey, currentCell, family));
      previousCell = currentCell;
      previousFamily = family;
    }

    /**
     * This method flattens the {@link #familyMap} which has a map of Lists keyed by family name.
     * The {@link #familyMap} TreeMap is sorted lexicographically, and each List is sorted by
     * qualifier in lexicographically ascending order, and timestamp in descending order.
     *
     * @return an array of HBase {@link Cell}s that is sorted by family asc, qualifier asc, timestamp desc.
     */
    private Cell[] flattenFamilyMap() {
      int cellCount = 0;
      for (List<RowCell> rowCellList : familyMap.values()) {
        cellCount += rowCellList.size();
      }
      Cell[] combined = new Cell[cellCount];
      int i = 0;
      for (List<RowCell> rowCellList : familyMap.values()) {
        for (int j = 0; j < rowCellList.size(); j++) {
          combined[i++] = rowCellList.get(j);
        }
      }
      return combined;
    }

  }

    /**
   * {@inheritDoc}
   *
   * Convert a {@link FlatRow} to a {@link Result}.
   */
  @Override
  public Result adaptResponse(FlatRow flatRow) {
    if (flatRow == null) {
      return Result.EMPTY_RESULT;
    }
    return Result.create(new CellSorter(flatRow).getSortedAndDedupedHBaseCells());
  }

  private static RowCell toRowCell(byte[] rowKey, FlatRow.Cell cell, byte[] family) {
    return new RowCell(
        rowKey,
        family,
        ByteStringer.extract(cell.getQualifier()),
        // Bigtable timestamp has more granularity than HBase one. It is possible that Bigtable
        // cells are deduped unintentionally here. On the other hand, if we don't dedup them,
        // HBase will treat them as duplicates.
        cell.getTimestamp() / TIME_CONVERSION_UNIT,
        ByteStringer.extract(cell.getValue()));
  }

  /**
   * Convert a {@link org.apache.hadoop.hbase.client.Result} to a {@link FlatRow}.
   *
   * @param result a {@link org.apache.hadoop.hbase.client.Result} object.
   * @return a {@link FlatRow} object.
   */
  public FlatRow adaptToRow(Result result) {
    // Result.getRow() is derived from its cells.  If the cells are empty, the row will be null.
    if (result.getRow() == null) {
      return null;
    }

    FlatRow.Builder rowBuilder =
        FlatRow.newBuilder().withRowKey(ByteStringer.wrap(result.getRow()));

    final Cell[] rawCells = result.rawCells();
    if (rawCells != null && rawCells.length > 0) {
      for (Cell rawCell : rawCells) {
        rowBuilder.addCell(
          Bytes.toString(rawCell.getFamilyArray()),
          ByteStringer.wrap(rawCell.getQualifierArray()),
          rawCell.getTimestamp() * TIME_CONVERSION_UNIT,
          ByteStringer.wrap(rawCell.getValueArray()));
      }
    }

    return rowBuilder.build();
  }
}
