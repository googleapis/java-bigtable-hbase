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
   * {@inheritDoc}
   * 
   * Convert a {@link FlatRow} to a {@link Result}.
   */
  @Override
  public Result adaptResponse(FlatRow flatRow) {
    return adaptResponse(flatRow, true);
  }

  /**
   * Convert a {@link FlatRow} to a {@link Result} without sorting the cells first.
   */
  public Result adaptResponsePresortedCells(FlatRow flatRow) {
    return adaptResponse(flatRow, false);
  }

  private Result adaptResponse(FlatRow flatRow, final boolean sort) {
    if (flatRow == null) {
      return Result.EMPTY_RESULT;
    }
    Cell[] hbaseCells = sort ? extractCellsSortAndDedup(flatRow) : extractPresortedCells(flatRow);
    return Result.create(hbaseCells);
  }

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
   * <p>
   * @param flatRow
   * @return a
   */
  private Cell[] extractCellsSortAndDedup(FlatRow flatRow) {
    byte[] rowKey = ByteStringer.extract(flatRow.getRowKey());

    FlatRow.Cell previousCell = null;
    byte[] previousFamily = null;

    Map<String, List<RowCell>> familyMap = new TreeMap<>();
    List<RowCell> currentFamilyRowCells = new ArrayList<>(flatRow.getCells().size());

    int cellCount = 0;
    for (FlatRow.Cell cell : flatRow.getCells()) {
      // Cells with labels are for internal use, do not return them.
      // TODO(kevinsi4508): Filter out targeted {@link WhileMatchFilter} labels.
      if (!cell.getLabels().isEmpty()) {
        continue;
      }

      byte[] family;
      String familyString = cell.getFamily();
      if (previousCell != null && Objects.equal(previousCell.getFamily(), familyString)) {
        // Dedup cells where the family, qualifier and timestamp are the same.
        if (cell.getTimestamp() == previousCell.getTimestamp()
            && Objects.equal(previousCell.getQualifier(), cell.getQualifier())) {
          continue;
        }
        family = previousFamily;
      } else {
        family = Bytes.toBytes(familyString);
        currentFamilyRowCells = new ArrayList<>();
        familyMap.put(familyString, currentFamilyRowCells);
      }

      currentFamilyRowCells.add(toRowCell(rowKey, cell, family));
      previousCell = cell;
      previousFamily = family;
      cellCount++;
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

  /**
   * Converts all of the {@link FlatRow.Cell}s into HBase {@link RowCell}s.
   * @param flatRow
   * @return
   */
  private Cell[] extractPresortedCells(FlatRow flatRow) {
    byte[] rowKey = ByteStringer.extract(flatRow.getRowKey());
    Cell[] hbaseCells = new Cell[flatRow.getCells().size()];
    int i = 0;
    for (FlatRow.Cell cell : flatRow.getCells()) {
      hbaseCells[i++] = toRowCell(rowKey, cell, Bytes.toBytes(cell.getFamily()));
    }
    return hbaseCells;
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
