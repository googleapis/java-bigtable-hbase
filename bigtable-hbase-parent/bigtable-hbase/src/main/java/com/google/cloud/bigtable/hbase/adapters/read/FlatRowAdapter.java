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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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

  static final Comparator<RowCell> QUALIFIER_AND_TIMESTAMP_COMPARATOR =
      new Comparator<RowCell>() {
        @Override
        public int compare(RowCell left, RowCell right) {
          int result = Bytes.compareTo(left.getQualifierArray(), right.getQualifierArray());
          if (result != 0) {
            return result;
          }
          return Long.compare(right.getTimestamp(), left.getTimestamp());
        }
      };

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
    List<Cell> hbaseCells = extractCells(flatRow, sort);
    return Result.create(hbaseCells.toArray(new Cell[hbaseCells.size()]));
  }

  /**
   * Converts all of the {@link FlatRow.Cell}s into HBase {@link RowCell}s.
   * <p>
   * All @link FlatRow.Cell}s with labels are discarded, since those are used only for
   * {@link WhileMatchFilter}
   * <p>
   * Some additional deduping and sorting is also required. Cloud Bigtable returns values sorted by
   * family in lexicographically ascending order, an arbitrary ordering of qualifiers (by internal
   * id rather than lexicographically) and finally by timestamp descending. Each cell can appear
   * more than once, if there are {@link Interleave}s in the {@link ReadRowsRequest#getFilter()},
   * but the duplicates will appear one after the other. An {@link Interleave}s can originate from a
   * {@link FilterList} with a {@link FilterList.Operator#MUST_PASS_ONE} operator or a
   * {@link WhileMatchFilter}.
   * <p>
   * @param flatRow
   * @param sort
   * @return
   */
  private List<Cell> extractCells(FlatRow flatRow, boolean sort) {
    byte[] rowKey = ByteStringer.extract(flatRow.getRowKey());

    FlatRow.Cell previousCell = null;
    byte[] previousFamily = null;

    List<FlatRow.Cell> cells = flatRow.getCells();
    List<Cell> hbaseCells = new ArrayList<>(cells.size());
    List<RowCell> familyCells = sort ? new ArrayList<RowCell>(cells.size()) : null;

    for (FlatRow.Cell cell : cells) {
      // Cells with labels are for internal use, do not return them.
      // TODO(kevinsi4508): Filter out targeted {@link WhileMatchFilter} labels.
      //
      // Perform deduplication by skipping cells with matching key (family, qualifier, timestamp).
      if (!cell.getLabels().isEmpty() || cell.equalFamilyQualifierAndTimestamp(previousCell)) {
        continue;
      }

      // Bigtable timestamp has more granularity than HBase one. It is possible that Bigtable
      // cells are deduped unintentionally here. On the other hand, if we don't dedup them,
      // HBase will treat them as duplicates.
      long hbaseTimestamp = cell.getTimestamp() / TIME_CONVERSION_UNIT;
      byte[] family = getFamily(previousCell, previousFamily, cell);
      RowCell rowCell =
          new RowCell(
              rowKey,
              family,
              ByteStringer.extract(cell.getQualifier()),
              hbaseTimestamp,
              ByteStringer.extract(cell.getValue()));
      if (!sort) {
        hbaseCells.add(rowCell);
      } else {
        // getFamily() returns previousFamily if the family Strings contained in the FlatRow.Cell
        // are equals. We can use 
        if (previousFamily != null && family != previousFamily) {
          addAll(hbaseCells, familyCells);
        }
        familyCells.add(rowCell);
      }
      previousCell = cell;
      previousFamily = family;
    }
    if (sort) {
      addAll(hbaseCells, familyCells);
    }
    return hbaseCells;
  }

  private static void addAll(List<Cell> hbaseCells, List<RowCell> familyCells) {
    Collections.sort(familyCells, QUALIFIER_AND_TIMESTAMP_COMPARATOR);
    hbaseCells.addAll(familyCells);
    familyCells.clear();
  }

  private byte[] getFamily(FlatRow.Cell previousCell, byte[] previousFamily, FlatRow.Cell cell) {
    if (previousCell != null && Objects.equal(previousCell.getFamily(), cell.getFamily())) {
      return previousFamily;
    } else {
      return Bytes.toBytes(cell.getFamily());
    }
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
