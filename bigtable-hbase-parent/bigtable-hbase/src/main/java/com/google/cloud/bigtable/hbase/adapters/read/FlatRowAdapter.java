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

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.util.ByteStringer;

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
   * Convert a {@link FlatRow} to a {@link Result}.
   */
  @Override
  public Result adaptResponse(FlatRow flatRow) {
    if (flatRow == null) {
      return Result.EMPTY_RESULT;
    }

    SortedSet<org.apache.hadoop.hbase.Cell> hbaseCells = new TreeSet<>(KeyValue.COMPARATOR);
    byte[] rowKey = ByteStringer.extract(flatRow.getRowKey());

    for (FlatRow.Cell cell : flatRow.getCells()) {
      // Cells with labels are for internal use, do not return them.
      // TODO(kevinsi4508): Filter out targeted {@link WhileMatchFilter} labels.
      if (!cell.getLabels().isEmpty()) {
        continue;
      }

      // Bigtable timestamp has more granularity than HBase one. It is possible that Bigtable
      // cells are deduped unintentionally here. On the other hand, if we don't dedup them,
      // HBase will treat them as duplicates.
      long hbaseTimestamp = cell.getTimestamp() / TIME_CONVERSION_UNIT;
      RowCell keyValue = new RowCell(
          rowKey,
          Bytes.toBytes(cell.getFamily()),
          ByteStringer.extract(cell.getQualifier()),
          hbaseTimestamp,
          ByteStringer.extract(cell.getValue()));

      hbaseCells.add(keyValue);
    }

    return Result.create(hbaseCells.toArray(new org.apache.hadoop.hbase.Cell[hbaseCells.size()]));
  }

  /**
   * Convert a {@link org.apache.hadoop.hbase.client.Result} to a {@link FlatRow}.
   *
   * @param result a {@link org.apache.hadoop.hbase.client.Result} object.
   * @return a {@link FlatRow} object.
   */
  public FlatRow adaptToRow(Result result) {
    FlatRow.Builder rowBuilder = FlatRow.newBuilder();

    // Result.getRow() is derived from its cells.  If the cells are empty, the row will be null.
    if (result.getRow() != null) {
      rowBuilder.withRowKey(ByteStringer.wrap(result.getRow()));
    } else {
      return null;
    }

    final org.apache.hadoop.hbase.Cell[] rawCells = result.rawCells();
    if (rawCells != null && rawCells.length > 0) {
      for (org.apache.hadoop.hbase.Cell rawCell : rawCells) {
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
