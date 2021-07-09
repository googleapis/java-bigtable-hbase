/*
 * Copyright 2016 Google LLC
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
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Adapt between a {@link FlatRow} and an hbase client {@link
 * org.apache.hadoop.hbase.client.Result}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class FlatRowAdapter implements ResponseAdapter<FlatRow, Result> {
  /** {@inheritDoc} Convert a {@link FlatRow} to a {@link Result}. */
  @Override
  public Result adaptResponse(FlatRow flatRow) {
    // flatRow shouldn't ever have a null row key. The second check is defensive only.
    if (flatRow == null || flatRow.getRowKey() == null) {
      return Result.EMPTY_RESULT;
    }
    byte[] RowKey = flatRow.getRowKey().toByteArray();
    List<FlatRow.Cell> cells = flatRow.getCells();
    List<Cell> hbaseCells = new ArrayList<>(cells.size());
    byte[] previousFamilyBytes = null;
    String previousFamily = null;
    for (FlatRow.Cell cell : cells) {
      // Cells with labels are filtered out in BigtableWhileMatchResultScannerAdapter.
      String family = cell.getFamily();
      byte[] familyBytes =
          !Objects.equal(family, previousFamily) ? Bytes.toBytes(family) : previousFamilyBytes;
      hbaseCells.add(toRowCell(RowKey, cell, familyBytes));
      previousFamily = family;
      previousFamilyBytes = familyBytes;
    }
    return Result.create(hbaseCells);
  }

  private static RowCell toRowCell(byte[] rowKey, FlatRow.Cell cell, byte[] family) {
    return new RowCell(
        rowKey,
        family,
        ByteStringer.extract(cell.getQualifier()),
        // Bigtable timestamp has more granularity than HBase one. It is possible that Bigtable
        // cells are deduped unintentionally here. On the other hand, if we don't dedup them,
        // HBase will treat them as duplicates.
        TimestampConverter.bigtable2hbase(cell.getTimestamp()),
        ByteStringer.extract(cell.getValue()),
        cell.getLabels());
  }
}
