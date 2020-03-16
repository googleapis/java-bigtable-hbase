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
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;

/**
 * Adapt between a {@link Row} and an hbase client {@link Result}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ProtoRowAdapter implements ResponseAdapter<Row, Result> {

  @Override
  public Result adaptResponse(Row row) {
    if (row == null) {
      return Result.EMPTY_RESULT;
    }

    List<org.apache.hadoop.hbase.Cell> hbaseCells = new ArrayList<>();
    byte[] rowKeyBytes = ByteStringer.extract(row.getKey());

    for (Family family : row.getFamiliesList()) {
      byte[] familyBytes = ByteStringer.extract(family.getNameBytes());

      for (Column column : family.getColumnsList()) {
        byte[] qualifierBytes = ByteStringer.extract(column.getQualifier());

        for (Cell cell : column.getCellsList()) {
          // Cells with labels are treated as internal cells, not sending them to user.
          if (cell.getLabelsList().isEmpty()) {
            hbaseCells.add(toRowCell(rowKeyBytes, familyBytes, qualifierBytes, cell));
          }
        }
      }
    }
    return Result.create(hbaseCells);
  }

  private static RowCell toRowCell(byte[] rowKey, byte[] family, byte[] qualifier, Cell cell) {
    return new RowCell(
        rowKey,
        family,
        qualifier,
        // Bigtable timestamp has more granularity than HBase one. It is possible that Bigtable
        // cells are deduped unintentionally here. On the other hand, if we don't dedup them,
        // HBase will treat them as duplicates.
        TimestampConverter.bigtable2hbase(cell.getTimestampMicros()),
        ByteStringer.extract(cell.getValue()),
        cell.getLabelsList());
  }
}
