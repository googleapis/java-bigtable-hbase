/*
 * Copyright 2019 Google LLC.
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

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.common.collect.ImmutableList;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Adapt between a {@link Row} and an hbase client {@link Result}.
 */
public class ModelRowAdapter implements ResponseAdapter<Row, Result> {

  @Override
  public Result adaptResponse(Row response) {
    if (response == null) {
      return new Result();
    }

    ImmutableList.Builder<Cell> hbaseCellBuilder = ImmutableList.builder();
    byte[] rowKey = ByteStringer.extract(response.getKey());
    for (com.google.cloud.bigtable.data.v2.models.RowCell rowCell : response.getCells()) {
      // Cells with labels are for internal use, do not return them.
      // TODO(kevinsi4508): Filter out targeted {@link WhileMatchFilter} labels.
      if (rowCell.getLabels().size() > 0) {
        continue;
      }
      byte[] familyNameBytes = Bytes.toBytes(rowCell.getFamily());
      byte[] columnQualifier = ByteStringer.extract(rowCell.getQualifier());
      long hbaseTimestamp = TimestampConverter.bigtable2hbase(rowCell.getTimestamp());
      RowCell keyValue = new RowCell(
          rowKey,
          familyNameBytes,
          columnQualifier,
          hbaseTimestamp,
          ByteStringer.extract(rowCell.getValue()));

      hbaseCellBuilder.add(keyValue);
    }

    ImmutableList<Cell> hbaseCells = hbaseCellBuilder.build();
    return Result.create(hbaseCells.toArray(new Cell[hbaseCells.size()]));
  }
}
