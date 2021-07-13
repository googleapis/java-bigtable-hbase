/*
 * Copyright 2015 Google LLC
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
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Adapt between a {@link Row} and an hbase client {@link Result}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class RowAdapter implements ResponseAdapter<Row, Result> {
  /**
   * {@inheritDoc}
   *
   * <p>Convert a {@link Row} to a {@link Result}.
   */
  @Override
  public Result adaptResponse(Row response) {
    if (response == null) {
      return Result.EMPTY_RESULT;
    }

    SortedSet<Cell> hbaseCells = new TreeSet<>(KeyValue.COMPARATOR);
    byte[] rowKey = response.getKey().toByteArray();

    for (com.google.cloud.bigtable.data.v2.models.RowCell rowCell : response.getCells()) {

      byte[] familyNameBytes = Bytes.toBytes(rowCell.getFamily());
      byte[] columnQualifier = ByteStringer.extract(rowCell.getQualifier());

      // Bigtable timestamp has more granularity than HBase one. It is possible that Bigtable
      // cells are deduped unintentionally here. On the other hand, if we don't dedup them,
      // HBase will treat them as duplicates.
      long hbaseTimestamp = TimestampConverter.bigtable2hbase(rowCell.getTimestamp());
      RowCell keyValue =
          new RowCell(
              rowKey,
              familyNameBytes,
              columnQualifier,
              hbaseTimestamp,
              ByteStringer.extract(rowCell.getValue()));

      hbaseCells.add(keyValue);
    }

    return Result.create(hbaseCells.toArray(new Cell[hbaseCells.size()]));
  }
}
