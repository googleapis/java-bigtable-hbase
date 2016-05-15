/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.protobuf.BigtableZeroCopyByteStringUtil;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Adapt a bigtable.v1.Row to an hbase client Result.
 */
public class RowAdapter implements ResponseAdapter<Row, Result> {
  // This only works because BIGTABLE_TIMEUNIT is smaller than HBASE_TIMEUNIT, otherwise we will get
  // 0.
  static final long TIME_CONVERSION_UNIT = BigtableConstants.BIGTABLE_TIMEUNIT.convert(1,
    BigtableConstants.HBASE_TIMEUNIT);

  @Override
  public Result adaptResponse(Row response) {
    if (response == null) {
      return new Result();
    }

    SortedSet<org.apache.hadoop.hbase.Cell> hbaseCells = new TreeSet<>(KeyValue.COMPARATOR);
    byte[] rowKey = ByteStringer.extract(response.getKey());

    for (Family family : response.getFamiliesList()) {
      byte[] familyNameBytes = Bytes.toBytes(family.getName());

      for (Column column : family.getColumnsList()) {
        byte[] columnQualifier = ByteStringer.extract(column.getQualifier());

        for (Cell cell : column.getCellsList()) {
          // Cells with labels are for internal use, do not return them.
          // TODO(kevinsi4508): Filter out targeted {@link WhileMatchFilter} labels.
          if (cell.getLabelsCount() > 0) {
            continue;
          }

          // Bigtable timestamp has more granularity than HBase one. It is possible that Bigtable
          // cells are deduped unintentionally here. On the other hand, if we don't dedup them,
          // HBase will treat them as duplicates.
          long hbaseTimestamp = cell.getTimestampMicros() / TIME_CONVERSION_UNIT;
          RowCell keyValue = new RowCell(
              rowKey,
              familyNameBytes,
              columnQualifier,
              hbaseTimestamp,
              ByteStringer.extract(cell.getValue()));

          hbaseCells.add(keyValue);
        }
      }
    }

    return Result.create(hbaseCells.toArray(new org.apache.hadoop.hbase.Cell[hbaseCells.size()]));
  }

  public Row adaptRow(Result result) {
    Row.Builder rowBuilder =
        Row.newBuilder().setKey(BigtableZeroCopyByteStringUtil.wrap(result.getRow()));
    for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : result
        .getMap().entrySet()) {
      Family.Builder familyBuilder =
          rowBuilder.addFamiliesBuilder().setName(Bytes.toString(familyEntry.getKey()));
      for (Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : familyEntry.getValue()
          .entrySet()) {
        ByteString qualifier = BigtableZeroCopyByteStringUtil.wrap(columnEntry.getKey());
        Column.Builder columnBuilder = familyBuilder.addColumnsBuilder().setQualifier(qualifier);
        for (Entry<Long, byte[]> cellData : columnEntry.getValue().entrySet()) {
          columnBuilder.addCellsBuilder()
              .setTimestampMicros(cellData.getKey().longValue() * TIME_CONVERSION_UNIT)
              .setValue(BigtableZeroCopyByteStringUtil.wrap(cellData.getValue()));
        }
      }
    }
    return rowBuilder.build();
  }
}
