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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.hbase.BigtableConstants;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Adapt a bigtable.v1.Row to an hbase client Result.
 */
public class RowAdapter implements ResponseAdapter<Row, Result> {
  @Override
  public Result adaptResponse(Row response) {
    if (response == null) {
      return new Result();
    }
    List<org.apache.hadoop.hbase.Cell> hbaseCells = new ArrayList<>();
    byte[] rowKey = response.getKey().toByteArray();

    for (Family family : response.getFamiliesList()) {
      byte[] familyNameBytes = Bytes.toBytes(family.getName());

      for (Column column : family.getColumnsList()) {
        byte[] columnQualifier = column.getQualifier().toByteArray();

        for (Cell cell : column.getCellsList()) {
          long hbaseTimestamp =
              BigtableConstants.HBASE_TIMEUNIT.convert(
                  cell.getTimestampMicros(), BigtableConstants.BIGTABLE_TIMEUNIT);
          KeyValue keyValue = new KeyValue(
              rowKey,
              familyNameBytes,
              columnQualifier,
              hbaseTimestamp,
              cell.getValue().toByteArray());

          hbaseCells.add(keyValue);
        }
      }
    }

    Collections.sort(hbaseCells, KeyValue.COMPARATOR);

    return Result.create(hbaseCells);
  }
}
