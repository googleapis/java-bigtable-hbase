/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData;
import com.google.bigtable.anviltop.AnviltopData.RowMutation;
import com.google.bigtable.anviltop.AnviltopData.RowMutation.Mod;
import com.google.bigtable.anviltop.AnviltopData.RowMutation.Mod.SetCell;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;
import java.util.Map.Entry;

/**
 * Adapt an HBase Put Operation into an Anviltop RowMutation
 */
public class PutAdapter implements OperationAdapter<Put, RowMutation.Builder> {
  private int maxKeyValueSize;

  public PutAdapter(Configuration configuration) {
    maxKeyValueSize = configuration.getInt("hbase.client.keyvalue.maxsize", -1);
  }

  @Override
  public RowMutation.Builder adapt(Put operation) {
    RowMutation.Builder result = AnviltopData.RowMutation.newBuilder();
    result.setRowKey(ByteString.copyFrom(operation.getRow()));

    if (operation.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }

    for (Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()) {
      ByteString familyByteString = ByteString.copyFrom(entry.getKey());

      for (Cell cell : entry.getValue()) {
        // Since we are not using the interface involving KeyValues, we reconstruct how big they would be.
        // 20 bytes for metadata plus the length of all the elements.
        int keyValueSize = (20 +
            cell.getRowLength() +
            cell.getFamilyLength() +
            cell.getQualifierLength() +
            cell.getValueLength());
        if (maxKeyValueSize > 0 && keyValueSize > maxKeyValueSize) {
          throw new IllegalArgumentException("KeyValue size too large");
        }
        Mod.Builder modBuilder = result.addModsBuilder();
        SetCell.Builder setCellBuilder = modBuilder.getSetCellBuilder();

        ByteString cellQualifierByteString = ByteString.copyFrom(
            cell.getQualifierArray(),
            cell.getQualifierOffset(),
            cell.getQualifierLength());

        setCellBuilder.setColumnName(
            ByteString.copyFrom(
                ImmutableList.of(
                    familyByteString,
                    BigtableConstants.BIGTABLE_COLUMN_SEPARATOR_BYTE_STRING,
                    cellQualifierByteString)));

        AnviltopData.Cell.Builder cellBuilder = setCellBuilder.getCellBuilder();

        if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
          long timestampMicros = BigtableConstants.BIGTABLE_TIMEUNIT.convert(
              cell.getTimestamp(),
              BigtableConstants.HBASE_TIMEUNIT);
          cellBuilder.setTimestampMicros(timestampMicros);
        }

        cellBuilder.setValue(
            ByteString.copyFrom(
                cell.getValueArray(),
                cell.getValueOffset(),
                cell.getValueLength()));
      }
    }

    return result;
  }
}
