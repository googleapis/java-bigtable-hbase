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
package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData;
import com.google.bigtable.anviltop.AnviltopData.RowMutation;
import com.google.bigtable.anviltop.AnviltopData.RowMutation.Mod;
import com.google.bigtable.anviltop.AnviltopData.RowMutation.Mod.SetCell;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Adapt an HBase Put Operation into an Anviltop RowMutation
 */
public class PutAdapter implements OperationAdapter<Put, RowMutation.Builder> {

  public static final ByteString SEPARATOR_BYTE_STRING = ByteString.copyFromUtf8(":");

  @Override
  public RowMutation.Builder adapt(Put operation) {
    RowMutation.Builder result = AnviltopData.RowMutation.newBuilder();
    result.setRowKey(ByteString.copyFrom(operation.getRow()));

    for (Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()) {
      ByteString familyByteString = ByteString.copyFrom(entry.getKey());

      for (Cell cell : entry.getValue()) {
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
                    SEPARATOR_BYTE_STRING,
                    cellQualifierByteString)));

        AnviltopData.Cell.Builder cellBuilder = setCellBuilder.getCellBuilder();

        if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
          long timestampMicros = TimeUnit.MILLISECONDS.toMicros(cell.getTimestamp());
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
