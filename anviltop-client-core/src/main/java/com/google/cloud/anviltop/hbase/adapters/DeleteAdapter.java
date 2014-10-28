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
import com.google.cloud.anviltop.hbase.AnviltopConstants;
import com.google.common.collect.ImmutableList;
import com.google.cloud.hadoop.hbase.repackaged.protobuf.ByteString;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;

import java.util.List;
import java.util.Map;

/**
 * Adapt a single Delete operation to a Anviltop RowMutation
 */
public class DeleteAdapter implements OperationAdapter<Delete, AnviltopData.RowMutation.Builder> {

  public static final ByteString SEPARATOR_BYTE_STRING =
      ByteString.copyFrom(KeyValue.COLUMN_FAMILY_DELIM_ARRAY);

  static boolean isPointDelete(Cell cell) {
    return cell.getTypeByte() == KeyValue.Type.Delete.getCode();
  }

  static boolean isColumnDelete(Cell cell) {
    return cell.getTypeByte() == KeyValue.Type.DeleteColumn.getCode();
  }

  static boolean isFamilyDelete(Cell cell) {
    return cell.getTypeByte() == KeyValue.Type.DeleteFamily.getCode();
  }

  static boolean isFamilyVersionDelete(Cell cell) {
    return cell.getTypeByte() == KeyValue.Type.DeleteFamilyVersion.getCode();
  }

  static void throwOnUnsupportedCellType(Cell cell) {
    throw new UnsupportedOperationException(
        String.format("Cell type %s is unsupported.", cell.getTypeByte()));
  }

  static void throwOnUnsupportedDeleteFamilyVersion(Cell cell) {
    throw new UnsupportedOperationException(
        "Cannot perform column family deletion at timestamp.");
  }

  static void throwIfUnsupportedDeleteFamily(Cell cell) {
    if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
      // TODO; implement when anviltop service supports deleting a column family before
      // a timestamp.
      throw new UnsupportedOperationException(
          "Cannot perform column family deletion before timestamp.");
    }
  }

  static void throwIfUnsupportedDeleteRow(Delete operation) {
    if (operation.getTimeStamp() != HConstants.LATEST_TIMESTAMP) {
      // TODO: implement when anviltop service supports deleting a row before a timestamp.
      throw new UnsupportedOperationException("Cannot perform row deletion at timestamp.");
    }
  }

  static void throwIfUnsupportedPointDelete(Cell cell) {
    if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
      // TODO: implement when anviltop service supports deleting the single latest version
      // of a cell.
      throw new UnsupportedOperationException("Cannot delete single latest cell.");
    }
  }

  static AnviltopData.RowMutation.Mod.DeleteFromColumn.Builder addDeleteFromColumnMods(
      AnviltopData.RowMutation.Builder result, ByteString familyByteString, Cell cell) {
    AnviltopData.RowMutation.Mod.Builder modBuilder = result.addModsBuilder();
    AnviltopData.RowMutation.Mod.DeleteFromColumn.Builder deleteBuilder =
        modBuilder.getDeleteFromColumnBuilder();

    ByteString cellQualifierByteString = ByteString.copyFrom(
        cell.getQualifierArray(),
        cell.getQualifierOffset(),
        cell.getQualifierLength());

    deleteBuilder.setColumnName(
        ByteString.copyFrom(
            ImmutableList.of(
                familyByteString,
                SEPARATOR_BYTE_STRING,
                cellQualifierByteString)));

    long timestamp = AnviltopConstants.ANVILTOP_TIMEUNIT.convert(
        cell.getTimestamp(),
        AnviltopConstants.HBASE_TIMEUNIT);

    if (isPointDelete(cell)) {
      // Delete a single cell
      deleteBuilder.getTimeRangeBuilder().setStartTimestampMicros(timestamp);
      deleteBuilder.getTimeRangeBuilder().setEndTimestampMicros(timestamp);
    } else {
      // Delete all cells before a timestamp
      if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
        deleteBuilder.getTimeRangeBuilder().setEndTimestampMicros(timestamp);
      }
    }
    return deleteBuilder;
  }

  static AnviltopData.RowMutation.Mod.DeleteFromFamily.Builder addDeleteFromFamilyMods(
      AnviltopData.RowMutation.Builder result, ByteString familyByteString) {
    AnviltopData.RowMutation.Mod.Builder modBuilder = result.addModsBuilder();
    AnviltopData.RowMutation.Mod.DeleteFromFamily.Builder deleteBuilder =
        modBuilder.getDeleteFromFamilyBuilder();
    deleteBuilder.setFamilyNameBytes(familyByteString);
    return deleteBuilder;
  }

  @Override
  public AnviltopData.RowMutation.Builder adapt(Delete operation) {
    AnviltopData.RowMutation.Builder result = AnviltopData.RowMutation.newBuilder();
    result.setRowKey(ByteString.copyFrom(operation.getRow()));

    if (operation.getFamilyCellMap().isEmpty()) {
      throwIfUnsupportedDeleteRow(operation);

      AnviltopData.RowMutation.Mod.Builder modBuilder = result.addModsBuilder();
      modBuilder.getDeleteRowBuilder();
    } else {
      for (Map.Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()) {

        ByteString familyByteString = ByteString.copyFrom(entry.getKey());

        for (Cell cell : entry.getValue()) {
          if (isColumnDelete(cell) || isPointDelete(cell)) {
            if (isPointDelete(cell)) {
              throwIfUnsupportedPointDelete(cell);
            }
            addDeleteFromColumnMods(result, familyByteString, cell);
          } else if (isFamilyDelete(cell)) {
            throwIfUnsupportedDeleteFamily(cell);

            addDeleteFromFamilyMods(result, familyByteString);
          } else if (isFamilyVersionDelete(cell)) {
            throwOnUnsupportedDeleteFamilyVersion(cell);
          } else {
            throwOnUnsupportedCellType(cell);
          }
        }
      }
    }
    return result;
  }
}
