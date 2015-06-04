/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.Mutation.Builder;
import com.google.bigtable.v1.Mutation.DeleteFromFamily;
import com.google.bigtable.v1.Mutation.DeleteFromRow;
import com.google.cloud.bigtable.config.BigtableConstants;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;

import java.util.List;
import java.util.Map;

/**
 * Adapt a single Delete operation to a Bigtable RowMutation
 */
public class DeleteAdapter implements OperationAdapter<Delete, MutateRowRequest.Builder> {
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
      // TODO; implement when bigtable service supports deleting a column family before
      // a timestamp.
      throw new UnsupportedOperationException(
          "Cannot perform column family deletion before timestamp.");
    }
  }

  static void throwIfUnsupportedDeleteRow(Delete operation) {
    if (operation.getTimeStamp() != HConstants.LATEST_TIMESTAMP) {
      // TODO: implement when bigtable service supports deleting a row before a timestamp.
      throw new UnsupportedOperationException("Cannot perform row deletion at timestamp.");
    }
  }

  static void throwIfUnsupportedPointDelete(Cell cell) {
    if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
      // TODO: implement when bigtable service supports deleting the single latest version
      // of a cell.
      throw new UnsupportedOperationException("Cannot delete single latest cell.");
    }
  }

  static Mutation.DeleteFromColumn.Builder addDeleteFromColumnMods(
      MutateRowRequest.Builder result, ByteString familyByteString, Cell cell) {
    Mutation.Builder modBuilder = result.addMutationsBuilder();
    Mutation.DeleteFromColumn.Builder deleteBuilder =
        modBuilder.getDeleteFromColumnBuilder();

    ByteString cellQualifierByteString = ByteString.copyFrom(
        cell.getQualifierArray(),
        cell.getQualifierOffset(),
        cell.getQualifierLength());

    deleteBuilder.setFamilyNameBytes(familyByteString);
    deleteBuilder.setColumnQualifier(cellQualifierByteString);

    long startTimestamp = BigtableConstants.BIGTABLE_TIMEUNIT.convert(
        cell.getTimestamp(),
        BigtableConstants.HBASE_TIMEUNIT);

    long endTimestamp = BigtableConstants.BIGTABLE_TIMEUNIT.convert(
        cell.getTimestamp() + 1,
        BigtableConstants.HBASE_TIMEUNIT);

    if (isPointDelete(cell)) {
      // Delete a single cell
      deleteBuilder.getTimeRangeBuilder().setStartTimestampMicros(startTimestamp);
      deleteBuilder.getTimeRangeBuilder().setEndTimestampMicros(endTimestamp);
    } else {
      // Delete all cells before a timestamp
      if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
        deleteBuilder.getTimeRangeBuilder().setEndTimestampMicros(endTimestamp);
      }
    }
    return deleteBuilder;
  }

  static Mutation.DeleteFromFamily.Builder addDeleteFromFamilyMods(
      MutateRowRequest.Builder result, ByteString familyByteString) {
    Builder modBuilder = result.addMutationsBuilder();
    DeleteFromFamily.Builder deleteBuilder = modBuilder.getDeleteFromFamilyBuilder();
    deleteBuilder.setFamilyNameBytes(familyByteString);
    return deleteBuilder;
  }

  @Override
  public MutateRowRequest.Builder adapt(Delete operation) {
    MutateRowRequest.Builder result = MutateRowRequest.newBuilder();
    result.setRowKey(ByteString.copyFrom(operation.getRow()));

    if (operation.getFamilyCellMap().isEmpty()) {
      throwIfUnsupportedDeleteRow(operation);

      result.addMutationsBuilder().setDeleteFromRow(DeleteFromRow.getDefaultInstance());
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
