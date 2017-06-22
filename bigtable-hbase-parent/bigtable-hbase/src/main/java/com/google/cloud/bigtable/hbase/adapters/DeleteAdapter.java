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

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.DeleteFromColumn;
import com.google.bigtable.v2.Mutation.DeleteFromFamily;
import com.google.bigtable.v2.Mutation.DeleteFromRow;
import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Adapt a single Delete operation to a Bigtable RowMutation
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class DeleteAdapter extends MutationAdapter<Delete> {
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

  static void throwOnUnsupportedDeleteFamilyVersion(@SuppressWarnings("unused") Cell cell) {
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

  static Mutation.DeleteFromColumn addDeleteFromColumnMods(ByteString familyByteString, Cell cell) {

    ByteString cellQualifierByteString = ByteString.copyFrom(
        cell.getQualifierArray(),
        cell.getQualifierOffset(),
        cell.getQualifierLength());

    Mutation.DeleteFromColumn.Builder deleteBuilder =
        Mutation.DeleteFromColumn.newBuilder()
          .setFamilyNameBytes(familyByteString)
          .setColumnQualifier(cellQualifierByteString);

    long endTimestamp = BigtableConstants.BIGTABLE_TIMEUNIT.convert(
        cell.getTimestamp() + 1,
        BigtableConstants.HBASE_TIMEUNIT);

    if (isPointDelete(cell)) {
      // Delete a single cell
      long startTimestamp = BigtableConstants.BIGTABLE_TIMEUNIT.convert(
        cell.getTimestamp(),
        BigtableConstants.HBASE_TIMEUNIT);

      deleteBuilder.getTimeRangeBuilder()
          .setStartTimestampMicros(startTimestamp)
          .setEndTimestampMicros(endTimestamp);
    } else {
      // Delete all cells before a timestamp
      if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
        deleteBuilder.getTimeRangeBuilder().setEndTimestampMicros(endTimestamp);
      }
    }
    return deleteBuilder.build();
  }

  @Override
  /** {@inheritDoc} */
  protected Collection<Mutation> adaptMutations(Delete operation) {
    List<Mutation> mutations = new ArrayList<>();
    if (operation.getFamilyCellMap().isEmpty()) {
      throwIfUnsupportedDeleteRow(operation);

      mutations
          .add(Mutation.newBuilder().setDeleteFromRow(DeleteFromRow.getDefaultInstance()).build());
    } else {
      for (Map.Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()) {

        ByteString familyByteString = ByteString.copyFrom(entry.getKey());

        for (Cell cell : entry.getValue()) {
          if (isColumnDelete(cell) || isPointDelete(cell)) {
            if (isPointDelete(cell)) {
              throwIfUnsupportedPointDelete(cell);
            }
            mutations.add(Mutation.newBuilder()
              .setDeleteFromColumn(addDeleteFromColumnMods(familyByteString, cell))
              .build());
          } else if (isFamilyDelete(cell)) {
            throwIfUnsupportedDeleteFamily(cell);

            mutations.add(Mutation.newBuilder()
              .setDeleteFromFamily(DeleteFromFamily.newBuilder()
                  .setFamilyNameBytes(familyByteString).build())
              .build());
          } else if (isFamilyVersionDelete(cell)) {
            throwOnUnsupportedDeleteFamilyVersion(cell);
          } else {
            throwOnUnsupportedCellType(cell);
          }
        }
      }
    }
    return mutations;
  }


  /**
   * <p>adapt.</p>
   *
   * @param request a {@link com.google.bigtable.v2.MutateRowRequest} object.
   * @return a {@link org.apache.hadoop.hbase.client.Delete} object.
   */
  public Delete adapt(MutateRowRequest request) {
    Delete delete = new Delete(request.getRowKey().toByteArray());

    boolean isDeleteRow = false;
    for (Mutation mutation : request.getMutationsList()) {
      switch (mutation.getMutationCase()) {
      case DELETE_FROM_COLUMN: {
        DeleteFromColumn deleteFromColumn = mutation.getDeleteFromColumn();
        long timestamp;
        TimestampRange timeRange = deleteFromColumn.getTimeRange();
        if (timeRange.getStartTimestampMicros() == 0) {
          timestamp = BigtableConstants.HBASE_TIMEUNIT.convert(timeRange.getEndTimestampMicros(),
            BigtableConstants.BIGTABLE_TIMEUNIT) - 1;
          delete.addColumns(getBytes(deleteFromColumn.getFamilyNameBytes()),
            getBytes(deleteFromColumn.getColumnQualifier()), timestamp);
        } else {
          timestamp = BigtableConstants.HBASE_TIMEUNIT.convert(timeRange.getStartTimestampMicros(),
            BigtableConstants.BIGTABLE_TIMEUNIT);
          delete.addColumn(getBytes(deleteFromColumn.getFamilyNameBytes()),
            getBytes(deleteFromColumn.getColumnQualifier()), timestamp);
        }

        break;
      }
      case DELETE_FROM_FAMILY:
        delete.addFamily(getBytes(mutation.getDeleteFromFamily().getFamilyNameBytes()));
        break;
      case DELETE_FROM_ROW:
        isDeleteRow = true;
        break;
      default:
        throw new IllegalArgumentException("DeleteAdapter does not support " + mutation.getMutationCase() + ".");
      }
    }
    if (isDeleteRow && !delete.getFamilyCellMap().isEmpty()) {
      throw new IllegalArgumentException(
          "DeleteAdapter does not support DELETE_FROM_ROW with other operations.");
    }
    return delete;
  }
}
