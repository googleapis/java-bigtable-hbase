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
package com.google.cloud.bigtable.hbase.adapters;

import static com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;

/**
 * Adapt a single Delete operation to a Google Cloud Java {@link
 * com.google.cloud.bigtable.data.v2.models.MutationApi}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
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

  static void throwOnUnsupportedDeleteFamilyVersion(Cell cell) {
    throw new UnsupportedOperationException("Cannot perform column family deletion at timestamp.");
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

  static void addDeleteFromColumnMods(
      ByteString familyByteString,
      Cell cell,
      com.google.cloud.bigtable.data.v2.models.MutationApi<?> mutation) {

    ByteString cellQualifierByteString =
        ByteString.copyFrom(
            cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

    long endTimestamp = TimestampConverter.hbase2bigtable(cell.getTimestamp() + 1);

    if (isPointDelete(cell)) {
      // Delete a single cell
      long startTimestamp = TimestampConverter.hbase2bigtable(cell.getTimestamp());
      mutation.deleteCells(
          familyByteString.toStringUtf8(),
          cellQualifierByteString,
          TimestampRange.create(startTimestamp, endTimestamp));
    } else {
      // Delete all cells before a timestamp
      if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
        mutation.deleteCells(
            familyByteString.toStringUtf8(),
            cellQualifierByteString,
            TimestampRange.unbounded().endOpen(endTimestamp));
      } else {
        mutation.deleteCells(familyByteString.toStringUtf8(), cellQualifierByteString);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void adapt(
      Delete operation, com.google.cloud.bigtable.data.v2.models.MutationApi<?> mutation) {
    if (operation.getFamilyCellMap().isEmpty()) {
      throwIfUnsupportedDeleteRow(operation);

      mutation.deleteRow();
    } else {
      for (Map.Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()) {

        ByteString familyByteString = ByteString.copyFrom(entry.getKey());

        for (Cell cell : entry.getValue()) {
          if (isColumnDelete(cell) || isPointDelete(cell)) {
            if (isPointDelete(cell)) {
              throwIfUnsupportedPointDelete(cell);
            }
            addDeleteFromColumnMods(familyByteString, cell, mutation);
          } else if (isFamilyDelete(cell)) {
            throwIfUnsupportedDeleteFamily(cell);

            mutation.deleteFamily(familyByteString.toStringUtf8());
          } else if (isFamilyVersionDelete(cell)) {
            throwOnUnsupportedDeleteFamilyVersion(cell);
          } else {
            throwOnUnsupportedCellType(cell);
          }
        }
      }
    }
  }
}
