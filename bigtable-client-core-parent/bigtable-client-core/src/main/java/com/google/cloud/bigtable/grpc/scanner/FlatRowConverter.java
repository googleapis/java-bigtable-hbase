/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.scanner;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;

/**
 * This class converts between instances of {@link FlatRow} and {@link Row}.
 * @author tyagihas
 * @version $Id: $Id
 */
public class FlatRowConverter {

  public static Row convert(FlatRow row) {
    if (row == null) {
      return null;
    }
    Row.Builder rowBuilder = Row.newBuilder().setKey(row.getRowKey());
    String prevFamily = null;
    Family.Builder familyBuilder = null;
    ByteString previousColumn = null;
    Column.Builder columnBuilder = null;

    for (FlatRow.Cell cell : row.getCells()) {
      final String currentFamily = cell.getFamily();
      if (!currentFamily.equals(prevFamily)) {
        if (familyBuilder != null) {
          if (columnBuilder != null) {
            familyBuilder.addColumns(columnBuilder.build());
            columnBuilder = null;
            previousColumn = null;
          }
          rowBuilder.addFamilies(familyBuilder.build());
        }
        familyBuilder = Family.newBuilder().setName(currentFamily);
        prevFamily = currentFamily;
      }
      ByteString currentQualifier = cell.getQualifier();
      if (!currentQualifier.equals(previousColumn)) {
        if (columnBuilder != null) {
          familyBuilder.addColumns(columnBuilder.build());
        }
        columnBuilder = Column.newBuilder().setQualifier(currentQualifier);
        previousColumn = currentQualifier;
      }

      columnBuilder.addCells(toCell(cell));
    }

    if (familyBuilder != null) {
      if (columnBuilder != null) {
        familyBuilder.addColumns(columnBuilder.build());
      }
      rowBuilder.addFamilies(familyBuilder.build());
    }
    return rowBuilder.build();
  }

  private static Cell toCell(FlatRow.Cell cell) {
    return Cell.newBuilder()
        .setTimestampMicros(cell.getTimestamp())
        .addAllLabels(cell.getLabels())
        .setValue(cell.getValue())
        .build();
  }

  public static FlatRow convert(Row row) {
    FlatRow.Builder builder = FlatRow.newBuilder().withRowKey(row.getKey());
    for (Family family : row.getFamiliesList()) {
      String familyName = family.getName();
      for (Column column : family.getColumnsList()) {
        ByteString qualifier = column.getQualifier();
        for (Cell cell : column.getCellsList()) {
          builder.addCell(familyName, qualifier, cell.getTimestampMicros(), cell.getValue(),
            cell.getLabelsList());
        }
      }
    }
    return builder.build();
  }

  public static com.google.cloud.bigtable.data.v2.models.Row convertToModelRow(FlatRow row) {
    if (row == null) {
      return null;
    }
    List<RowCell> rowCellList = new ArrayList<>();
    for (FlatRow.Cell cell : row.getCells()) {
      rowCellList.add(toRowCell(cell));
    }

    return com.google.cloud.bigtable.data.v2.models.Row.create(row.getRowKey(), rowCellList);
  }

  private static RowCell toRowCell(FlatRow.Cell cell) {
    return RowCell.create(cell.getFamily(), cell.getQualifier(), cell.getTimestamp(),
        cell.getLabels(), cell.getValue());
  }
}
