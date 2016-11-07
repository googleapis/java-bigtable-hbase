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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.protobuf.ByteString;

@RunWith(JUnit4.class)
public class TestFlatRowConverter {

  private static Cell asProtoCell(FlatRow.Cell simpleCell) {
    return Cell.newBuilder()
      .setValue(simpleCell.getValue())
      .setTimestampMicros(simpleCell.getTimestamp())
      .build();
  }

  @Test
  public void testOneCell() {
    FlatRow simpleRow = FlatRow.newBuilder()
        .withRowKey(toByteString("key"))
        .addCell("family", toByteString("column"), 500, toByteString("value"), null)
        .build();
    FlatRow.Cell simpleCell = simpleRow.getCells().get(0);

    Row expectedRow = Row.newBuilder()
        .setKey(simpleRow.getRowKey())
        .addFamilies(Family.newBuilder()
          .setName(simpleCell.getFamily())
          .addColumns(Column.newBuilder()
            .setQualifier(simpleCell.getQualifier())
            .addCells(asProtoCell(simpleCell))
            .build())
          .build())
        .build();
    testBothWays(simpleRow, expectedRow);
  }

  @Test
  public void testManyCells() {
    FlatRow simpleRow = FlatRow.newBuilder()
        .withRowKey(toByteString("key"))
        .addCell("family1", toByteString("column"), 500, toByteString("value"), null)
        .addCell("family1", toByteString("column2"), 500, toByteString("value"), null)
        .addCell("family1", toByteString("column2"), 400, toByteString("value"), null)
        .addCell("family2", toByteString("column"), 500, toByteString("value"), null)
        .build();
    FlatRow.Cell simpleCell0 = simpleRow.getCells().get(0);
    FlatRow.Cell simpleCell1 = simpleRow.getCells().get(1);
    FlatRow.Cell simpleCell2 = simpleRow.getCells().get(2);
    FlatRow.Cell simpleCell3 = simpleRow.getCells().get(3);

    Row expectedRow = Row.newBuilder()
        .setKey(simpleRow.getRowKey())
        .addFamilies(Family.newBuilder()
          .setName(simpleCell0.getFamily())
          .addColumns(Column.newBuilder()
            .setQualifier(simpleCell0.getQualifier())
            .addCells(asProtoCell(simpleCell0))
            .build())
          .addColumns(Column.newBuilder()
            .setQualifier(simpleCell1.getQualifier())
            .addCells(asProtoCell(simpleCell1))
            .addCells(asProtoCell(simpleCell2))
            .build())
          .build())
        .addFamilies(Family.newBuilder()
          .setName(simpleCell3.getFamily())
          .addColumns(Column.newBuilder()
            .setQualifier(simpleCell3.getQualifier())
            .addCells(asProtoCell(simpleCell3))
            .build()))
        .build();
    testBothWays(simpleRow, expectedRow);
  }

  private void testBothWays(FlatRow simpleRow, Row row) {
    Assert.assertEquals(row, FlatRowConverter.convert(simpleRow));
    Assert.assertEquals(simpleRow, FlatRowConverter.convert(row));
  }

  private ByteString toByteString(final String string) {
    return ByteString.copyFrom(string.getBytes());
  }
}
