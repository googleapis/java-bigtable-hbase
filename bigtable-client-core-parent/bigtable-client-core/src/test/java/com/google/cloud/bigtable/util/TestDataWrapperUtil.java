/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.util;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestDataWrapperUtil {

  public static final ByteString QUALIFIER_1 = ByteString.copyFromUtf8("qualifier1");
  public static final ByteString QUALIFIER_2 = ByteString.copyFromUtf8("qualifier2");
  public static final int TIMESTAMP = 12345;
  public static final String LABEL = "label";
  public static final List<String> LABEL_LIST = Arrays.asList(LABEL);
  public static final ByteString VALUE = ByteString.copyFromUtf8("test-value");
  public static final ByteString ROW_KEY = ByteString.copyFromUtf8("test-key");

  @Test
  public void testConvertWithOneCell(){
    Cell cell = Cell.newBuilder()
        .setValue(VALUE)
        .setTimestampMicros(TIMESTAMP)
        .addLabels(LABEL)
        .build();
    Row row = Row.newBuilder()
        .setKey(ROW_KEY)
        .addFamilies(Family.newBuilder()
            .setName("firstFamily")
            .addColumns(Column.newBuilder()
                .setQualifier(QUALIFIER_1)
                .addCells(cell)
                .build())
            .build())
        .build();
    RowCell rowCell = RowCell.create("firstFamily", QUALIFIER_1, TIMESTAMP, LABEL_LIST, VALUE);
    com.google.cloud.bigtable.data.v2.models.Row modelRow =
        com.google.cloud.bigtable.data.v2.models.Row.create(row.getKey(), Arrays.asList(rowCell));

    Assert.assertEquals(modelRow, DataWrapperUtil.convert(row));
  }

  @Test
  public void testConvertWithMultipleCell(){
    Row row = Row.newBuilder()
        .setKey(ROW_KEY)
        .addFamilies(Family.newBuilder()
            .setName("firstFamily")
            .addColumns(Column.newBuilder()
                .setQualifier(QUALIFIER_1)
                .addCells(Cell.newBuilder()
                    .setValue(VALUE)
                    .setTimestampMicros(TIMESTAMP)
                    .addLabels(LABEL)
                    .build())
                .build())
            .addColumns(Column.newBuilder()
                .setQualifier(QUALIFIER_2)
                .addCells(Cell.newBuilder()
                        .setValue(VALUE)
                        .setTimestampMicros(TIMESTAMP)
                        .addLabels(LABEL)
                        .build())
                .addCells(Cell.newBuilder()
                    .setValue(VALUE)
                    .setTimestampMicros(54321)
                    .addLabels(LABEL)
                    .build())
                .build())
            .build())
        .addFamilies(Family.newBuilder()
            .setName("secondFamily")
            .addColumns(Column.newBuilder()
                .setQualifier(QUALIFIER_1)
                .addCells(Cell.newBuilder()
                    .setValue(VALUE)
                    .setTimestampMicros(TIMESTAMP)
                    .addLabels(LABEL)
                    .build())
                .build()))
        .build();

    ImmutableList.Builder<RowCell> rowCells = ImmutableList.builder();
    rowCells.add(RowCell.create("firstFamily", QUALIFIER_1, TIMESTAMP, LABEL_LIST, VALUE));
    rowCells.add(RowCell.create("firstFamily", QUALIFIER_2, TIMESTAMP, LABEL_LIST, VALUE));
    rowCells.add(RowCell.create("firstFamily", QUALIFIER_2, 54321, LABEL_LIST, VALUE));
    rowCells.add(RowCell.create("secondFamily", QUALIFIER_1, TIMESTAMP, LABEL_LIST, VALUE));
    com.google.cloud.bigtable.data.v2.models.Row expectedModelRow =
        com.google.cloud.bigtable.data.v2.models.Row.create(ROW_KEY, rowCells.build());

    Assert.assertEquals(expectedModelRow, DataWrapperUtil.convert(row));
  }
}
