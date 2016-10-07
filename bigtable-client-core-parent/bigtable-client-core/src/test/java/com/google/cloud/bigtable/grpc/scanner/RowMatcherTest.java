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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static com.google.cloud.bigtable.grpc.scanner.RowMatcher.matchesRow;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.protobuf.ByteString;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Basic unit tests for the {@link RowMatcher} utility class.
 */
@RunWith(JUnit4.class)
public class RowMatcherTest {
  @Test
  public void testMatchesBasicRowKey() {
    Row.Builder builder = Row.newBuilder();
    builder.setKey(ByteString.copyFromUtf8("row-1"));
    Row row = builder.build();
    assertThat(row, matchesRow("row-1"));
    assertThat(row, not(matchesRow("row-1").withFamily("some family")));
    assertThat(row, not(matchesRow("row-2")));
  }

  @Test
  public void testMatchesRowKeyMatcher() {
    Row.Builder builder = Row.newBuilder();
    builder.setKey(ByteString.copyFromUtf8("row-1"));
    Row row = builder.build();
    assertThat(row, matchesRow(any(ByteString.class)));
    assertThat(row, matchesRow(equalTo(ByteString.copyFromUtf8("row-1"))));
    // Can't match any row key.
    assertThat(row, not(matchesRow(not(any(ByteString.class)))));
  }

  @Test
  public void testMatchesFamily() {
    Row.Builder builder = Row.newBuilder();
    builder.setKey(ByteString.copyFromUtf8("row-1"));
    Family.Builder family = Family.newBuilder();
    family.setName("some family");
    builder.addFamilies(family);
    Row row = builder.build();
    assertThat(row, not(matchesRow("row-1")));
    assertThat(row, matchesRow("row-1").withFamily("some family"));
    assertThat(row, not(matchesRow("row-1").withFamily("different family")));
    assertThat(row, not(matchesRow("row-2")));
    assertThat(row, not(matchesRow("row-2").withFamily("some family")));
  }

  @Test
  public void testMatchesMultipleFamilies() {
    Row.Builder builder = Row.newBuilder();
    builder.setKey(ByteString.copyFromUtf8("row-1"));
    Family.Builder family = Family.newBuilder();
    family.setName("some family");
    builder.addFamilies(family);

    family.setName("other family");
    builder.addFamilies(family);

    Row row = builder.build();
    // Not enough families expected.
    assertThat(row, not(matchesRow("row-1").withFamily("some family")));
    // Correct.
    assertThat(row, matchesRow("row-1")
        .withFamily("some family")
        .withFamily("other family"));
    // Too many families expected.
    assertThat(row, not(matchesRow("row-1")
        .withFamily("some family")
        .withFamily("other family")
        .withFamily("third family")));
  }

  @Test
  public void testMatchesSingleColumnWithValue() {
    Row.Builder builder = Row.newBuilder();
    builder.setKey(ByteString.copyFromUtf8("row-1"));
    Family.Builder family = Family.newBuilder();
    family.setName("some family");
    Column.Builder column = Column.newBuilder();
    column.setQualifier(ByteString.copyFromUtf8("column"));
    Cell.Builder cell = Cell.newBuilder();
    cell.setValue(ByteString.copyFromUtf8("cell"));
    column.addCells(cell);
    family.addColumns(column);
    builder.addFamilies(family);
    Row row = builder.build();

    // Missing column, cell, etc.
    assertThat(row, not(matchesRow("row-1").withFamily("some family")));
    // Fully specified should match.
    assertThat(row,
        matchesRow("row-1")
            .withFamily("some family")
            .withColumn("column")
            .withCellValue("cell"));
  }

  @Test
  public void testMatchesMultipleColumns() {
    Row.Builder builder = Row.newBuilder();
    builder.setKey(ByteString.copyFromUtf8("row-1"));
    Family.Builder family = Family.newBuilder();
    family.setName("some family");
    Column.Builder column = Column.newBuilder();
    column.setQualifier(ByteString.copyFromUtf8("column"));
    Cell.Builder cell = Cell.newBuilder();
    cell.setValue(ByteString.copyFromUtf8("cell"));
    column.addCells(cell);
    family.addColumns(column);

    column.clearCells();
    cell.setValue(ByteString.copyFromUtf8("other cell"));
    cell.setTimestampMicros(12345);
    column.setQualifier(ByteString.copyFromUtf8("second column"));
    column.addCells(cell);
    family.addColumns(column);

    builder.addFamilies(family);
    Row row = builder.build();

    // Missing column, cell, etc.
    assertThat(row, not(matchesRow("row-1").withFamily("some family")));
    // Missing timestamp for the second column in the matcher.
    assertThat(row,
        not(matchesRow("row-1").withFamily("some family")
            .withColumn("column").withCellValue("cell")
            .withColumn("second column").withCellValue("other cell")));
    // Fully specified should match.
    assertThat(row,
        matchesRow("row-1").withFamily("some family")
            .withColumn("column").withCellValue("cell")
            .withColumn("second column").withCellValue("other cell").atTimestamp(12345));
  }
}
