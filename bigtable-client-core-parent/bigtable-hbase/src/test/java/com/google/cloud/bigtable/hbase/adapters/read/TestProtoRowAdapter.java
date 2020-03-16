/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.adapters.read;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestProtoRowAdapter {

  private static final ByteString ROW_KEY = ByteString.copyFromUtf8("test-row");

  private ProtoRowAdapter underTest = new ProtoRowAdapter();

  @Test
  public void testAdapterWithNull() {
    assertNull(underTest.adaptResponse(null).rawCells());
  }

  @Test
  public void testAdapterWithMarkerRow() {
    Row row = Row.newBuilder().setKey(ROW_KEY).build();

    Result actualResult = underTest.adaptResponse(row);
    assertNull(actualResult.getRow());
    assertEquals(0, actualResult.rawCells().length);
  }

  @Test
  public void testAdapterWithRow() {
    String family_1 = "col-family-1";
    String family_2 = "col-family-2";
    String family_3 = "col-family-3";

    ByteString qualifier_1 = ByteString.copyFromUtf8("test-qualifier_1-1");
    ByteString qualifier_2 = ByteString.copyFromUtf8("test-qualifier_1-2");
    ByteString qualifier_3 = ByteString.copyFromUtf8("test-qualifier_1-2");

    ByteString value_1 = ByteString.copyFromUtf8("test-values-1");
    ByteString value_2 = ByteString.copyFromUtf8("test-values-2");
    ByteString value_3 = ByteString.copyFromUtf8("test-values-3");
    ByteString value_4 = ByteString.copyFromUtf8("test-values-4");
    ByteString value_5 = ByteString.copyFromUtf8("test-values-5");
    ByteString ignoredValue = ByteString.copyFromUtf8("test-values-6");

    Row row =
        Row.newBuilder()
            .setKey(ROW_KEY)
            .addFamilies(
                Family.newBuilder()
                    .setName(family_1)
                    .addColumns(
                        Column.newBuilder()
                            .setQualifier(qualifier_1)
                            // First Cell
                            .addCells(
                                Cell.newBuilder().setTimestampMicros(11_111L).setValue(value_1))
                            // Same Cell with another timestamp and value
                            .addCells(
                                Cell.newBuilder().setTimestampMicros(22_222L).setValue(value_2)))
                    .addColumns(
                        Column.newBuilder()
                            .setQualifier(qualifier_2)
                            // With label
                            .addCells(
                                Cell.newBuilder()
                                    .setTimestampMicros(33_333L)
                                    .setValue(ignoredValue)
                                    .addLabels("label"))
                            // Same family, same timestamp, but different column.
                            .addCells(
                                Cell.newBuilder().setTimestampMicros(11_111L).setValue(value_3)))
                    .build())
            .addFamilies(
                Family.newBuilder()
                    .setName(family_2)
                    .addColumns(
                        Column.newBuilder()
                            .setQualifier(qualifier_1)
                            // Same column, same timestamp, but different family.
                            .addCells(
                                Cell.newBuilder().setTimestampMicros(11_111L).setValue(value_4))))
            .addFamilies(
                Family.newBuilder()
                    .setName(family_3)
                    .addColumns(
                        Column.newBuilder()
                            .setQualifier(qualifier_3)
                            // Same timestamp, but different family and column.
                            .addCells(Cell.newBuilder().setValue(value_5))))
            .build();

    Result result = underTest.adaptResponse(row);
    assertEquals(5, result.rawCells().length);

    List<org.apache.hadoop.hbase.Cell> cells1 =
        result.getColumnCells(family_1.getBytes(), qualifier_1.toByteArray());
    assertEquals(2, cells1.size());
    assertEquals(11L, cells1.get(0).getTimestamp());
    assertArrayEquals(value_1.toByteArray(), CellUtil.cloneValue(cells1.get(0)));

    assertEquals(22L, cells1.get(1).getTimestamp());
    assertArrayEquals(value_2.toByteArray(), CellUtil.cloneValue(cells1.get(1)));

    List<org.apache.hadoop.hbase.Cell> cells2 =
        result.getColumnCells(family_1.getBytes(), qualifier_2.toByteArray());
    assertEquals(1, cells2.size());
    assertEquals(11L, cells2.get(0).getTimestamp());
    assertArrayEquals(value_3.toByteArray(), CellUtil.cloneValue(cells2.get(0)));

    List<org.apache.hadoop.hbase.Cell> cells3 =
        result.getColumnCells(family_2.getBytes(), qualifier_1.toByteArray());
    assertEquals(1, cells3.size());
    assertArrayEquals(value_4.toByteArray(), CellUtil.cloneValue(cells3.get(0)));

    List<org.apache.hadoop.hbase.Cell> cells4 =
        result.getColumnCells(family_3.getBytes(), qualifier_3.toByteArray());
    assertEquals(1, cells4.size());
    assertArrayEquals(value_5.toByteArray(), CellUtil.cloneValue(cells4.get(0)));
  }
}
