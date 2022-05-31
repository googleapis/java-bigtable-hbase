/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.core.utils.timestamper;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestInPlaceTimestamper {
  @Test
  public void testFillingPutTimestamps() throws IOException {
    Put inputPut = new Put("row".getBytes(StandardCharsets.UTF_8));
    inputPut.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());
    inputPut.addColumn("f1".getBytes(), "q2".getBytes(), 123L, "v".getBytes());
    inputPut.addImmutable("f2".getBytes(), "q1".getBytes(), "v".getBytes());
    inputPut.addImmutable("f2".getBytes(), "q2".getBytes(), 123L, "v".getBytes());

    long timestampBefore = System.currentTimeMillis();
    Put outputPut = new InPlaceTimestamper().fillTimestamp(inputPut);
    long timestampAfter = System.currentTimeMillis();

    assertRowEquals(outputPut, inputPut);

    assertThat(getCell(inputPut, 0).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(inputPut, 0).getTimestamp()).isAtMost(timestampAfter);

    assertThat(getCell(inputPut, 1).getTimestamp()).isEqualTo(123L);

    assertThat(getCell(inputPut, 2).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(inputPut, 2).getTimestamp()).isAtMost(timestampAfter);

    assertThat(getCell(inputPut, 3).getTimestamp()).isEqualTo(123L);
  }

  @Test
  public void testFillingRowMutationsTimestamps() throws IOException {
    RowMutations inputRowMutations = new RowMutations("row".getBytes());
    Put inputPut = new Put("row".getBytes(StandardCharsets.UTF_8));
    inputPut.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());
    inputPut.addColumn("f1".getBytes(), "q2".getBytes(), 123L, "v".getBytes());
    inputRowMutations.add(inputPut);

    Delete inputDelete = new Delete("row".getBytes(StandardCharsets.UTF_8));
    inputDelete.addColumn("f1".getBytes(), "q1".getBytes());
    inputDelete.addColumn("f1".getBytes(), "q2".getBytes(), 123L);
    inputRowMutations.add(inputDelete);

    long timestampBefore = System.currentTimeMillis();
    RowMutations result = new InPlaceTimestamper().fillTimestamp(inputRowMutations);
    long timestampAfter = System.currentTimeMillis();

    assertRowEquals(inputRowMutations, result);

    assertThat(getCell(inputRowMutations, 0, 0).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(inputRowMutations, 0, 0).getTimestamp()).isAtMost(timestampAfter);
    assertThat(getCell(inputRowMutations, 0, 1).getTimestamp()).isEqualTo(123L);

    assertThat(getCell(inputRowMutations, 1, 0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputRowMutations, 1, 1).getTimestamp()).isEqualTo(123L);
  }

  @Test
  public void testFillingListOfMutations() throws IOException {
    Put inputPut = new Put("row".getBytes(StandardCharsets.UTF_8));
    inputPut.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());

    Delete inputDelete = new Delete("row".getBytes(StandardCharsets.UTF_8));
    inputDelete.addColumn("f1".getBytes(), "q1".getBytes());

    Increment inputIncrement = new Increment("row".getBytes(StandardCharsets.UTF_8));
    inputIncrement.addColumn("f1".getBytes(), "q1".getBytes(), 1);

    Append inputAppend = new Append("row".getBytes(StandardCharsets.UTF_8));
    inputAppend.add("f1".getBytes(), "q1".getBytes(), "v".getBytes());

    RowMutations inputRowMutations = new RowMutations("row".getBytes());

    List<Row> list =
        Arrays.asList(inputPut, inputDelete, inputIncrement, inputAppend, inputRowMutations);

    long timestampBefore = System.currentTimeMillis();
    List<Row> result = new InPlaceTimestamper().fillTimestamp(list);
    long timestampAfter = System.currentTimeMillis();

    assertRecursiveEquals(list, result);

    assertThat(getCell(inputPut, 0).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(inputPut, 0).getTimestamp()).isAtMost(timestampAfter);

    assertThat(getCell(inputDelete, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputIncrement, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputAppend, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
  }

  @Test
  public void testFillingListOfRowMutations() throws IOException {
    Put inputPut = new Put("row".getBytes(StandardCharsets.UTF_8));
    inputPut.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());

    Delete inputDelete = new Delete("row".getBytes(StandardCharsets.UTF_8));
    inputDelete.addColumn("f1".getBytes(), "q1".getBytes());

    RowMutations inputRowMutations = new RowMutations("row".getBytes());
    inputRowMutations.add(inputPut);
    inputRowMutations.add(inputDelete);

    List<RowMutations> input = Arrays.asList(inputRowMutations);

    long timestampBefore = System.currentTimeMillis();
    List<RowMutations> result = new InPlaceTimestamper().fillTimestamp(input);
    long timestampAfter = System.currentTimeMillis();

    assertThat(result).isEqualTo(input);
    assertRowMutationsEqual(result.get(0), input.get(0));

    assertThat(getCell(inputPut, 0).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(inputPut, 0).getTimestamp()).isAtMost(timestampAfter);
  }

  private void assertRecursiveEquals(List<Row> list, List<Row> result) {
    assertThat(list).isEqualTo(result);
    for (int i = 0; i < list.size(); i++) {
      assertRowEquals(list.get(0), result.get(0));
    }
  }

  private void assertRowEquals(Row row, Row row1) {
    assertThat(row).isEqualTo(row1);
    if (row instanceof RowMutations) {
      assertRowMutationsEqual((RowMutations) row, (RowMutations) row1);
    }
  }

  private void assertRowMutationsEqual(RowMutations rm, RowMutations result) {
    assertThat(rm).isEqualTo(result);
    assertThat(rm.getMutations()).isEqualTo(result.getMutations());
    for (int i = 0; i < rm.getMutations().size(); i++) {
      assertThat(rm.getMutations().get(i)).isEqualTo(result.getMutations().get(i));
    }
  }

  public static Cell getCell(Mutation m, int id) throws IOException {
    CellScanner cs = m.cellScanner();
    assertThat(cs.advance()).isTrue();
    for (int i = 0; i < id; i++) {
      assertThat(cs.advance()).isTrue();
    }
    return cs.current();
  }

  public static Cell getCell(RowMutations m, int mutationId, int id) throws IOException {
    CellScanner cs = m.getMutations().get(mutationId).cellScanner();
    assertThat(cs.advance()).isTrue();
    for (int i = 0; i < id; i++) {
      assertThat(cs.advance()).isTrue();
    }
    return cs.current();
  }
}
