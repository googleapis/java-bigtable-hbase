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

import static com.google.cloud.bigtable.mirroring.core.utils.timestamper.TestInPlaceTimestamper.getCell;
import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestCopyingTimestamper {
  @Test
  public void testFillingPutTimestamps() throws IOException {
    Put inputPut = new Put("row".getBytes(StandardCharsets.UTF_8));
    inputPut.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());
    inputPut.addColumn("f1".getBytes(), "q2".getBytes(), 123L, "v".getBytes());
    inputPut.addImmutable("f2".getBytes(), "q1".getBytes(), "v".getBytes());
    inputPut.addImmutable("f2".getBytes(), "q2".getBytes(), 123L, "v".getBytes());

    long timestampBefore = System.currentTimeMillis();
    Put resultPut = new CopyingTimestamper().fillTimestamp(inputPut);
    long timestampAfter = System.currentTimeMillis();

    // Input is not modified
    assertThat(getCell(inputPut, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputPut, 1).getTimestamp()).isEqualTo(123L);
    assertThat(getCell(inputPut, 2).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputPut, 3).getTimestamp()).isEqualTo(123L);

    // Result has assigned timestamps
    assertThat(getCell(resultPut, 0).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(resultPut, 0).getTimestamp()).isAtMost(timestampAfter);

    assertThat(getCell(resultPut, 1).getTimestamp()).isEqualTo(123L);

    assertThat(getCell(resultPut, 2).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(resultPut, 2).getTimestamp()).isAtMost(timestampAfter);

    assertThat(resultPut.get("f2".getBytes(), "q2".getBytes()).get(0).getTimestamp())
        .isEqualTo(123L);
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
    RowMutations rm = new CopyingTimestamper().fillTimestamp(inputRowMutations);
    long timestampAfter = System.currentTimeMillis();

    // Input is not modified.
    assertThat(getCell(inputRowMutations, 0, 0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputRowMutations, 0, 1).getTimestamp()).isEqualTo(123L);

    assertThat(getCell(inputRowMutations, 1, 0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputRowMutations, 1, 1).getTimestamp()).isEqualTo(123L);

    // Result has assigned timestamps.
    assertThat(getCell(rm, 0, 0).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(rm, 0, 0).getTimestamp()).isAtMost(timestampAfter);
    assertThat(getCell(rm, 0, 1).getTimestamp()).isEqualTo(123L);

    assertThat(getCell(rm, 1, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(rm, 1, 1).getTimestamp()).isEqualTo(123L);
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
    Put inputRowMutationsPut = new Put("row".getBytes());
    inputRowMutationsPut.addColumn("f1".getBytes(), "q2".getBytes(), "v".getBytes());
    inputRowMutations.add(inputRowMutationsPut);

    Delete inputRowMutationsDelete = new Delete("row".getBytes());
    inputRowMutationsDelete.addColumn("f1".getBytes(), "q2".getBytes());
    inputRowMutations.add(inputRowMutationsDelete);

    long timestampBefore = System.currentTimeMillis();
    List<Row> result =
        new CopyingTimestamper()
            .fillTimestamp(
                Arrays.asList(
                    inputPut, inputDelete, inputIncrement, inputAppend, inputRowMutations));
    long timestampAfter = System.currentTimeMillis();

    // Input is not modified
    assertThat(getCell(inputPut, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputDelete, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputIncrement, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputAppend, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputRowMutations, 0, 0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(inputRowMutations, 1, 0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);

    // Result has assigned timestamps
    Put resultPut = (Put) result.get(0);
    Delete resultDelete = (Delete) result.get(1);
    Increment resultIncrement = (Increment) result.get(2);
    Append resultAppend = (Append) result.get(3);
    RowMutations resultRowMutations = (RowMutations) result.get(4);

    assertThat(getCell(resultPut, 0).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(resultPut, 0).getTimestamp()).isAtMost(timestampAfter);
    assertThat(getCell(resultDelete, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(resultIncrement, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(resultAppend, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(resultRowMutations, 0, 0).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(resultRowMutations, 0, 0).getTimestamp()).isAtMost(timestampAfter);
    assertThat(getCell(resultRowMutations, 1, 0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);
  }

  @Test
  public void testFillingListOfRowMutations() throws IOException {
    Put p = new Put("row".getBytes(StandardCharsets.UTF_8));
    p.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());

    Delete d = new Delete("row".getBytes(StandardCharsets.UTF_8));
    d.addColumn("f1".getBytes(), "q1".getBytes());

    RowMutations rm = new RowMutations("row".getBytes());
    rm.add(p);
    rm.add(d);

    long timestampBefore = System.currentTimeMillis();
    List<RowMutations> result = new CopyingTimestamper().fillTimestamp(Arrays.asList(rm));
    long timestampAfter = System.currentTimeMillis();

    RowMutations resultRowMutations = result.get(0);

    assertThat(getCell(rm, 0, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(getCell(rm, 1, 0).getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);

    assertThat(getCell(resultRowMutations, 0, 0).getTimestamp()).isAtLeast(timestampBefore);
    assertThat(getCell(resultRowMutations, 0, 0).getTimestamp()).isAtMost(timestampAfter);

    assertThat(getCell(resultRowMutations, 1, 0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);
  }
}
