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
package com.google.cloud.bigtable.mirroring.core.utils;

import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createCell;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createResult;
import static com.google.cloud.bigtable.mirroring.core.utils.Comparators.resultsEqual;
import static com.google.common.truth.Truth.assertThat;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestResultComparator {
  @Test
  public void testEqualResults() {
    assertThat(
            resultsEqual(
                createResult(
                    createCell("r1", "f1", "q1", 1, Type.Put, "v1"),
                    createCell("r2", "f2", "q2", 2, Type.Put, "v2")),
                createResult(
                    createCell("r1", "f1", "q1", 1, Type.Put, "v1"),
                    createCell("r2", "f2", "q2", 2, Type.Put, "v2"))))
        .isTrue();

    assertThat(
            resultsEqual(
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v1")),
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v1"))))
        .isTrue();
  }

  @Test
  public void testNotMatchingLengths() {
    assertThat(
            resultsEqual(
                createResult(
                    createCell("r1", "f1", "q1", 1, Type.Put, "v1"),
                    createCell("r2", "f2", "q2", 2, Type.Put, "v2"),
                    createCell("r3", "", "", 0, Type.Put, "")),
                createResult(
                    createCell("r1", "f1", "q1", 1, Type.Put, "v1"),
                    createCell("r2", "f2", "q2", 2, Type.Put, "v2"))))
        .isFalse();

    assertThat(
            resultsEqual(
                createResult(
                    createCell("r1", "f1", "q1", 1, Type.Put, "v1"),
                    createCell("r2", "f2", "q2", 2, Type.Put, "v2")),
                createResult(
                    createCell("r1", "f1", "q1", 1, Type.Put, "v1"),
                    createCell("r2", "f2", "q2", 2, Type.Put, "v2"),
                    createCell("r3", "", "", 0, Type.Put, ""))))
        .isFalse();
  }

  @Test
  public void testNotMatchingCellContents() {
    assertThat(
            resultsEqual(
                createResult(createCell("r3", "f1", "q1", 1, Type.Put, "v1")),
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v1"))))
        .isFalse();
    assertThat(
            resultsEqual(
                createResult(createCell("r1", "f3", "q1", 1, Type.Put, "v1")),
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v1"))))
        .isFalse();
    assertThat(
            resultsEqual(
                createResult(createCell("r1", "f1", "q3", 1, Type.Put, "v1")),
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v1"))))
        .isFalse();

    assertThat(
            resultsEqual(
                createResult(createCell("r1", "f1", "q1", 3, Type.Put, "v1")),
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v1"))))
        .isFalse();

    assertThat(
            resultsEqual(
                createResult(createCell("r1", "f1", "q1", 1, Type.Delete, "v1")),
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v1"))))
        .isFalse();
    assertThat(
            resultsEqual(
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v3")),
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v1"))))
        .isFalse();
  }

  @Test
  public void testResultOfEmptyGetIsCompared() {
    // Gets on non existent cells return Results with cells set to null.
    assertThat(resultsEqual(createResult((Cell[]) null), createResult((Cell[]) null))).isTrue();
    assertThat(
            resultsEqual(
                createResult((Cell[]) null),
                createResult(createCell("r1", "f1", "q1", 1, Type.Put, "v1"))))
        .isFalse();
  }
}
