/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.bigtable.hbase.test_helpers;

import static com.google.common.truth.Fact.simpleFact;
import static com.google.common.truth.Truth.assertAbout;

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ResultSubject extends Subject {
  // User-defined entry point
  public static ResultSubject assertThat(org.apache.hadoop.hbase.client.Result result) {
    return assertAbout(results()).that(result);
  }

  // Static method for getting the subject factory (for use with assertAbout())
  public static Subject.Factory<ResultSubject, Result> results() {
    return ResultSubject::new;
  }

  private final Result actual;

  public ResultSubject(FailureMetadata metadata, @Nullable Result actual) {
    super(metadata, actual);
    this.actual = actual;
  }

  public void latestCellHasValue(byte[] family, byte[] qualifier, byte[] value) {
    Cell cell = actual.getColumnLatestCell(family, qualifier);
    byte[] actualValue = CellUtil.cloneValue(cell);
    check("getColumnLatestCell(%s, %s).value()", family, Bytes.toStringBinary(qualifier))
        .that(Bytes.toStringBinary(actualValue))
        .isEqualTo(Bytes.toStringBinary(value));
  }

  public void doesNotHaveCell(byte[] family, byte[] qualifier) {
    List<Cell> columnCells = actual.getColumnCells(family, qualifier);

    check("doesNotHaveCell(%s,%s)", Bytes.toStringBinary(family), Bytes.toStringBinary(qualifier))
        .that(columnCells)
        .isEmpty();
  }

  @Override
  public void isEqualTo(@Nullable Object expected) {
    if (actual == expected) {
      return;
    }
    if (actual == null) {
      failWithoutActual(simpleFact("got null when expecting a value"));
      return;
    }
    if (expected == null) {
      failWithoutActual(simpleFact("expected null but got: " + actual));
      return;
    }

    try {
      Result.compareResults(actual, (Result) expected);
    } catch (Exception e) {
      failWithoutActual(simpleFact(e.getMessage()));
    }
  }

  @Override
  public void isNotEqualTo(@Nullable Object unexpected) {
    if (actual == unexpected) {
      failWithoutActual(simpleFact("Expected different values, but got same instance"));
      return;
    }
    if (actual == null || unexpected == null) {
      return;
    }

    try {
      Result.compareResults(actual, (Result) unexpected);
      failWithoutActual(simpleFact("Results were equal to each other"));
    } catch (Exception e) {
      // noop
    }
  }
}
