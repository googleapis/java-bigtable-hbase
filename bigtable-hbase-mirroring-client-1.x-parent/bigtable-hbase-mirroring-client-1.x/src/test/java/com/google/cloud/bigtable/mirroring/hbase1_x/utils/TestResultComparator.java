package com.google.cloud.bigtable.mirroring.hbase1_x.utils;

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.Comparators.resultsEqual;
import static com.google.common.truth.Truth.assertThat;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestResultComparator {
  private Cell createCell(
      String row, String family, String qualifier, int timestamp, Type type, String value) {
    return CellUtil.createCell(
        row.getBytes(),
        family.getBytes(),
        qualifier.getBytes(),
        timestamp,
        type.getCode(),
        value.getBytes());
  }

  private Result createResult(Cell... cells) {
    return Result.create(cells);
  }

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
}
