package com.google.cloud.bigtable.hbase.replication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

/** Utility class containing various helpers for tests. */
public class TestUtils {

  // ONLY STATIC METHODS
  private TestUtils() {}

  public static void assertEquals(String message, byte[] expected, byte[] actual) {
    Assert.assertEquals(
        message,
        new String(expected, StandardCharsets.UTF_8),
        new String(actual, StandardCharsets.UTF_8));
  }

  public static void assertEquals(Cell expected, Cell actual) {
    String rowKey = new String(CellUtil.cloneRow(expected), StandardCharsets.UTF_8);
    assertEquals("Cell row key mismatch", CellUtil.cloneRow(expected), CellUtil.cloneRow(actual));
    assertEquals(
        "Column qualifier mismatch for row " + rowKey,
        CellUtil.cloneQualifier(expected),
        CellUtil.cloneQualifier(actual));
    Assert.assertEquals(
        "Timestamp mismatch for row " + rowKey, expected.getTimestamp(), actual.getTimestamp());
    assertEquals(
        "Value mismatch for row " + rowKey, CellUtil.cloneRow(expected), CellUtil.cloneRow(actual));
  }

  public static void assertEquals(Result expected, Result actual) {
    assertEquals("Result row keys mismatch", expected.getRow(), actual.getRow());

    // Prevent creating a list every time.
    List<Cell> expectedCells = expected.listCells();
    List<Cell> actualCells = actual.listCells();
    Assert.assertEquals(
        "Result cell count mismatch for row "
            + new String(expected.getRow())
            + "\n \t\texpected: "
            + expectedCells
            + "\n \t\tactual: "
            + actualCells,
        expected.size(),
        actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expectedCells.get(i), actualCells.get(i));
    }
  }

  /** Scans the 2 Tables and compares the data. Fails assertion in case of a mismatch. */
  public static void assertTableEquals(Table expected, Table actual) throws IOException {

    ResultScanner expectedScanner = expected.getScanner(new Scan().setMaxVersions());
    ResultScanner actualScanner = actual.getScanner(new Scan().setMaxVersions());

    Iterator<Result> expectedIterator = expectedScanner.iterator();
    Iterator<Result> actualIterator = actualScanner.iterator();

    // Both databases should return exact same results in exact same order.
    while (expectedIterator.hasNext()) {
      // Check size mismatch
      if (!actualIterator.hasNext()) {
        Assert.fail(
            "Expected less rows. Extra results starting at "
                + Bytes.toStringBinary(expectedIterator.next().getRow()));
      }

      // Both the databases have next, so compare the results
      assertEquals(expectedIterator.next(), actualIterator.next());
    }
    // Verify that actual has no more data left
    if (actualIterator.hasNext()) {
      Assert.fail(
          "Expected more rows, missing rows starting from "
              + actualIterator.next().toString());
    }
  }
}
