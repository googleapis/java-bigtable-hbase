/*
 * Copyright 2022 Google LLC
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

package com.google.cloud.bigtable.hbase.replication.utils;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class containing various helpers for tests. */
// TODO: move it to test folder and mark replication-core as test dependency for replication 1.x and
// 2.x
@InterfaceAudience.Private
public class TestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  public static final String ROW_KEY_PREFIX = "test-row-";
  public static final byte[] ROW_KEY = "test-row".getBytes();
  public static final byte[] ROW_KEY_2 = "test-row-2".getBytes();
  public static final byte[] FILTERED_ROW_KEY = "filtered-row".getBytes();
  public static final byte[] CF1 = "cf1".getBytes();
  public static final byte[] CF2 = "cf2".getBytes();
  public static final String TABLE_NAME_STRING = "replication-test";
  public static final TableName TABLE_NAME = TableName.valueOf(TABLE_NAME_STRING);
  public static final String TABLE_NAME_STRING_2 = "replication-test-2";
  public static final TableName TABLE_NAME_2 = TableName.valueOf(TABLE_NAME_STRING_2);
  public static final byte[] COL_QUALIFIER = "col1".getBytes();
  public static final byte[] COL_QUALIFIER_2 = "col2".getBytes();
  public static final String VALUE_PREFIX = "Value-";
  public static final byte[] VALUE = "Value".getBytes();
  public static final long TIMESTAMP = 1000l;

  /** How long to wait for asynchronous replication before giving up. */
  private static final long REPLICATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(60);

  /** How long to wait between polls while waiting on asynchronous replication. */
  private static final long POLL_INTERVAL_MILLIS = 500;

  /** Consecutive unchanged polls required before replication is considered idle. */
  private static final int QUIESCE_STABLE_POLLS = 3;

  // ONLY STATIC METHODS
  private TestUtils() {}

  public static byte[] getRowKey(int i) {
    return (ROW_KEY_PREFIX + i).getBytes();
  }

  public static byte[] getValue(int i) {
    return (VALUE_PREFIX + i).getBytes();
  }

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

  public static void assertEquals(Mutation expected, Mutation actual) {
    assertEquals("Row keys mismatch", expected.getRow(), actual.getRow());

    // Prevent creating a list every time.
    NavigableMap<byte[], List<Cell>> expectedFamilyCellMap = expected.getFamilyCellMap();
    NavigableMap<byte[], List<Cell>> actualFamilyCellMap = actual.getFamilyCellMap();

    Assert.assertEquals(
        "Mutation family count mismatch for row "
            + new String(expected.getRow())
            + "\n \t\texpectedFamilyCellMap: "
            + expectedFamilyCellMap
            + "\n \t\tactualFamilyCellMap: "
            + actualFamilyCellMap,
        expected.getFamilyCellMap().size(),
        actual.getFamilyCellMap().size());

    for (byte[] cf : expected.getFamilyCellMap().keySet()) {
      List<Cell> expectedCells = expectedFamilyCellMap.get(cf);
      List<Cell> actualCells = actualFamilyCellMap.get(cf);

      assertTrue(
          "Expected cells for family: " + Bytes.toString(cf),
          actualCells != null && !actualCells.isEmpty());

      Assert.assertEquals(
          "Mutation cell count mismatch for row "
              + new String(expected.getRow())
              + " family: "
              + cf
              + "\n \t\texpected: "
              + expectedCells
              + "\n \t\tactual: "
              + actualCells,
          expectedCells.size(),
          actualCells.size());

      for (int i = 0; i < expectedCells.size(); i++) {
        assertEquals(expectedCells.get(i), actualCells.get(i));
      }
    }
  }

  public static void assertEquals(RowMutations expected, RowMutations actual) {
    assertEquals("Row keys mismatch", expected.getRow(), actual.getRow());

    Assert.assertEquals(
        "RowMutations mutation count mismatch for row "
            + new String(expected.getRow())
            + "\n \t\texpectedMutations: "
            + expected.getMutations()
            + "\n \t\tactualMutations: "
            + actual.getMutations(),
        expected.getMutations().size(),
        actual.getMutations().size());

    for (int i = 0; i < expected.getMutations().size(); i++) {
      assertEquals(expected.getMutations().get(i), actual.getMutations().get(i));
    }
  }

  /** Scans the 2 Tables and compares the data. Fails assertion in case of a mismatch. */
  public static void assertTableEquals(Table expected, Table actual) throws IOException {

    try (ResultScanner expectedScanner = expected.getScanner(new Scan().setMaxVersions());
        ResultScanner actualScanner = actual.getScanner(new Scan().setMaxVersions())) {

      Iterator<Result> expectedIterator = expectedScanner.iterator();
      Iterator<Result> actualIterator = actualScanner.iterator();

      // Both databases should return exact same results in exact same order.
      while (expectedIterator.hasNext()) {
        // Check size mismatch
        if (!actualIterator.hasNext()) {
          Assert.fail(
              "Expected more rows. CBT missing results starting at "
                  + Bytes.toStringBinary(expectedIterator.next().getRow()));
        }

        // Both the databases have next, so compare the results
        assertEquals(expectedIterator.next(), actualIterator.next());
      }
      // Verify that actual has no more data left
      if (actualIterator.hasNext()) {
        Assert.fail(
            "Expected less rows, extra results in CBT starting from "
                + actualIterator.next().toString());
      }
    }
  }

  /** Waits for replication to replicate numReplicatedEntries */
  public static void waitForReplication(BooleanSupplier isReplicationCurrent)
      throws InterruptedException {
    long start = System.nanoTime();
    long deadline = start + TimeUnit.MILLISECONDS.toNanos(REPLICATION_TIMEOUT_MILLIS);
    while (true) {
      if (isReplicationCurrent.getAsBoolean()) {
        LOG.info("Replication caught up after " + elapsedMillis(start) + "ms");
        return;
      }
      if (System.nanoTime() >= deadline) {
        Assert.fail(
            "Timed out waiting for replication to catch up after " + elapsedMillis(start) + "ms");
      }
      Thread.sleep(POLL_INTERVAL_MILLIS);
    }
  }

  private static long elapsedMillis(long startNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }

  /**
   * Waits until {@code replicatedEntries} stops advancing, draining entries left in flight by an
   * earlier test.
   *
   * <p>The replication endpoint's entry counter is static and shared by every test in a class.
   * Resetting it between tests is not enough on its own: replication is asynchronous, so entries
   * belonging to the previous test keep arriving after the reset and count towards the next test's
   * target. Call this before resetting the counter so it starts from a genuinely idle state.
   *
   * <p>This is best effort and does not fail on timeout; callers still verify the data itself.
   */
  public static void waitForReplicationToQuiesce(IntSupplier replicatedEntries)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(REPLICATION_TIMEOUT_MILLIS);
    int previous = -1;
    int stablePolls = 0;
    while (stablePolls < QUIESCE_STABLE_POLLS && System.nanoTime() < deadline) {
      int current = replicatedEntries.getAsInt();
      stablePolls = (current == previous) ? stablePolls + 1 : 0;
      previous = current;
      Thread.sleep(POLL_INTERVAL_MILLIS);
    }
  }

  /**
   * Waits for numReplicatedEntries to be replicated to the TestReplicationEndpoint and then compare
   * the 2 tables to be equal.
   */
  public static void assertTableEventuallyEquals(
      Table expected, Table actual, BooleanSupplier isReplicationCurrent)
      throws InterruptedException, IOException {
    waitForReplication(isReplicationCurrent);

    // The entry counter the predicate reads is only a coarse progress signal: it is shared across
    // tests and counts entries for every table, so it can be satisfied before this table's own
    // writes land. Retry the comparison so the assertion waits on the replicated data itself.
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(REPLICATION_TIMEOUT_MILLIS);
    while (true) {
      try {
        TestUtils.assertTableEquals(expected, actual);
        return;
      } catch (AssertionError e) {
        if (System.nanoTime() >= deadline) {
          throw e;
        }
      }
      Thread.sleep(POLL_INTERVAL_MILLIS);
    }
  }

  /**
   * Creates a {@link CompletableFuture} that has already completed exceptionally with the given
   * {@code error}.
   *
   * @return
   */
  public static Future<Object> failedFuture(Throwable error) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }
}
