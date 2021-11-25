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
package com.google.cloud.bigtable.hbase.mirroring;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers.DatabaseSelector;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestWriteErrorConsumer;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegionRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOptions;
import com.google.common.base.Predicate;
import com.google.common.primitives.Longs;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.shaded.org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class TestMirroringTable {
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();
  @Rule public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();
  private DatabaseHelpers databaseHelpers =
      new DatabaseHelpers(connectionRule, executorServiceRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  @Rule public FailingHBaseHRegionRule failingHBaseHRegionRule = new FailingHBaseHRegionRule();

  static final byte[] columnFamily1 = "cf1".getBytes();
  static final byte[] qualifier1 = "cq1".getBytes();
  static final byte[] qualifier2 = "cq2".getBytes();
  static final byte[] qualifier3 = "cq3".getBytes();
  static final byte[] qualifier4 = "cq4".getBytes();
  static final byte[] qualifier5 = "cq5".getBytes();

  final Predicate<byte[]> failPredicate =
      new Predicate<byte[]>() {
        @Override
        public boolean apply(@NullableDecl byte[] bytes) {
          return bytes.length == 8 && Longs.fromByteArray(bytes) % 2 == 0;
        }
      };

  public static byte[] rowKeyFromId(int id) {
    return Longs.toByteArray(id);
  }

  @Test
  public void testPut() throws IOException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          t1.put(Helpers.createPut(i, columnFamily1, qualifier1));
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName2)) {
        int id = 0;
        for (int i = 0; i < 10; i++) {
          List<Put> puts = new ArrayList<>();
          for (int j = 0; j < 100; j++) {
            puts.add(Helpers.createPut(id, columnFamily1, qualifier1));
            id++;
          }
          t1.put(puts);
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName2);
  }

  @Test
  public void testPutWithPrimaryErrors() throws IOException {
    int databaseEntriesCount = 1000;
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    FailingHBaseHRegion.failMutation(
        failPredicate, OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          final byte[] rowKey = rowKeyFromId(i);
          final int finalI = i;
          catchIOExceptionsIfWillThrow(
              rowKey,
              new RunnableThrowingIO() {
                @Override
                public void run() throws IOException {
                  t1.put(Helpers.createPut(finalI, columnFamily1, qualifier1));
                }
              });
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName2)) {
        int id = 0;
        for (int i = 0; i < 100; i++) {
          List<Put> puts = new ArrayList<>();
          for (int j = 0; j < 100; j++) {
            puts.add(Helpers.createPut(id, columnFamily1, qualifier1));
            id++;
          }
          try {
            t1.put(puts);
          } catch (RetriesExhaustedWithDetailsException e) {
            assertThat(e.getNumExceptions()).isEqualTo(50);
          }
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName2);
  }

  @Test
  public void testPutWithSecondaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    FailingHBaseHRegion.failMutation(
        failPredicate, OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    final TableName tableName1 = connectionRule.createTable(columnFamily1);

    ReportedErrorsContext reportedErrorsContext1 = new ReportedErrorsContext();
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          t1.put(Helpers.createPut(i, columnFamily1, qualifier1));
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName1, failPredicate);

    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);

    ReportedErrorsContext reportedErrorsContext2 = new ReportedErrorsContext();
    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName2)) {
        int id = 0;
        for (int i = 0; i < databaseEntriesCount / 100; i++) {
          List<Put> puts = new ArrayList<>();
          for (int j = 0; j < 100; j++) {
            puts.add(Helpers.createPut(id, columnFamily1, qualifier1));
            id++;
          }
          t1.put(puts);
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName2, failPredicate);
    reportedErrorsContext2.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testDelete() throws IOException {
    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          t1.delete(Helpers.createDelete(rowKey, columnFamily1, qualifier1));
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName2, databaseEntriesCount, columnFamily1, qualifier1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName2)) {
        int id = 0;
        for (int i = 0; i < databaseEntriesCount / 100; i++) {
          List<Delete> deletes = new ArrayList<>();
          for (int j = 0; j < 100; j++) {
            byte[] rowKey = rowKeyFromId(i);
            deletes.add(Helpers.createDelete(rowKey, columnFamily1, qualifier1));
            id++;
          }
          t1.delete(deletes);
          assertThat(deletes).isEmpty();
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName2);
  }

  @Test
  public void testDeleteWithPrimaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    // Fill tables before forcing operations to fail.
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName2, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(
        failPredicate, OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          final byte[] rowKey = rowKeyFromId(i);
          catchIOExceptionsIfWillThrow(
              rowKey,
              new RunnableThrowingIO() {
                @Override
                public void run() throws IOException {
                  t1.delete(Helpers.createDelete(rowKey, columnFamily1, qualifier1));
                }
              });
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName1);

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName2)) {
        int id = 0;
        for (int i = 0; i < databaseEntriesCount / 100; i++) {
          List<Delete> deletes = new ArrayList<>();
          for (int j = 0; j < 100; j++) {
            id++;
            byte[] rowKey = rowKeyFromId(id);
            deletes.add(Helpers.createDelete(rowKey, columnFamily1, qualifier1));
          }
          try {
            t1.delete(deletes);
          } catch (RetriesExhaustedWithDetailsException e) {
            assertThat(e.getNumExceptions()).isEqualTo(50);
            assertThat(deletes.size()).isEqualTo(50);
            List<byte[]> thrownRows = new ArrayList<>();
            for (int exceptionId = 0; exceptionId < e.getNumExceptions(); exceptionId++) {
              thrownRows.add(e.getRow(exceptionId).getRow());
            }

            List<byte[]> notDeletedRows = new ArrayList<>();
            for (Delete delete : deletes) {
              notDeletedRows.add(delete.getRow());
            }
            assertThat(notDeletedRows).containsExactlyElementsIn(thrownRows);
          }
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName2);
  }

  @Test
  public void testDeleteWithSecondaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    // Fill tables before forcing operations to fail.
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName2, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, OperationStatusCode.BAD_FAMILY, "failed");

    ReportedErrorsContext reportedErrorsContext1 = new ReportedErrorsContext();
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          table.delete(Helpers.createDelete(rowKey, columnFamily1, qualifier1));
        }
      }
    }
    assertThat(databaseHelpers.countRows(tableName1, DatabaseSelector.PRIMARY)).isEqualTo(0);
    assertThat(databaseHelpers.countRows(tableName1, DatabaseSelector.SECONDARY))
        .isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);

    ReportedErrorsContext reportedErrorsContext2 = new ReportedErrorsContext();
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName2)) {
        int id = 0;
        for (int i = 0; i < databaseEntriesCount / 100; i++) {
          List<Delete> deletes = new ArrayList<>();
          for (int j = 0; j < 100; j++) {
            byte[] rowKey = rowKeyFromId(id);
            deletes.add(Helpers.createDelete(rowKey, columnFamily1, qualifier1));
            id++;
          }
          table.delete(deletes);
          assertThat(deletes).isEmpty();
        }
      }
    }
    assertThat(databaseHelpers.countRows(tableName2, DatabaseSelector.PRIMARY)).isEqualTo(0);
    assertThat(databaseHelpers.countRows(tableName2, DatabaseSelector.SECONDARY))
        .isEqualTo(databaseEntriesCount / 2);

    reportedErrorsContext2.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testCheckAndPut() throws IOException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          table.checkAndPut(
              rowKey,
              columnFamily1,
              qualifier1,
              Longs.toByteArray(i),
              Helpers.createPut(i, columnFamily1, qualifier2));
          table.checkAndPut(
              rowKey,
              columnFamily1,
              qualifier1,
              CompareOp.EQUAL,
              Longs.toByteArray(i),
              Helpers.createPut(i, columnFamily1, qualifier3));
          table.checkAndPut(
              rowKey,
              columnFamily1,
              qualifier1,
              CompareOp.GREATER,
              Longs.toByteArray(i + 1),
              Helpers.createPut(i, columnFamily1, qualifier4));
          table.checkAndPut(
              rowKey,
              columnFamily1,
              qualifier1,
              CompareOp.NOT_EQUAL,
              Longs.toByteArray(i),
              Helpers.createPut(i, columnFamily1, qualifier5));
        }
      }
    }

    assertThat(databaseHelpers.countRows(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);

    assertThat(databaseHelpers.countCells(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount * 4);

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndPutPrimaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          final byte[] rowKey = rowKeyFromId(i);
          final int finalI = i;
          catchIOExceptionsIfWillThrow(
              rowKey,
              new RunnableThrowingIO() {
                @Override
                public void run() throws IOException {
                  assertThat(
                          table.checkAndPut(
                              rowKey,
                              columnFamily1,
                              qualifier1,
                              rowKey,
                              Helpers.createPut(finalI, columnFamily1, qualifier2)))
                      .isTrue();
                }
              });
        }
      }
    }
    assertThat(databaseHelpers.countRows(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);
    assertThat(databaseHelpers.countCells(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo((int) (databaseEntriesCount * 1.5));
    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndPutSecondaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    ReportedErrorsContext reportedErrorsContext1 = new ReportedErrorsContext();
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          assertThat(
                  table.checkAndPut(
                      rowKey,
                      columnFamily1,
                      qualifier1,
                      rowKey,
                      Helpers.createPut(i, columnFamily1, qualifier2)))
              .isTrue();
        }
      }
    }
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier2)))
        .isEqualTo(databaseEntriesCount);
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseSelector.SECONDARY,
                Helpers.createScan(columnFamily1, qualifier2)))
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(databaseHelpers.countCells(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount * 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testCheckAndDelete() throws IOException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(
        tableName1,
        databaseEntriesCount,
        columnFamily1,
        qualifier1,
        qualifier2,
        qualifier3,
        qualifier4,
        qualifier5);

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          assertThat(
                  table.checkAndDelete(
                      rowKey,
                      columnFamily1,
                      qualifier1,
                      Longs.toByteArray(i),
                      Helpers.createDelete(Longs.toByteArray(i), columnFamily1, qualifier2)))
              .isTrue();
          assertThat(
                  table.checkAndDelete(
                      rowKey,
                      columnFamily1,
                      qualifier1,
                      CompareOp.EQUAL,
                      Longs.toByteArray(i),
                      Helpers.createDelete(Longs.toByteArray(i), columnFamily1, qualifier3)))
              .isTrue();
          assertThat(
                  table.checkAndDelete(
                      rowKey,
                      columnFamily1,
                      qualifier1,
                      CompareOp.GREATER,
                      Longs.toByteArray(i + 1),
                      Helpers.createDelete(Longs.toByteArray(i), columnFamily1, qualifier4)))
              .isTrue();
          assertThat(
                  table.checkAndDelete(
                      rowKey,
                      columnFamily1,
                      qualifier1,
                      CompareOp.NOT_EQUAL,
                      Longs.toByteArray(i),
                      Helpers.createDelete(Longs.toByteArray(i), columnFamily1, qualifier5)))
              .isFalse();
        }
      }
    }

    assertThat(databaseHelpers.countRows(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);

    assertThat(databaseHelpers.countCells(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount * 2);

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndDeletePrimaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(
        tableName1, databaseEntriesCount, columnFamily1, qualifier1, qualifier2);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          final byte[] rowKey = rowKeyFromId(i);
          catchIOExceptionsIfWillThrow(
              rowKey,
              new RunnableThrowingIO() {
                @Override
                public void run() throws IOException {
                  assertThat(
                          table.checkAndDelete(
                              rowKey,
                              columnFamily1,
                              qualifier1,
                              rowKey,
                              Helpers.createDelete(rowKey, columnFamily1, qualifier2)))
                      .isTrue();
                }
              });
        }
      }
    }

    assertThat(databaseHelpers.countRows(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);

    assertThat(databaseHelpers.countCells(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo((int) (databaseEntriesCount * 1.5));

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndDeleteSecondaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    ReportedErrorsContext reportedErrorsContext1 = new ReportedErrorsContext();
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          assertThat(
                  table.checkAndDelete(
                      rowKey,
                      columnFamily1,
                      qualifier1,
                      rowKey,
                      Helpers.createDelete(rowKey, columnFamily1, qualifier1)))
              .isTrue();
        }
      }
    }
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(0);
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseSelector.SECONDARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testCheckAndMutate() throws IOException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          assertThat(
                  table.checkAndMutate(
                      rowKey,
                      columnFamily1,
                      qualifier1,
                      CompareOp.EQUAL,
                      Longs.toByteArray(i),
                      Helpers.createRowMutations(
                          rowKey,
                          Helpers.createPut(i, columnFamily1, qualifier2),
                          Helpers.createDelete(rowKey, columnFamily1, qualifier1))))
              .isTrue();
          assertThat(
                  table.checkAndMutate(
                      rowKey,
                      columnFamily1,
                      qualifier1,
                      CompareOp.EQUAL,
                      Longs.toByteArray(i),
                      Helpers.createRowMutations(
                          rowKey, Helpers.createDelete(rowKey, columnFamily1, qualifier2))))
              .isFalse();
        }
      }
    }

    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(0);

    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier2)))
        .isEqualTo(databaseEntriesCount);

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndMutatePrimaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(
        tableName1, databaseEntriesCount, columnFamily1, qualifier1, qualifier2);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          final byte[] rowKey = rowKeyFromId(i);
          catchIOExceptionsIfWillThrow(
              rowKey,
              new RunnableThrowingIO() {
                @Override
                public void run() throws IOException {
                  assertThat(
                          table.checkAndMutate(
                              rowKey,
                              columnFamily1,
                              qualifier1,
                              CompareOp.EQUAL,
                              rowKey,
                              Helpers.createRowMutations(
                                  rowKey, Helpers.createDelete(rowKey, columnFamily1, qualifier2))))
                      .isTrue();
                }
              });
        }
      }
    }

    assertThat(databaseHelpers.countRows(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);

    assertThat(databaseHelpers.countCells(tableName1, DatabaseSelector.PRIMARY))
        .isEqualTo((int) (databaseEntriesCount * 1.5));

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndMutateSecondaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    ReportedErrorsContext reportedErrorsContext1 = new ReportedErrorsContext();
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          assertThat(
                  table.checkAndMutate(
                      rowKey,
                      columnFamily1,
                      qualifier1,
                      CompareOp.EQUAL,
                      rowKey,
                      Helpers.createRowMutations(
                          rowKey, Helpers.createDelete(rowKey, columnFamily1, qualifier1))))
              .isTrue();
        }
      }
    }
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(0);
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseSelector.SECONDARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testIncrement() throws IOException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          table.increment(Helpers.createIncrement(rowKey, columnFamily1, qualifier1));
        }
      }
    }

    databaseHelpers.verifyTableConsistency(tableName1);

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          Result r = table.get(Helpers.createGet(rowKey, columnFamily1, qualifier1));
          assertThat(Longs.fromByteArray(r.getValue(columnFamily1, qualifier1))).isEqualTo(i + 1);
        }
      }
    }
  }

  @Test
  public void testIncrementPrimaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          final byte[] rowKey = rowKeyFromId(i);
          catchIOExceptionsIfWillThrow(
              rowKey,
              new RunnableThrowingIO() {
                @Override
                public void run() throws IOException {
                  table.increment(Helpers.createIncrement(rowKey, columnFamily1, qualifier1));
                }
              });
        }
      }
    }

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testIncrementSecondaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    TestWriteErrorConsumer.clearErrors();

    FailingHBaseHRegion.failMutation(
        failPredicate, OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    ReportedErrorsContext reportedErrorsContext1 = new ReportedErrorsContext();
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          table.increment(Helpers.createIncrement(rowKey, columnFamily1, qualifier1));
        }
      }
    }

    assertThat(TestWriteErrorConsumer.getErrorCount()).isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testAppend() throws IOException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          table.append(Helpers.createAppend(rowKey, columnFamily1, qualifier1, new byte[] {1}));
        }
      }
    }

    databaseHelpers.verifyTableConsistency(tableName1);

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          Result r = table.get(Helpers.createGet(rowKey, columnFamily1, qualifier1));
          byte[] expectedValue = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 1};
          System.arraycopy(rowKey, 0, expectedValue, 0, 8);
          assertThat(r.getValue(columnFamily1, qualifier1)).isEqualTo(expectedValue);
        }
      }
    }
  }

  @Test
  public void testAppendPrimaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          final byte[] rowKey = rowKeyFromId(i);
          catchIOExceptionsIfWillThrow(
              rowKey,
              new RunnableThrowingIO() {
                @Override
                public void run() throws IOException {
                  table.append(
                      Helpers.createAppend(rowKey, columnFamily1, qualifier1, new byte[] {1}));
                }
              });
        }
      }
    }

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testAppendSecondaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    TestWriteErrorConsumer.clearErrors();

    FailingHBaseHRegion.failMutation(
        failPredicate, OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    ReportedErrorsContext reportedErrorsContext1 = new ReportedErrorsContext();
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table table = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          table.append(Helpers.createAppend(rowKey, columnFamily1, qualifier1, new byte[] {1}));
        }
      }
    }

    assertThat(TestWriteErrorConsumer.getErrorCount()).isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testGet() throws IOException {
    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          t1.get(Helpers.createGet(rowKey, columnFamily1, qualifier1));
        }
      }
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount()).isEqualTo(0);
  }

  @Test
  public void testGetWithPrimaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          final byte[] rowKey = rowKeyFromId(i);
          catchIOExceptionsIfWillThrow(
              rowKey,
              new RunnableThrowingIO() {
                @Override
                public void run() throws IOException {
                  t1.get(Helpers.createGet(rowKey, columnFamily1, qualifier1));
                }
              });
        }
      }
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount()).isEqualTo(0);
  }

  @Test
  public void testGetWithSecondaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          t1.get(Helpers.createGet(rowKey, columnFamily1, qualifier1));
        }
      }
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount())
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount("failure"))
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount("mismatch")).isEqualTo(0);
  }

  @Test
  public void testExists() throws IOException {
    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          t1.exists(Helpers.createGet(rowKey, columnFamily1, qualifier1));
        }
      }
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount()).isEqualTo(0);
  }

  @Test
  public void testExistsWithPrimaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          final byte[] rowKey = rowKeyFromId(i);
          catchIOExceptionsIfWillThrow(
              rowKey,
              new RunnableThrowingIO() {
                @Override
                public void run() throws IOException {
                  t1.exists(Helpers.createGet(rowKey, columnFamily1, qualifier1));
                }
              });
        }
      }
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount()).isEqualTo(0);
  }

  @Test
  public void testExistsWithSecondaryErrors() throws IOException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName1)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          t1.exists(Helpers.createGet(rowKey, columnFamily1, qualifier1));
        }
      }
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount())
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount("failure"))
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount("mismatch")).isEqualTo(0);
  }

  @Test
  public void testBatch() throws IOException, InterruptedException {
    int databaseEntriesCount = 1000;

    final TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName)) {
        int id = 0;
        while (id < databaseEntriesCount) {
          List<Put> batch = new ArrayList<>();
          for (int j = 0; j < 100 && id < databaseEntriesCount; j++) {
            batch.add(Helpers.createPut(id, columnFamily1, qualifier1));
            id++;
          }
          Object[] result = new Object[batch.size()];
          t1.batch(batch, result);
          for (Object o : result) {
            assertThat(o).isInstanceOf(Result.class);
          }
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName);
  }

  @Test
  public void testBatchWithPrimaryErrors() throws IOException, InterruptedException {
    int databaseEntriesCount = 1000;
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    final TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName)) {
        int id = 0;
        while (id < databaseEntriesCount) {
          List<Put> batch = new ArrayList<>();
          for (int j = 0; j < 100 && id < databaseEntriesCount; j++) {
            batch.add(Helpers.createPut(id, columnFamily1, qualifier1));
            id++;
          }
          Object[] result = new Object[batch.size()];
          try {
            t1.batch(batch, result);
          } catch (RetriesExhaustedWithDetailsException e) {
            assertThat(e.getNumExceptions()).isEqualTo(50);
            int correctResults = 0;
            for (Object o : result) {
              if (o instanceof Result) {
                correctResults++;
              }
            }
            assertThat(correctResults).isEqualTo(50);
          }
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName);
  }

  @Test
  public void testBatchWithSecondaryErrors() throws IOException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    ReportedErrorsContext reportedErrorsContext1 = new ReportedErrorsContext();
    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName2)) {
        int id = 0;
        while (id < databaseEntriesCount) {
          List<Put> batch = new ArrayList<>();
          for (int j = 0; j < 100 && id < databaseEntriesCount; j++) {
            batch.add(Helpers.createPut(id, columnFamily1, qualifier1));
            id++;
          }
          Object[] result = new Object[batch.size()];
          t1.batch(batch, result);
          for (Object o : result) {
            assertThat(o).isInstanceOf(Result.class);
          }
        }
      }
    }
    databaseHelpers.verifyTableConsistency(tableName2, failPredicate);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  interface RunnableThrowingIO {
    void run() throws IOException;
  }

  private void catchIOExceptionsIfWillThrow(byte[] rowKey, RunnableThrowingIO runnable) {
    boolean willThrow = failPredicate.apply(rowKey);
    try {
      runnable.run();
      if (willThrow) {
        fail("should throw");
      }
    } catch (IOException e) {
      if (!willThrow) {
        fail("shouldn't throw");
      }
    }
  }

  private static int getSecondaryWriteErrorLogMessagesWritten() throws IOException {
    Configuration configuration = ConfigurationHelper.newConfiguration();
    MirroringOptions mirroringOptions = new MirroringOptions(configuration);
    String prefixPath = mirroringOptions.faillog.prefixPath;
    String[] prefixParts = prefixPath.split("/");
    final String fileNamePrefix = prefixParts[prefixParts.length - 1];
    String[] directoryParts = Arrays.copyOf(prefixParts, prefixParts.length - 1);
    StringBuilder sb = new StringBuilder();
    for (String directoryPart : directoryParts) {
      sb.append(directoryPart);
      sb.append("/");
    }
    String directoryPath = sb.toString();
    File dir = new File(directoryPath);
    File[] files =
        dir.listFiles(
            new FileFilter() {
              @Override
              public boolean accept(File file) {
                return file.getName().startsWith(fileNamePrefix);
              }
            });

    int numberOfLines = 0;
    for (File f : files) {
      String fileStr = FileUtils.readFileToString(f);
      if (!fileStr.isEmpty()) {
        numberOfLines += fileStr.split("\n").length;
      }
    }
    return numberOfLines;
  }

  static class ReportedErrorsContext {
    final int initialErrorsConsumed;
    final int initialErrorsWritten;

    public ReportedErrorsContext() throws IOException {
      this.initialErrorsConsumed = TestWriteErrorConsumer.getErrorCount();
      this.initialErrorsWritten = getSecondaryWriteErrorLogMessagesWritten();
    }

    public void assertNewErrorsReported(int expectedNewErrors) throws IOException {
      int errorsConsumed = TestWriteErrorConsumer.getErrorCount();
      int errorsWritten = getSecondaryWriteErrorLogMessagesWritten();

      assertThat(errorsConsumed - initialErrorsConsumed).isEqualTo(expectedNewErrors);
      assertThat(errorsWritten - initialErrorsWritten).isEqualTo(expectedNewErrors);
    }
  }
}
