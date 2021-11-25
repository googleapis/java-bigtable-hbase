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

import com.google.cloud.bigtable.hbase.mirroring.utils.AsyncConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestWriteErrorConsumer;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegionRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase2_x.MirroringAsyncConnection;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMirroringAsyncTable {
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();

  @ClassRule
  public static AsyncConnectionRule asyncConnectionRule = new AsyncConnectionRule(connectionRule);

  @Rule public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();

  @Rule public FailingHBaseHRegionRule failingHBaseHRegionRule = new FailingHBaseHRegionRule();

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  final Predicate<byte[]> failPredicate =
      (bytes) -> bytes.length == 8 && Longs.fromByteArray(bytes) % 2 == 0;

  public DatabaseHelpers databaseHelpers = new DatabaseHelpers(connectionRule, executorServiceRule);

  public static final Configuration config = ConfigurationHelper.newConfiguration();

  static final byte[] columnFamily1 = "cf1".getBytes();
  static final byte[] qualifier1 = "cq1".getBytes();
  static final byte[] qualifier2 = "cq2".getBytes();
  static final byte[] qualifier3 = "cq3".getBytes();
  static final byte[] qualifier4 = "cq4".getBytes();
  static final byte[] qualifier5 = "cq5".getBytes();

  public static byte[] rowKeyFromId(int id) {
    return Longs.toByteArray(id);
  }

  @Test
  public void testPut() throws IOException, ExecutionException, InterruptedException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Void>> putFutures =
          IntStream.range(0, databaseEntriesCount)
              .mapToObj(i -> t.put(Helpers.createPut(i, columnFamily1, qualifier1)))
              .collect(Collectors.toList());

      CompletableFuture.allOf(putFutures.toArray(new CompletableFuture[0])).get();
    }
    databaseHelpers.verifyTableConsistency(tableName1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<List<CompletableFuture<Void>>> putBatches = new ArrayList<>();
      int id = 0;
      for (int i = 0; i < 10; i++) {
        List<Put> puts = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
          puts.add(Helpers.createPut(id, columnFamily1, qualifier1));
          id++;
        }
        putBatches.add(t.put(puts));
      }
      CompletableFuture.allOf(
              putBatches.stream()
                  .flatMap(List::stream)
                  .collect(Collectors.toList())
                  .toArray(new CompletableFuture[0]))
          .get();
    }
    databaseHelpers.verifyTableConsistency(tableName2);
  }

  @Test
  public void testPutWithPrimaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    FailingHBaseHRegion.failMutation(
        failPredicate, HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Void>> putFutures =
          IntStream.range(0, databaseEntriesCount)
              .mapToObj(i -> t.put(Helpers.createPut(i, columnFamily1, qualifier1)))
              .collect(Collectors.toList());
      CompletableFuture.allOf(putFutures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < putFutures.size(); i++) {
        checkIfShouldHaveThrown(putFutures.get(i), rowKeyFromId(i));
      }
    }
    databaseHelpers.verifyTableConsistency(tableName1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<List<CompletableFuture<Void>>> putBatches = new ArrayList<>();
      int id = 0;
      for (int i = 0; i < 100; i++) {
        List<Put> puts = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
          puts.add(Helpers.createPut(id, columnFamily1, qualifier1));
          id++;
        }
        putBatches.add(t.put(puts));
      }
      List<CompletableFuture<Void>> flatFutures =
          putBatches.stream().flatMap(List::stream).collect(Collectors.toList());
      CompletableFuture.allOf(flatFutures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < flatFutures.size(); i++) {
        checkIfShouldHaveThrown(flatFutures.get(i), rowKeyFromId(i));
      }
    }
    databaseHelpers.verifyTableConsistency(tableName2);
  }

  @Test
  public void testPutWithSecondaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    FailingHBaseHRegion.failMutation(
        failPredicate, HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    final TableName tableName1 = connectionRule.createTable(columnFamily1);

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext1 =
        new TestMirroringTable.ReportedErrorsContext();
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Void>> putFutures =
          IntStream.range(0, databaseEntriesCount)
              .mapToObj(i -> t.put(Helpers.createPut(i, columnFamily1, qualifier1)))
              .collect(Collectors.toList());

      CompletableFuture.allOf(putFutures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();
    }
    databaseHelpers.verifyTableConsistency(tableName1, failPredicate);

    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext2 =
        new TestMirroringTable.ReportedErrorsContext();
    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<List<CompletableFuture<Void>>> putBatches = new ArrayList<>();
      int id = 0;
      for (int i = 0; i < 10; i++) {
        List<Put> puts = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
          puts.add(Helpers.createPut(id, columnFamily1, qualifier1));
          id++;
        }
        putBatches.add(t.put(puts));
      }
      CompletableFuture.allOf(
              putBatches.stream()
                  .flatMap(List::stream)
                  .collect(Collectors.toList())
                  .toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();
    }

    databaseHelpers.verifyTableConsistency(tableName2, failPredicate);
    reportedErrorsContext2.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testDelete() throws IOException, ExecutionException, InterruptedException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Void>> deleteFutures =
          IntStream.range(0, databaseEntriesCount)
              .mapToObj(
                  i -> t.delete(Helpers.createDelete(rowKeyFromId(i), columnFamily1, qualifier1)))
              .collect(Collectors.toList());

      CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0])).get();
    }
    databaseHelpers.verifyTableConsistency(tableName1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName2, databaseEntriesCount, columnFamily1, qualifier1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<List<CompletableFuture<Void>>> deleteBatches = new ArrayList<>();
      int id = 0;
      for (int i = 0; i < 10; i++) {
        List<Delete> deletes = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
          deletes.add(Helpers.createDelete(rowKeyFromId(id), columnFamily1, qualifier1));
          id++;
        }
        deleteBatches.add(t.delete(deletes));
      }
      CompletableFuture.allOf(
              deleteBatches.stream()
                  .flatMap(List::stream)
                  .collect(Collectors.toList())
                  .toArray(new CompletableFuture[0]))
          .get();
    }
    databaseHelpers.verifyTableConsistency(tableName2);
  }

  @Test
  public void testDeleteWithPrimaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    // Fill tables before forcing operations to fail.
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName2, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(
        failPredicate, HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Void>> deleteFutures =
          IntStream.range(0, databaseEntriesCount)
              .mapToObj(
                  i -> t.delete(Helpers.createDelete(rowKeyFromId(i), columnFamily1, qualifier1)))
              .collect(Collectors.toList());

      CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < deleteFutures.size(); i++) {
        checkIfShouldHaveThrown(deleteFutures.get(i), rowKeyFromId(i));
      }
    }
    databaseHelpers.verifyTableConsistency(tableName1);

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<List<CompletableFuture<Void>>> deleteBatches = new ArrayList<>();
      int id = 0;
      for (int i = 0; i < databaseEntriesCount / 100; i++) {
        List<Delete> deletes = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
          deletes.add(Helpers.createDelete(rowKeyFromId(id), columnFamily1, qualifier1));
          id++;
        }
        deleteBatches.add(t.delete(deletes));
      }
      List<CompletableFuture<Void>> flatFutures =
          deleteBatches.stream().flatMap(List::stream).collect(Collectors.toList());
      CompletableFuture.allOf(flatFutures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < flatFutures.size(); i++) {
        checkIfShouldHaveThrown(flatFutures.get(i), rowKeyFromId(i));
      }

      assertThat(flatFutures.stream().filter(CompletableFuture::isCompletedExceptionally).count())
          .isEqualTo(flatFutures.stream().filter(f -> !f.isCompletedExceptionally()).count());
    }
    databaseHelpers.verifyTableConsistency(tableName2);
  }

  @Test
  public void testDeleteWithSecondaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    // Fill tables before forcing operations to fail.
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName2, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(
        failPredicate, HConstants.OperationStatusCode.BAD_FAMILY, "failed");

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext1 =
        new TestMirroringTable.ReportedErrorsContext();
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Void>> deleteFutures =
          IntStream.range(0, databaseEntriesCount)
              .mapToObj(
                  i -> t.delete(Helpers.createDelete(rowKeyFromId(i), columnFamily1, qualifier1)))
              .collect(Collectors.toList());

      CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0])).get();
    }
    assertThat(databaseHelpers.countRows(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(0);
    assertThat(databaseHelpers.countRows(tableName1, DatabaseHelpers.DatabaseSelector.SECONDARY))
        .isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext2 =
        new TestMirroringTable.ReportedErrorsContext();
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName2);

      List<List<CompletableFuture<Void>>> deleteBatches = new ArrayList<>();
      int id = 0;
      for (int i = 0; i < 10; i++) {
        List<Delete> deletes = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
          deletes.add(Helpers.createDelete(rowKeyFromId(id), columnFamily1, qualifier1));
          id++;
        }
        deleteBatches.add(t.delete(deletes));
      }
      CompletableFuture.allOf(
              deleteBatches.stream()
                  .flatMap(List::stream)
                  .collect(Collectors.toList())
                  .toArray(new CompletableFuture[0]))
          .get();
    }
    assertThat(databaseHelpers.countRows(tableName2, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(0);
    assertThat(databaseHelpers.countRows(tableName2, DatabaseHelpers.DatabaseSelector.SECONDARY))
        .isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext2.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testCheckAndPut() throws IOException, ExecutionException, InterruptedException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();

      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(
            t.checkAndMutate(rowKey, columnFamily1)
                .qualifier(qualifier1)
                .ifEquals(Longs.toByteArray(i))
                .thenPut(Helpers.createPut(i, columnFamily1, qualifier2)));
        futures.add(
            t.checkAndMutate(rowKey, columnFamily1)
                .qualifier(qualifier1)
                .ifMatches(CompareOperator.EQUAL, Longs.toByteArray(i))
                .thenPut(Helpers.createPut(i, columnFamily1, qualifier3)));
        futures.add(
            t.checkAndMutate(rowKey, columnFamily1)
                .qualifier(qualifier1)
                .ifMatches(CompareOperator.GREATER, Longs.toByteArray(i + 1))
                .thenPut(Helpers.createPut(i, columnFamily1, qualifier4)));
        futures.add(
            t.checkAndMutate(rowKey, columnFamily1)
                .qualifier(qualifier1)
                .ifMatches(CompareOperator.NOT_EQUAL, Longs.toByteArray(i))
                .thenPut(Helpers.createPut(i, columnFamily1, qualifier5)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }
    assertThat(databaseHelpers.countRows(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);
    assertThat(databaseHelpers.countCells(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount * 4);
    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndPutPrimaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();

      for (int i = 0; i < databaseEntriesCount; i++) {
        final byte[] rowKeyAndValue = rowKeyFromId(i);
        futures.add(
            t.checkAndMutate(rowKeyAndValue, columnFamily1)
                .qualifier(qualifier1)
                .ifEquals(rowKeyAndValue)
                .thenPut(Helpers.createPut(i, columnFamily1, qualifier2)));
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < futures.size(); i++) {
        checkIfShouldHaveThrown(futures.get(i), rowKeyFromId(i));
      }
    }
    assertThat(databaseHelpers.countRows(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);
    assertThat(databaseHelpers.countCells(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo((int) (databaseEntriesCount * 1.5));
    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndPutSecondaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(
        failPredicate, HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext1 =
        new TestMirroringTable.ReportedErrorsContext();
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();

      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKeyAndValue = rowKeyFromId(i);
        futures.add(
            t.checkAndMutate(rowKeyAndValue, columnFamily1)
                .qualifier(qualifier1)
                .ifEquals(rowKeyAndValue)
                .thenPut(Helpers.createPut(i, columnFamily1, qualifier2)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

      for (CompletableFuture<Boolean> fut : futures) {
        assertThat(fut.getNow(false)).isEqualTo(true);
      }
    }

    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseHelpers.DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier2)))
        .isEqualTo(databaseEntriesCount);
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseHelpers.DatabaseSelector.SECONDARY,
                Helpers.createScan(columnFamily1, qualifier2)))
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(databaseHelpers.countCells(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount * 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testCheckAndDelete() throws IOException, ExecutionException, InterruptedException {
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

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        assertThat(
                t.checkAndMutate(rowKey, columnFamily1)
                    .qualifier(qualifier1)
                    .ifEquals(Longs.toByteArray(i))
                    .thenDelete(
                        Helpers.createDelete(Longs.toByteArray(i), columnFamily1, qualifier2))
                    .get())
            .isTrue();
        assertThat(
                t.checkAndMutate(rowKey, columnFamily1)
                    .qualifier(qualifier1)
                    .ifMatches(CompareOperator.EQUAL, Longs.toByteArray(i))
                    .thenDelete(
                        Helpers.createDelete(Longs.toByteArray(i), columnFamily1, qualifier3))
                    .get())
            .isTrue();
        assertThat(
                t.checkAndMutate(rowKey, columnFamily1)
                    .qualifier(qualifier1)
                    .ifMatches(CompareOperator.GREATER, Longs.toByteArray(i + 1))
                    .thenDelete(
                        Helpers.createDelete(Longs.toByteArray(i), columnFamily1, qualifier4))
                    .get())
            .isTrue();
        assertThat(
                t.checkAndMutate(rowKey, columnFamily1)
                    .qualifier(qualifier1)
                    .ifMatches(CompareOperator.NOT_EQUAL, Longs.toByteArray(i))
                    .thenDelete(
                        Helpers.createDelete(Longs.toByteArray(i), columnFamily1, qualifier5))
                    .get())
            .isFalse();
      }
    }

    assertThat(databaseHelpers.countRows(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);

    assertThat(databaseHelpers.countCells(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount * 2);

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndDeletePrimaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(
        tableName1, databaseEntriesCount, columnFamily1, qualifier1, qualifier2);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Boolean>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        final byte[] rowKeyAndValue = rowKeyFromId(i);
        futures.add(
            t.checkAndMutate(rowKeyAndValue, columnFamily1)
                .qualifier(qualifier1)
                .ifEquals(rowKeyAndValue)
                .thenDelete(Helpers.createDelete(rowKeyAndValue, columnFamily1, qualifier2)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < futures.size(); i++) {
        checkIfShouldHaveThrown(futures.get(i), rowKeyFromId(i));
      }
    }

    assertThat(databaseHelpers.countRows(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);

    assertThat(databaseHelpers.countCells(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo((int) (databaseEntriesCount * 1.5));

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndDeleteSecondaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(
        failPredicate, HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext1 =
        new TestMirroringTable.ReportedErrorsContext();
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();

      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKeyAndValue = rowKeyFromId(i);
        futures.add(
            t.checkAndMutate(rowKeyAndValue, columnFamily1)
                .qualifier(qualifier1)
                .ifEquals(rowKeyAndValue)
                .thenDelete(Helpers.createDelete(rowKeyAndValue, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      for (CompletableFuture<Boolean> fut : futures) {
        assertThat(fut.getNow(false)).isEqualTo(true);
      }
    }
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseHelpers.DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(0);
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseHelpers.DatabaseSelector.SECONDARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testCheckAndMutate() throws IOException, ExecutionException, InterruptedException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        assertThat(
                t.checkAndMutate(rowKey, columnFamily1)
                    .qualifier(qualifier1)
                    .ifMatches(CompareOperator.EQUAL, Longs.toByteArray(i))
                    .thenMutate(
                        Helpers.createRowMutations(
                            rowKey,
                            Helpers.createPut(i, columnFamily1, qualifier2),
                            Helpers.createDelete(rowKey, columnFamily1, qualifier1)))
                    .get())
            .isTrue();
        assertThat(
                t.checkAndMutate(rowKey, columnFamily1)
                    .qualifier(qualifier1)
                    .ifMatches(CompareOperator.EQUAL, Longs.toByteArray(i))
                    .thenMutate(
                        Helpers.createRowMutations(
                            rowKey, Helpers.createDelete(rowKey, columnFamily1, qualifier2)))
                    .get())
            .isFalse();
      }
    }

    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseHelpers.DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(0);

    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseHelpers.DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier2)))
        .isEqualTo(databaseEntriesCount);

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testCheckAndMutatePrimaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(
        tableName1, databaseEntriesCount, columnFamily1, qualifier1, qualifier2);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        final byte[] rowKeyAndValue = rowKeyFromId(i);
        futures.add(
            t.checkAndMutate(rowKeyAndValue, columnFamily1)
                .qualifier(qualifier1)
                .ifMatches(CompareOperator.EQUAL, rowKeyAndValue)
                .thenMutate(
                    Helpers.createRowMutations(
                        rowKeyAndValue,
                        Helpers.createDelete(rowKeyAndValue, columnFamily1, qualifier2))));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < futures.size(); i++) {
        checkIfShouldHaveThrown(futures.get(i), rowKeyFromId(i));
      }
    }

    assertThat(databaseHelpers.countRows(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo(databaseEntriesCount);

    assertThat(databaseHelpers.countCells(tableName1, DatabaseHelpers.DatabaseSelector.PRIMARY))
        .isEqualTo((int) (databaseEntriesCount * 1.5));

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  // TODO(mwalkiewicz): fix
  @Ignore("Fails for unknown reasons")
  @Test
  public void testCheckAndMutateSecondaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(
        failPredicate, HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext1 =
        new TestMirroringTable.ReportedErrorsContext();
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();

      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKeyAndValue = rowKeyFromId(i);
        futures.add(
            t.checkAndMutate(rowKeyAndValue, columnFamily1)
                .qualifier(qualifier1)
                .ifMatches(CompareOperator.EQUAL, rowKeyAndValue)
                .thenMutate(
                    Helpers.createRowMutations(
                        rowKeyAndValue,
                        Helpers.createDelete(rowKeyAndValue, columnFamily1, qualifier1))));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

      for (CompletableFuture<Boolean> fut : futures) {
        assertThat(fut.getNow(false)).isEqualTo(true);
      }
    }
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseHelpers.DatabaseSelector.PRIMARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(0);
    assertThat(
            databaseHelpers.countRows(
                tableName1,
                DatabaseHelpers.DatabaseSelector.SECONDARY,
                Helpers.createScan(columnFamily1, qualifier1)))
        .isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testIncrement() throws IOException, ExecutionException, InterruptedException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Result>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(t.increment(Helpers.createIncrement(rowKey, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }

    databaseHelpers.verifyTableConsistency(tableName1);

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        Result r = t.get(Helpers.createGet(rowKey, columnFamily1, qualifier1)).get();
        assertThat(Longs.fromByteArray(r.getValue(columnFamily1, qualifier1))).isEqualTo(i + 1);
      }
    }
  }

  @Test
  public void testIncrementPrimaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Result>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(t.increment(Helpers.createIncrement(rowKey, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < futures.size(); i++) {
        checkIfShouldHaveThrown(futures.get(i), rowKeyFromId(i));
      }
    }

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testIncrementSecondaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    TestWriteErrorConsumer.clearErrors();

    FailingHBaseHRegion.failMutation(
        failPredicate, HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext1 =
        new TestMirroringTable.ReportedErrorsContext();
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Result>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(t.increment(Helpers.createIncrement(rowKey, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }

    assertThat(TestWriteErrorConsumer.getErrorCount()).isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testAppend() throws IOException, ExecutionException, InterruptedException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Result>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(
            t.append(Helpers.createAppend(rowKey, columnFamily1, qualifier1, new byte[] {1})));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }

    databaseHelpers.verifyTableConsistency(tableName1);

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        Result r = t.get(Helpers.createGet(rowKey, columnFamily1, qualifier1)).get();
        byte[] expectedValue = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 1};
        System.arraycopy(rowKey, 0, expectedValue, 0, 8);
        assertThat(r.getValue(columnFamily1, qualifier1)).isEqualTo(expectedValue);
      }
    }
  }

  @Test
  public void testAppendPrimaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Result>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(
            t.append(Helpers.createAppend(rowKey, columnFamily1, qualifier1, new byte[] {1})));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();
      for (int i = 0; i < futures.size(); i++) {
        checkIfShouldHaveThrown(futures.get(i), rowKeyFromId(i));
      }
    }

    databaseHelpers.verifyTableConsistency(tableName1);
  }

  @Test
  public void testAppendSecondaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    TestWriteErrorConsumer.clearErrors();

    FailingHBaseHRegion.failMutation(
        failPredicate, HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, "failed");

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext1 =
        new TestMirroringTable.ReportedErrorsContext();
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Result>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(
            t.append(Helpers.createAppend(rowKey, columnFamily1, qualifier1, new byte[] {1})));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }

    assertThat(TestWriteErrorConsumer.getErrorCount()).isEqualTo(databaseEntriesCount / 2);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testGet() throws IOException, ExecutionException, InterruptedException {
    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Result>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(t.get(Helpers.createGet(rowKey, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount()).isEqualTo(0);
  }

  @Test
  public void testGetWithPrimaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Result>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(t.get(Helpers.createGet(rowKey, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < futures.size(); i++) {
        checkIfShouldHaveThrown(futures.get(i), rowKeyFromId(i));
      }
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount()).isEqualTo(0);
  }

  @Test
  public void testGetWithSecondaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Result>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(t.get(Helpers.createGet(rowKey, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount())
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount("failure"))
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount("mismatch")).isEqualTo(0);
  }

  @Test
  public void testExists() throws IOException, ExecutionException, InterruptedException {
    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(t.exists(Helpers.createGet(rowKey, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      assertThat(futures.stream().allMatch(fut -> fut.getNow(false))).isTrue();
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount()).isEqualTo(0);
  }

  @Test
  public void testExistsWithPrimaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(t.exists(Helpers.createGet(rowKey, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < futures.size(); i++) {
        checkIfShouldHaveThrown(futures.get(i), rowKeyFromId(i));
      }
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount()).isEqualTo(0);
  }

  @Test
  public void testExistsWithSecondaryErrors()
      throws IOException, ExecutionException, InterruptedException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;
    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName1, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName1);
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();
      for (int i = 0; i < databaseEntriesCount; i++) {
        byte[] rowKey = rowKeyFromId(i);
        futures.add(t.exists(Helpers.createGet(rowKey, columnFamily1, qualifier1)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      assertThat(futures.stream().allMatch(fut -> fut.getNow(false))).isTrue();
    }
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount())
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount("failure"))
        .isEqualTo(databaseEntriesCount / 2);
    assertThat(MismatchDetectorCounter.getInstance().getErrorCount("mismatch")).isEqualTo(0);
  }

  @Test
  public void testBatch() throws IOException, InterruptedException, ExecutionException {
    int databaseEntriesCount = 1000;

    final TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName);

      List<List<CompletableFuture<Void>>> batches = new ArrayList<>();
      int id = 0;
      while (id < databaseEntriesCount) {
        List<Put> batch = new ArrayList<>();
        for (int j = 0; j < 100 && id < databaseEntriesCount; j++) {
          batch.add(Helpers.createPut(id, columnFamily1, qualifier1));
          id++;
        }
        batches.add(t.batch(batch));
      }
      List<CompletableFuture<Void>> flatResults =
          batches.stream().flatMap(List::stream).collect(Collectors.toList());
      CompletableFuture.allOf(flatResults.toArray(new CompletableFuture[0])).get();
    }
    databaseHelpers.verifyTableConsistency(tableName);
  }

  @Test
  public void testBatchWithPrimaryErrors()
      throws IOException, InterruptedException, ExecutionException {
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;
    FailingHBaseHRegion.failMutation(failPredicate, "failed");
    final TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName);

      List<List<CompletableFuture<Void>>> batches = new ArrayList<>();
      int id = 0;
      while (id < databaseEntriesCount) {
        List<Put> batch = new ArrayList<>();
        for (int j = 0; j < 100 && id < databaseEntriesCount; j++) {
          batch.add(Helpers.createPut(id, columnFamily1, qualifier1));
          id++;
        }
        batches.add(t.batch(batch));
      }
      List<CompletableFuture<Void>> flatResults =
          batches.stream().flatMap(List::stream).collect(Collectors.toList());
      CompletableFuture.allOf(flatResults.toArray(new CompletableFuture[0]))
          .exceptionally(e -> null)
          .get();

      for (int i = 0; i < flatResults.size(); i++) {
        checkIfShouldHaveThrown(flatResults.get(i), rowKeyFromId(i));
      }
    }
    databaseHelpers.verifyTableConsistency(tableName);
  }

  @Test
  public void testBatchWithSecondaryErrors()
      throws IOException, InterruptedException, ExecutionException {
    Assume.assumeTrue(
        ConfigurationHelper.isSecondaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    int databaseEntriesCount = 1000;

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    TestMirroringTable.ReportedErrorsContext reportedErrorsContext1 =
        new TestMirroringTable.ReportedErrorsContext();
    final TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName);

      List<List<CompletableFuture<Void>>> batches = new ArrayList<>();
      int id = 0;
      while (id < databaseEntriesCount) {
        List<Put> batch = new ArrayList<>();
        for (int j = 0; j < 100 && id < databaseEntriesCount; j++) {
          batch.add(Helpers.createPut(id, columnFamily1, qualifier1));
          id++;
        }
        batches.add(t.batch(batch));
      }
      List<CompletableFuture<Void>> flatResults =
          batches.stream().flatMap(List::stream).collect(Collectors.toList());
      CompletableFuture.allOf(flatResults.toArray(new CompletableFuture[0])).get();
    }
    databaseHelpers.verifyTableConsistency(tableName, failPredicate);
    reportedErrorsContext1.assertNewErrorsReported(databaseEntriesCount / 2);
  }

  @Test
  public void testResultScanner() throws IOException {
    int databaseEntriesCount = 1000;

    TableName tableName = connectionRule.createTable(columnFamily1);
    databaseHelpers.fillTable(tableName, databaseEntriesCount, columnFamily1, qualifier1);

    FailingHBaseHRegion.failMutation(failPredicate, "failed");

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> table = asyncConnection.getTable(tableName);

      try (ResultScanner scanner = table.getScanner(columnFamily1)) {
        assertThat(Iterators.size(scanner.iterator())).isEqualTo(databaseEntriesCount);
      }
    }
  }

  private void checkIfShouldHaveThrown(CompletableFuture<?> future, byte[] rowKey) {
    assertThat(failPredicate.apply(rowKey)).isEqualTo(future.isCompletedExceptionally());
  }
}
