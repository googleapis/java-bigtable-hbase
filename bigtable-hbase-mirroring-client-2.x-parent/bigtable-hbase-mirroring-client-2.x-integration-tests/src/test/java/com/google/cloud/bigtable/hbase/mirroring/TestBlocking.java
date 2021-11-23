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

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_MISMATCH_DETECTOR_CLASS;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.hbase.mirroring.utils.AsyncConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.ExecutorServiceRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.SlowMismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase2_x.MirroringAsyncConnection;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Get;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class TestBlocking {
  static final byte[] columnFamily1 = "cf1".getBytes();
  static final byte[] qualifier1 = "q1".getBytes();
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();

  @ClassRule
  public static AsyncConnectionRule asyncConnectionRule = new AsyncConnectionRule(connectionRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  @Rule public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();
  public DatabaseHelpers databaseHelpers = new DatabaseHelpers(connectionRule, executorServiceRule);

  @Test
  public void testConnectionCloseBlocksUntilAllRequestsHaveBeenVerified() throws IOException {
    long beforeConnectionClose;
    long afterConnectionClose;

    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(MIRRORING_MISMATCH_DETECTOR_CLASS, SlowMismatchDetector.class.getCanonicalName());
    SlowMismatchDetector.sleepTime = 1000;

    TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName);
      for (int i = 0; i < 10; i++) {
        Get get = new Get("1".getBytes());
        get.addColumn(columnFamily1, qualifier1);
        t.get(get);
      }
      beforeConnectionClose = System.currentTimeMillis();
    }
    afterConnectionClose = System.currentTimeMillis();
    long connectionCloseDuration = afterConnectionClose - beforeConnectionClose;
    assertThat(connectionCloseDuration).isGreaterThan(900);
    assertThat(MismatchDetectorCounter.getInstance().getVerificationsStartedCounter())
        .isEqualTo(10);
    assertThat(MismatchDetectorCounter.getInstance().getVerificationsFinishedCounter())
        .isEqualTo(10);
  }

  @Test
  public void testSlowSecondaryConnection()
      throws IOException, ExecutionException, InterruptedException {
    // TODO(mwalkiewicz): fix BigtableToHBase2
    Assume.assumeTrue(
        ConfigurationHelper.isPrimaryHBase() && ConfigurationHelper.isUsingHBaseMiniCluster());

    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(MIRRORING_MISMATCH_DETECTOR_CLASS, SlowMismatchDetector.class.getCanonicalName());
    SlowMismatchDetector.sleepTime = 100;
    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "10");
    TableName tableName = connectionRule.createTable(columnFamily1);
    byte[] row = "1".getBytes();
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      asyncConnection
          .getTable(tableName)
          .put(Helpers.createPut(row, columnFamily1, qualifier1, "1".getBytes()))
          .get();
    }

    long startTime;
    long endTime;
    long duration;

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      startTime = System.currentTimeMillis();
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName);
      for (int i = 0; i < 1000; i++) {
        t.get(Helpers.createGet(row, columnFamily1, qualifier1));
      }
    }
    endTime = System.currentTimeMillis();
    duration = endTime - startTime;
    // 1000 requests * 100 ms / 10 concurrent requests
    assertThat(duration).isGreaterThan(10000);

    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "50");
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      startTime = System.currentTimeMillis();
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName);
      for (int i = 0; i < 1000; i++) {
        t.get(Helpers.createGet(row, columnFamily1, qualifier1));
      }
    }
    endTime = System.currentTimeMillis();
    duration = endTime - startTime;
    // 1000 requests * 100 ms / 50 concurrent requests
    assertThat(duration).isGreaterThan(2000);

    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "1000");
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      startTime = System.currentTimeMillis();
      AsyncTable<AdvancedScanResultConsumer> t = asyncConnection.getTable(tableName);
      for (int i = 0; i < 1000; i++) {
        t.get(Helpers.createGet(row, columnFamily1, qualifier1));
      }
    }
    endTime = System.currentTimeMillis();
    duration = endTime - startTime;
    // 1000 requests * 100 ms / 1000 concurrent requests
    assertThat(duration).isLessThan(1000);
  }
}
