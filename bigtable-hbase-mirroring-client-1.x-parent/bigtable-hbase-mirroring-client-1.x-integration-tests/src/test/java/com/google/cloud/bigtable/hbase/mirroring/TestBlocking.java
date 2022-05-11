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

import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.ExecutorServiceRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.SlowMismatchDetector;
import com.google.cloud.bigtable.hbase.mirroring.utils.ZipkinTracingRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBlocking {
  static final byte[] columnFamily1 = "cf1".getBytes();
  static final byte[] qualifier1 = "q1".getBytes();
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();
  @ClassRule public static ZipkinTracingRule zipkinTracingRule = new ZipkinTracingRule();
  @Rule public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();
  public DatabaseHelpers databaseHelpers = new DatabaseHelpers(connectionRule, executorServiceRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  @Test
  public void testConnectionCloseBlocksUntilAllRequestsHaveBeenVerified()
      throws IOException, InterruptedException {
    long beforeTableClose;
    long afterTableClose;
    long afterConnectionClose;

    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(MIRRORING_MISMATCH_DETECTOR_CLASS, SlowMismatchDetector.class.getCanonicalName());
    SlowMismatchDetector.sleepTime = 1000;

    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      try (Table t = connection.getTable(tableName)) {
        for (int i = 0; i < 10; i++) {
          Get get = new Get("1".getBytes());
          get.addColumn(columnFamily1, qualifier1);
          t.get(get);
        }
        beforeTableClose = System.currentTimeMillis();
      }
      afterTableClose = System.currentTimeMillis();
    }
    afterConnectionClose = System.currentTimeMillis();
    long tableCloseDuration = afterTableClose - beforeTableClose;
    long connectionCloseDuration = afterConnectionClose - afterTableClose;
    assertThat(tableCloseDuration).isLessThan(100);
    assertThat(connectionCloseDuration).isGreaterThan(900);
    assertThat(MismatchDetectorCounter.getInstance().getVerificationsStartedCounter())
        .isEqualTo(10);
    assertThat(MismatchDetectorCounter.getInstance().getVerificationsFinishedCounter())
        .isEqualTo(10);
  }

  @Test
  public void testSlowSecondaryConnection() throws IOException {
    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(MIRRORING_MISMATCH_DETECTOR_CLASS, SlowMismatchDetector.class.getCanonicalName());
    SlowMismatchDetector.sleepTime = 100;
    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "10");
    TableName tableName;
    byte[] row = "1".getBytes();
    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      try (Table table = connection.getTable(tableName)) {
        table.put(Helpers.createPut(row, columnFamily1, qualifier1, "1".getBytes()));
      }
    }

    long startTime;
    long endTime;
    long duration;

    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      startTime = System.currentTimeMillis();
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 1000; i++) {
          table.get(Helpers.createGet(row, columnFamily1, qualifier1));
        }
      }
    }
    endTime = System.currentTimeMillis();
    duration = endTime - startTime;
    // 1000 requests * 100 ms / 10 concurrent requests
    assertThat(duration).isGreaterThan(10000);

    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "50");
    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      startTime = System.currentTimeMillis();
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 1000; i++) {
          table.get(Helpers.createGet(row, columnFamily1, qualifier1));
        }
      }
    }
    endTime = System.currentTimeMillis();
    duration = endTime - startTime;
    // 1000 requests * 100 ms / 50 concurrent requests
    assertThat(duration).isGreaterThan(2000);

    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "1000");
    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      startTime = System.currentTimeMillis();
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 1000; i++) {
          table.get(Helpers.createGet(row, columnFamily1, qualifier1));
        }
      }
    }
    endTime = System.currentTimeMillis();
    duration = endTime - startTime;
    // 1000 requests * 100 ms / 1000 concurrent requests
    assertThat(duration).isLessThan(1000);
  }
}
