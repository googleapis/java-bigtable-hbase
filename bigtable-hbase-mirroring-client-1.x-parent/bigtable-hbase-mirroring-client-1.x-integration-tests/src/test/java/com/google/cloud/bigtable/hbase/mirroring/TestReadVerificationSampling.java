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
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.ExecutorServiceRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegionRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestReadVerificationSampling {

  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();

  @Rule public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();
  @Rule public FailingHBaseHRegionRule failingHBaseHRegionRule = new FailingHBaseHRegionRule();
  public DatabaseHelpers databaseHelpers = new DatabaseHelpers(connectionRule, executorServiceRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  static final byte[] family1 = "cf1".getBytes();
  static final byte[] qualifier1 = "cq1".getBytes();

  @Test
  public void testPartialReadsVerificationOnGets() throws IOException {
    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, family1);
      databaseHelpers.fillTable(tableName, 1000, family1, qualifier1);
    }

    Configuration configuration = ConfigurationHelper.newConfiguration();
    configuration.set("google.bigtable.mirroring.read-verification-rate", "1");

    try (MirroringConnection connection = databaseHelpers.createConnection(configuration)) {
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 500; i++) {
          int index = (i % 100) * 10;
          table.get(Helpers.createGet(Longs.toByteArray(index), family1, qualifier1));
          table.get(
              ImmutableList.of(
                  Helpers.createGet(Longs.toByteArray(index + 1), family1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 2), family1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 3), family1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 4), family1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 5), family1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 6), family1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 7), family1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 8), family1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 9), family1, qualifier1)));
        }
      }
    }

    assertThat(MismatchDetectorCounter.getInstance().getVerificationsFinishedCounter())
        .isLessThan(20);
  }

  @Test
  public void testAllReadsVerificationOnGets() throws IOException {
    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, family1);
      databaseHelpers.fillTable(tableName, 10, family1, qualifier1);
    }

    Configuration configuration = ConfigurationHelper.newConfiguration();
    configuration.set("google.bigtable.mirroring.read-verification-rate", "100");

    try (MirroringConnection connection = databaseHelpers.createConnection(configuration)) {
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 10; i++) {
          table.get(Helpers.createGet(Longs.toByteArray(i), family1, qualifier1));
        }
      }
    }

    assertEquals(10, MismatchDetectorCounter.getInstance().getVerificationsFinishedCounter());
  }

  @Test
  public void testNoReadsVerificationOnGets() throws IOException {
    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, family1);
      databaseHelpers.fillTable(tableName, 10, family1, qualifier1);
    }

    Configuration configuration = ConfigurationHelper.newConfiguration();
    configuration.set("google.bigtable.mirroring.read-verification-rate", "0");

    try (MirroringConnection connection = databaseHelpers.createConnection(configuration)) {
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 10; i++) {
          table.get(Helpers.createGet(Longs.toByteArray(i), family1, qualifier1));
        }
      }
    }

    assertEquals(0, MismatchDetectorCounter.getInstance().getVerificationsFinishedCounter());
  }
}
