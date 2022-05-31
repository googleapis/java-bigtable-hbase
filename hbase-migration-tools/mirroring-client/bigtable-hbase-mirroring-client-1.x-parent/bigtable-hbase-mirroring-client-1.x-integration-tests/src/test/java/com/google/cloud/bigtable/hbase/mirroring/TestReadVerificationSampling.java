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
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestMismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegionRule;
import com.google.cloud.bigtable.mirroring.core.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.core.MirroringConnection;
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
  @Rule public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();
  private DatabaseHelpers databaseHelpers =
      new DatabaseHelpers(connectionRule, executorServiceRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  @Rule public FailingHBaseHRegionRule failingHBaseHRegionRule = new FailingHBaseHRegionRule();

  private static final byte[] columnFamily1 = "cf1".getBytes();
  private static final byte[] qualifier1 = "cq1".getBytes();

  @Test
  public void testPartialReadsVerificationOnGets() throws IOException {
    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      databaseHelpers.fillTable(tableName, 1000, columnFamily1, qualifier1);
    }

    Configuration configuration = ConfigurationHelper.newConfiguration();
    configuration.set("google.bigtable.mirroring.read-verification-rate-percent", "1");

    try (MirroringConnection connection = databaseHelpers.createConnection(configuration)) {
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 500; i++) {
          int index = (i % 100) * 10;
          table.get(Helpers.createGet(Longs.toByteArray(index), columnFamily1, qualifier1));
          table.get(
              ImmutableList.of(
                  Helpers.createGet(Longs.toByteArray(index + 1), columnFamily1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 2), columnFamily1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 3), columnFamily1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 4), columnFamily1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 5), columnFamily1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 6), columnFamily1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 7), columnFamily1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 8), columnFamily1, qualifier1),
                  Helpers.createGet(Longs.toByteArray(index + 9), columnFamily1, qualifier1)));
        }
      }
    }

    // ReadSampler decides whether to verify read per whole request (e.g. it verifies all Gets in a
    // batch or none). We sent 1000 requests. None of them should fail or be a mismatch.
    // Our ReadSampler is probabilistic. We have a 0.01 chance of verifying a request.
    // Assuming that our random number generator really is random and there are no unexpected
    // errors, probability that this counter is at least 25 is about 0.000042.
    assertThat(TestMismatchDetectorCounter.getInstance().getVerificationsFinishedCounter())
        .isLessThan(25);
  }

  @Test
  public void testAllReadsVerificationOnGets() throws IOException {
    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      databaseHelpers.fillTable(tableName, 10, columnFamily1, qualifier1);
    }

    Configuration configuration = ConfigurationHelper.newConfiguration();
    configuration.set("google.bigtable.mirroring.read-verification-rate-percent", "100");

    try (MirroringConnection connection = databaseHelpers.createConnection(configuration)) {
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 10; i++) {
          table.get(Helpers.createGet(Longs.toByteArray(i), columnFamily1, qualifier1));
        }
      }
    }

    assertEquals(10, TestMismatchDetectorCounter.getInstance().getVerificationsFinishedCounter());
  }

  @Test
  public void testNoReadsVerificationOnGets() throws IOException {
    TableName tableName;
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      tableName = connectionRule.createTable(connection, columnFamily1);
      databaseHelpers.fillTable(tableName, 10, columnFamily1, qualifier1);
    }

    Configuration configuration = ConfigurationHelper.newConfiguration();
    configuration.set("google.bigtable.mirroring.read-verification-rate-percent", "0");

    try (MirroringConnection connection = databaseHelpers.createConnection(configuration)) {
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 10; i++) {
          table.get(Helpers.createGet(Longs.toByteArray(i), columnFamily1, qualifier1));
        }
      }
    }

    assertEquals(0, TestMismatchDetectorCounter.getInstance().getVerificationsFinishedCounter());
  }
}
