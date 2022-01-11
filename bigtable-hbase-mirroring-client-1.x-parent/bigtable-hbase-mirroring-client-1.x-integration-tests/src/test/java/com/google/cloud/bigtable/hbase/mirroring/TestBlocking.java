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

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_CONNECTION_CONNECTION_TERMINATION_TIMEOUT;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_STRATEGY_FACTORY_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.hbase.mirroring.utils.BlockingFlowControllerStrategy;
import com.google.cloud.bigtable.hbase.mirroring.utils.BlockingMismatchDetector;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestMismatchDetectorCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBlocking {
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();
  @Rule public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();
  private DatabaseHelpers databaseHelpers =
      new DatabaseHelpers(connectionRule, executorServiceRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  private static final byte[] columnFamily1 = "cf1".getBytes();
  private static final byte[] qualifier1 = "q1".getBytes();

  private TableName tableName;

  @Before
  public void setUp() throws IOException {
    this.tableName = connectionRule.createTable(columnFamily1);
  }

  @Test(timeout = 10000)
  public void testConnectionCloseBlocksUntilAllRequestsHaveBeenVerified()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(
        MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS,
        BlockingMismatchDetector.Factory.class.getName());
    BlockingMismatchDetector.reset();

    TableName tableName;
    final MirroringConnection connection = databaseHelpers.createConnection(config);
    tableName = connectionRule.createTable(connection, columnFamily1);
    try (Table t = connection.getTable(tableName)) {
      for (int i = 0; i < 10; i++) {
        Get get = new Get("1".getBytes());
        get.addColumn(columnFamily1, qualifier1);
        t.get(get);
      }
    } // There are in-flight requests but closing a Table object shouldn't block.

    final SettableFuture<Void> closingThreadStarted = SettableFuture.create();
    final SettableFuture<Void> closingThreadEnded = SettableFuture.create();

    Thread closingThread =
        new Thread() {
          @Override
          public void run() {
            try {
              closingThreadStarted.set(null);
              connection.close();
              closingThreadEnded.set(null);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
    closingThread.start();

    // Wait until closing thread starts.
    closingThreadStarted.get(1, TimeUnit.SECONDS);

    // And give it some time to run, to verify that is has blocked.
    try {
      closingThreadEnded.get(5, TimeUnit.SECONDS);
      fail("should throw");
    } catch (TimeoutException ignored) {
      // expected
    }

    // Finish running verifications
    BlockingMismatchDetector.unblock();

    // And now Connection#close() should unblock.
    closingThreadEnded.get(1, TimeUnit.SECONDS);

    // And all verification should have finished.
    assertThat(TestMismatchDetectorCounter.getInstance().getVerificationsFinishedCounter())
        .isEqualTo(10);
  }

  @Test(timeout = 10000)
  public void flowControllerBlocksScheduling()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(
        MIRRORING_FLOW_CONTROLLER_STRATEGY_FACTORY_CLASS,
        BlockingFlowControllerStrategy.Factory.class.getName());
    BlockingFlowControllerStrategy.reset();

    final byte[] row = "1".getBytes();
    final SettableFuture<Void> closingThreadStarted = SettableFuture.create();
    final SettableFuture<Void> closingThreadEnded = SettableFuture.create();

    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      try (Table table = connection.getTable(tableName)) {
        Thread t =
            new Thread() {
              @Override
              public void run() {
                closingThreadStarted.set(null);
                try {
                  table.put(Helpers.createPut(row, columnFamily1, qualifier1, "1".getBytes()));
                  closingThreadEnded.set(null);
                } catch (IOException e) {
                  closingThreadEnded.setException(e);
                  throw new RuntimeException(e);
                }
              }
            };
        t.start();

        // Wait until thread starts.
        closingThreadStarted.get(1, TimeUnit.SECONDS);

        // Give it some time to run, to verify that is has blocked.
        try {
          closingThreadEnded.get(5, TimeUnit.SECONDS);
          fail("should throw");
        } catch (TimeoutException ignored) {
          // expected
        }
        // Unlock flow controller.
        BlockingFlowControllerStrategy.unblock();
        // And verify that it has unblocked.
        closingThreadEnded.get(1, TimeUnit.SECONDS);
      }
    }
  }

  @Test(timeout = 10000)
  public void testMirroringConnectionCloseTimeout()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    long timeoutMillis = 1000;

    final Configuration config = ConfigurationHelper.newConfiguration();
    config.set(MIRRORING_CONNECTION_CONNECTION_TERMINATION_TIMEOUT, String.valueOf(timeoutMillis));
    config.set(
        MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS,
        BlockingMismatchDetector.Factory.class.getName());
    BlockingMismatchDetector.reset();

    final byte[] row = "1".getBytes();

    final TableName tableName = connectionRule.createTable(columnFamily1);
    final SettableFuture<MirroringConnection> closingThreadStartedFuture = SettableFuture.create();
    final SettableFuture<Long> closingThreadFinishedFuture = SettableFuture.create();

    Thread t =
        new Thread() {
          @Override
          public void run() {
            try {
              // Not in try-with-resources, we are calling close() explicitly.
              MirroringConnection connection = databaseHelpers.createConnection(config);
              Table table = connection.getTable(tableName);
              table.get(Helpers.createGet(row, columnFamily1, qualifier1));
              table.close();

              closingThreadStartedFuture.set(connection);
              Stopwatch stopwatch = Stopwatch.createStarted();
              connection.close();
              stopwatch.stop();
              closingThreadFinishedFuture.set(stopwatch.elapsed(TimeUnit.MILLISECONDS));
            } catch (IOException e) {
              closingThreadFinishedFuture.setException(e);
            }
          }
        };

    t.start();

    // Wait until the thread starts.
    MirroringConnection c = closingThreadStartedFuture.get(1, TimeUnit.SECONDS);
    // And wait for it to finish. It should time-out after 1 second.
    long closeDuration = closingThreadFinishedFuture.get(3, TimeUnit.SECONDS);
    // Just knowing that the future did not time out is enough to know that it closing the
    // connection lasted no longer than 3 seconds, but we also need to check that it waited at least
    // `timeoutMillis`. `closeDuration` is strictly greater than timeout because it includes some
    // overhead, but `timeoutMillis` >> expected overhead, thus false-positives are unlikely.
    assertThat(closeDuration).isAtLeast(timeoutMillis);
    assertThat(c.getPrimaryConnection().isClosed()).isTrue();
    assertThat(c.getSecondaryConnection().isClosed()).isFalse();

    // Finish asynchronous operation.
    BlockingMismatchDetector.unblock();
    // Give it a second to run.
    Thread.sleep(1000);
    assertThat(c.getPrimaryConnection().isClosed()).isTrue();
    assertThat(c.getSecondaryConnection().isClosed()).isTrue();
  }
}
