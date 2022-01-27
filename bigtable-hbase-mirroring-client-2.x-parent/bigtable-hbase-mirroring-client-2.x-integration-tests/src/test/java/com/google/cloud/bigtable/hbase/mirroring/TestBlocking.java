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

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_STRATEGY_FACTORY_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.hbase.mirroring.utils.AsyncConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.BlockingFlowControllerStrategy;
import com.google.cloud.bigtable.hbase.mirroring.utils.BlockingMismatchDetector;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestMismatchDetectorCounter;
import com.google.cloud.bigtable.mirroring.hbase2_x.MirroringAsyncConnection;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Get;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class TestBlocking {

  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();

  @ClassRule
  public static AsyncConnectionRule asyncConnectionRule = new AsyncConnectionRule(connectionRule);

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

  @Test(timeout = 15000)
  public void testConnectionCloseBlocksUntilAllRequestsHaveBeenVerified()
      throws InterruptedException, TimeoutException, ExecutionException {
    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(
        MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS,
        BlockingMismatchDetector.Factory.class.getName());
    BlockingMismatchDetector.reset();

    final MirroringAsyncConnection connection = asyncConnectionRule.createAsyncConnection(config);
    AsyncTable<?> t = connection.getTable(tableName);
    final List<CompletableFuture<?>> getFutures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Get get = new Get("1".getBytes());
      get.addColumn(columnFamily1, qualifier1);
      getFutures.add(t.get(get));
    }

    final SettableFuture<Void> closingThreadStarted = SettableFuture.create();
    final SettableFuture<Void> closingThreadEnded = SettableFuture.create();

    Thread closingThread =
        new Thread(
            () -> {
              try {
                closingThreadStarted.set(null);
                CompletableFuture.allOf(getFutures.toArray(new CompletableFuture[0])).get();
                connection.close();
                closingThreadEnded.set(null);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
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

    try (MirroringAsyncConnection connection = asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable table = connection.getTable(tableName);
      Thread t =
          new Thread(
              () -> {
                closingThreadStarted.set(null);
                try {
                  table
                      .put(Helpers.createPut(row, columnFamily1, qualifier1, "1".getBytes()))
                      .get();
                  closingThreadEnded.set(null);
                } catch (InterruptedException | ExecutionException e) {
                  closingThreadEnded.setException(e);
                  throw new RuntimeException(e);
                }
              });
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
