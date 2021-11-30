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
package com.google.cloud.bigtable.mirroring.core;

import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class TestMirroringConnectionClosing {
  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.spyedSingleThreadedExecutor();

  private Configuration createConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set("hbase.client.connection.impl", MirroringConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    // Prefix keys have to be set because we are using the same class as primary and secondary
    // connection class.
    configuration.set(MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "primary-connection");
    configuration.set(MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "secondary-connection");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.prefix-path", "/tmp/test-");
    configuration.set("google.bigtable.mirroring.write-error-log.appender.max-buffer-size", "1024");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.drop-on-overflow", "false");
    return configuration;
  }

  MirroringConnection mirroringConnection;
  MirroringTable mirroringTable;
  MirroringResultScanner mirroringScanner;

  @Before
  public void setUp() throws IOException {
    TestConnection.reset();
    Configuration configuration = createConfiguration();

    mirroringConnection =
        spy(
            (MirroringConnection)
                ConnectionFactory.createConnection(
                    configuration, executorServiceRule.executorService));
    assertThat(TestConnection.connectionMocks.size()).isEqualTo(2);

    mirroringTable = (MirroringTable) mirroringConnection.getTable(TableName.valueOf("test"));
    mirroringScanner = (MirroringResultScanner) mirroringTable.getScanner(new Scan());
  }

  @Test
  public void testUnderlingObjectsAreClosedInCorrectOrder()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final SettableFuture<Void> unblockSecondaryScanner = SettableFuture.create();
    final SettableFuture<Void> scannerAndTableClosed = SettableFuture.create();
    final SettableFuture<Void> closeFinished = SettableFuture.create();
    TestHelpers.blockMethodCall(TestConnection.scannerMocks.get(1), unblockSecondaryScanner).next();

    // We expect secondary objects to be closed in correct order - from the innermost to the
    // outermost.
    // TestConnection object is created for each both primary and secondary, that connections,
    // tables and scanners created using those connections are stored in static *Mocks field of
    // TestConnection, in order of creation.
    // Thus, `TestConnection.connectionMocks.get(1)` is secondary connection mock,
    // `TestConnection.tableMocks.get(1)` is a table created using this connection,
    // and `TestConnection.scannerMocks.get(1)` is a scanner created using this table.
    InOrder inOrder =
        Mockito.inOrder(
            TestConnection.scannerMocks.get(1),
            TestConnection.tableMocks.get(1),
            TestConnection.connectionMocks.get(1));

    Thread t =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  mirroringScanner.next();

                  mirroringScanner.close();
                  mirroringTable.close();

                  scannerAndTableClosed.set(null);
                  mirroringConnection.close();
                  closeFinished.set(null);
                } catch (Exception e) {
                  closeFinished.setException(e);
                }
              }
            });
    t.start();

    // Wait until secondary request is scheduled.
    scannerAndTableClosed.get(5, TimeUnit.SECONDS);
    // Give mirroringConnection.close() some time to run
    Thread.sleep(3000);
    // and verify that it was called.
    verify(mirroringConnection).close();

    // Finish async call.
    unblockSecondaryScanner.set(null);
    // The close() should finish.
    closeFinished.get(5, TimeUnit.SECONDS);
    t.join();

    executorServiceRule.waitForExecutor();

    inOrder.verify(TestConnection.scannerMocks.get(1)).close();
    inOrder.verify(TestConnection.tableMocks.get(1)).close();
    inOrder.verify(TestConnection.connectionMocks.get(1)).close();

    assertThat(mirroringConnection.isClosed()).isTrue();
    verify(TestConnection.connectionMocks.get(0), times(1)).close();
    verify(TestConnection.tableMocks.get(0), times(1)).close();
    verify(TestConnection.scannerMocks.get(0), times(1)).close();
  }

  @Test(timeout = 5000)
  public void testClosingConnectionWithoutClosingUnderlyingObjectsShouldntBlock()
      throws IOException {
    // We have created a connection, table and scanner.
    // They are not use asynchronously now, thus connection should be closed without delay.
    mirroringConnection.close();
    verify(TestConnection.connectionMocks.get(0)).close();
    verify(TestConnection.connectionMocks.get(1)).close();
  }

  @Test
  public void testInFlightRequestBlockClosingConnection()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    final SettableFuture<Void> unblockSecondaryScanner = SettableFuture.create();
    final SettableFuture<Void> asyncScheduled = SettableFuture.create();
    final SettableFuture<Void> closeFinished = SettableFuture.create();
    TestHelpers.blockMethodCall(TestConnection.scannerMocks.get(1), unblockSecondaryScanner).next();

    Thread t =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  mirroringScanner.next();

                  // Not calling close on scanner nor on table.
                  asyncScheduled.set(null);

                  mirroringConnection.close();
                  closeFinished.set(null);
                } catch (Exception e) {
                  closeFinished.setException(e);
                }
              }
            });
    t.start();

    // Wait until secondary request is scheduled.
    asyncScheduled.get(5, TimeUnit.SECONDS);
    // Give mirroringConnection.close() some time to run
    Thread.sleep(3000);
    // and verify that it was called.
    verify(mirroringConnection).close();

    // Finish async call.
    unblockSecondaryScanner.set(null);
    // The close() should finish even though we didn't close scanner nor table.
    closeFinished.get(5, TimeUnit.SECONDS);
    t.join();
  }

  @Test
  public void testConnectionWaitsForAsynchronousClose()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    final SettableFuture<Void> unblockScannerNext = SettableFuture.create();
    final SettableFuture<Void> unblockScannerClose = SettableFuture.create();
    final SettableFuture<Void> asyncScheduled = SettableFuture.create();
    final SettableFuture<Void> closeFinished = SettableFuture.create();
    TestHelpers.blockMethodCall(TestConnection.scannerMocks.get(1), unblockScannerNext).next();
    TestHelpers.blockMethodCall(TestConnection.scannerMocks.get(1), unblockScannerClose).close();

    Thread t =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  mirroringScanner.next();
                  mirroringScanner.close();
                  asyncScheduled.set(null);
                  mirroringConnection.close();
                  closeFinished.set(null);
                } catch (Exception e) {
                  closeFinished.setException(e);
                }
              }
            });
    t.start();

    // Wait until secondary request is scheduled.
    asyncScheduled.get(5, TimeUnit.SECONDS);
    // Unblock scanner next.
    unblockScannerNext.set(null);
    // Give mirroringConnection.close() and secondaryScanner.close() some time to run
    Thread.sleep(1000);
    // and verify that they were called.
    verify(mirroringConnection).close();
    verify(TestConnection.scannerMocks.get(1)).close();

    // secondary.close() was not yet finished, close should be blocked.
    try {
      closeFinished.get(2, TimeUnit.SECONDS);
      fail();
    } catch (TimeoutException expected) {
      // async operation has not finished - close should block.
    }

    // Finish secondaryScanner.close().
    unblockScannerClose.set(null);
    // And now connection.close() should unblock.
    closeFinished.get(5, TimeUnit.SECONDS);
    t.join();
  }
}
