/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.config.Logger;
import com.google.common.collect.ImmutableMap;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(KnownHBaseGap.class)
public abstract class TestRpcRetryBehavior {
  private static final Logger LOG = new Logger(TestRpcRetryBehavior.class);

  /**
   * Indicates whether the test will involve timeouts from the user side. In case we don't, the
   * client would only retry the logic on a server response.
   */
  protected boolean timeoutEnabled;
  /**
   * Indicates whether the test is for attempt timeouts enabled (the new opt-in feature as of
   * 1.18.x) or disabled (the default logic for the versions prior to that).
   */
  protected boolean attemptTimeoutEnabled;
  /**
   * Indicates whether our test server RPC will explicitly abort (true) or hang (false). This allows
   * us to test the timeout capability on the client side.
   */
  protected boolean serverRpcAbortsForTest;

  // Choose timeouts such that we can have multiple attempts while having some overlap in the last
  // attempt over the timeout. Here, we have attempts at the approximate times 0, 450, 900, 1350,
  // 1800 ms before reaching timeout at 2000 ms. Hence, we expect 5 attempts. The last attempt at
  // 1800 would end at 2250 if it weren't for a timeout, so our tests need to verify that our
  // operations don't reach to that point and instead end at around 2000 ms.
  protected final long attemptTimeoutMs = 450;
  protected final long operationTimeoutMs = 2000;
  protected final int expectedAttemptsWithRetryLogic = 5;
  protected final long maxElapsedBackoffMs = 1900;

  private Server server;

  @Before
  public void setup() throws Exception {
    this.server = startFake();
  }

  @After
  public void teardown() {
    this.server.shutdownNow();
  }

  // TODO(stepanian): move away from these parameterized-test flags and move the logic directly into
  // these test methods and out of testMain.
  @Test
  public void testOperationAndAttemptTimeoutsWillRetryOnServerAbort() throws Exception {
    timeoutEnabled = true;
    attemptTimeoutEnabled = true;
    serverRpcAbortsForTest = true;
    testMain();
  }

  @Test
  public void testOperationAndAttemptTimeoutsWillRetryOnServerHang() throws Exception {
    timeoutEnabled = true;
    attemptTimeoutEnabled = true;
    serverRpcAbortsForTest = false;
    testMain();
  }

  @Test
  public void testOperationTimeoutOnlyWillNotRetryAndEndEarlyOnServerAbort() throws Exception {
    timeoutEnabled = true;
    attemptTimeoutEnabled = false;
    serverRpcAbortsForTest = true;
    testMain();
  }

  @Test
  public void testOperationTimeoutOnlyWillNotRetryAndEndEarlyOnServerHang() throws Exception {
    timeoutEnabled = true;
    attemptTimeoutEnabled = false;
    serverRpcAbortsForTest = false;
    testMain();
  }

  @Test
  public void testNoTimeoutsWillStillRetryOnServerAbort() throws Exception {
    timeoutEnabled = false;
    attemptTimeoutEnabled = false;
    serverRpcAbortsForTest = true;
    testMain();
  }

  private void testMain() throws Exception {
    ImmutableMap.Builder<String, String> connPropsBuilder = defineProperties();

    try (Connection conn = makeConnection(connPropsBuilder.build())) {
      Table table = conn.getTable(TableName.valueOf("table"));

      StopWatch sw = new StopWatch();
      executeLogic(table, sw);

      validateOperationRuntime(sw);
    }

    validateInvocations(getInvocations());
  }

  protected abstract AtomicInteger getInvocations();

  protected abstract void executeLogic(Table table, StopWatch sw) throws Exception;

  protected abstract ImmutableMap.Builder<String, String> defineProperties();

  protected void validateOperationRuntime(StopWatch sw) {
    if (timeoutEnabled) {
      assertThat(
          sw.getTime(),
          Matchers.both(greaterThanOrEqualTo(operationTimeoutMs))
              .and(lessThan(operationTimeoutMs + 200))); // plus some buffer
    } else {
      // Without timeouts and just based on the backoff millis, the upper bound isn't as exact since
      // we may or may not
      // do an additional attempt. Update the upper bound accordingly.
      assertThat(
          sw.getTime(),
          Matchers.both(greaterThanOrEqualTo(operationTimeoutMs))
              .and(lessThan(operationTimeoutMs + attemptTimeoutMs + 200))); // plus some buffer
    }
  }

  protected void validateInvocations(AtomicInteger counter) {
    if (!timeoutEnabled && serverRpcAbortsForTest) {
      // Backoff and jitter logic is not as deterministic. Best we can do is verify a minimum number
      // of retries.
      assertThat(counter.get(), greaterThanOrEqualTo(expectedAttemptsWithRetryLogic - 1));
      return;
    }

    // Expect multiple retries (given our timeout parameters) if attempt timeouts are enabled;
    // otherwise,
    // only 1 retry happens.
    int expectedInvocations;
    if (attemptTimeoutEnabled || serverRpcAbortsForTest) {
      expectedInvocations = expectedAttemptsWithRetryLogic;
    } else {
      expectedInvocations = 1;
    }

    LOG.info("Expecting invocations for test: {}", expectedInvocations);

    assertThat(counter.get(), equalTo(expectedInvocations));
  }

  protected Connection makeConnection(Map<String, String> customConnProps) throws IOException {
    Configuration config = BigtableConfiguration.configure("project", "instance");

    config.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");

    // only for emulator
    config.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + server.getPort());

    for (Map.Entry<String, String> connProp : customConnProps.entrySet()) {
      config.set(connProp.getKey(), connProp.getValue());
    }
    // retry on aborted to differentiate server hang an explicit server error
    config.set(BigtableOptionsFactory.ADDITIONAL_RETRY_CODES, "ABORTED");

    return ConnectionFactory.createConnection(config);
  }

  protected abstract BigtableGrpc.BigtableImplBase setupRpcCall();

  private Server startFake() throws Exception {
    final BigtableGrpc.BigtableImplBase rpcBase = setupRpcCall();
    BigtableGrpc.BigtableImplBase rpcSleepWrapper = setupRpcServerWithSleepHandler(rpcBase);

    int portNum;
    try (ServerSocket ss = new ServerSocket(0)) {
      portNum = ss.getLocalPort();
    }
    Server fakeBigtableServer = ServerBuilder.forPort(portNum).addService(rpcSleepWrapper).build();
    fakeBigtableServer.start();
    return fakeBigtableServer;
  }

  /**
   * Wraps the given base RPC handler defined by the implementation tests with our handler for the
   * sleep and abort logic.
   */
  private BigtableGrpc.BigtableImplBase setupRpcServerWithSleepHandler(
      final BigtableGrpc.BigtableImplBase rpcBase) {
    return new BigtableGrpc.BigtableImplBase() {
      @Override
      public void mutateRow(
          MutateRowRequest request, StreamObserver<MutateRowResponse> responseObserver) {
        rpcBase.mutateRow(request, responseObserver);
        sleepAndAbortIfApplicable(responseObserver);
      }

      @Override
      public void checkAndMutateRow(
          CheckAndMutateRowRequest request,
          StreamObserver<CheckAndMutateRowResponse> responseObserver) {
        rpcBase.checkAndMutateRow(request, responseObserver);
        sleepAndAbortIfApplicable(responseObserver);
      }

      @Override
      public void readRows(
          ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
        rpcBase.readRows(request, responseObserver);
        sleepAndAbortIfApplicable(responseObserver);
      }

      @Override
      public void mutateRows(
          MutateRowsRequest request, StreamObserver<MutateRowsResponse> responseObserver) {
        rpcBase.mutateRows(request, responseObserver);
        sleepAndAbortIfApplicable(responseObserver);
      }

      @Override
      public void readModifyWriteRow(
          ReadModifyWriteRowRequest request,
          StreamObserver<ReadModifyWriteRowResponse> responseObserver) {
        rpcBase.readModifyWriteRow(request, responseObserver);
        sleepAndAbortIfApplicable(responseObserver);
      }

      private void sleepAndAbortIfApplicable(StreamObserver<?> responseObserver) {
        if (serverRpcAbortsForTest) {
          try {
            Thread.sleep(attemptTimeoutMs);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          responseObserver.onError(new StatusRuntimeException(Status.ABORTED));
        }
      }
    };
  }
}
