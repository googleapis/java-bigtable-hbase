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
package com.google.cloud.bigtable.hbase;

import static com.google.common.truth.Truth.assertThat;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.cloud.bigtable.test.helper.TestServerBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.truth.Correspondence;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class TestRetryBehavior {

  private static final Duration DELAY = Duration.ofMillis(5);
  private static final Duration ATTEMPT_TIMEOUT = Duration.ofMillis(450);
  private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(2);
  private final TestOp testOp;

  private Server fakeServer;

  private final List<Duration> remainingDurations = Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger callCount = new AtomicInteger();

  private final AtomicReference<Consumer<ServerCall<?, ?>>> rpcRunnable =
      new AtomicReference<>(
          (ServerCall<?, ?> call) -> call.close(Status.UNIMPLEMENTED, new Metadata()));

  private static final Correspondence<Duration, Duration> MIN_DURATION_CORRESPONDENCE =
      Correspondence.from((actual, expected) -> actual.compareTo(expected) >= 0, ">=");
  private static final Correspondence<Duration, Duration> MAX_DURATION_CORRESPONDENCE =
      Correspondence.from((actual, expected) -> actual.compareTo(expected) <= 0, "<=");

  interface TableRunnable {
    void run(Table table) throws IOException;
  }

  static class TestOp {
    private final String name;
    private final TableRunnable runnable;

    public TestOp(String name, TableRunnable runnable) {
      this.name = name;
      this.runnable = runnable;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Object[] getOps() {
    return new Object[] {
      new TestOp("single-get", (table) -> table.get(newGet("a"))),
      new TestOp("single-put", (table) -> table.put(newPut("some-key"))),
      new TestOp("scan", (table) -> Lists.newArrayList(table.getScanner(new Scan()))),
      new TestOp("multi-get", (table) -> table.get(Arrays.asList(newGet("a"), newGet("b")))),
      new TestOp("multi-put", (table) -> table.put(Arrays.asList(newPut("a"), newPut("b"))))
    };
  }

  public TestRetryBehavior(TestOp testOp) {
    this.testOp = testOp;
  }

  @Before
  public void setup() throws IOException {
    fakeServer =
        TestServerBuilder.newInstance()
            .intercept(
                new ServerInterceptor() {
                  @Override
                  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                      ServerCall<ReqT, RespT> serverCall,
                      Metadata metadata,
                      ServerCallHandler<ReqT, RespT> serverCallHandler) {
                    callCount.incrementAndGet();

                    // record the deadline
                    @Nullable
                    Duration duration =
                        Optional.ofNullable(Context.current().getDeadline())
                            .map(d -> Duration.ofMillis(d.timeRemaining(TimeUnit.MILLISECONDS)))
                            .orElse(null);
                    remainingDurations.add(duration);

                    // execute test specific callback
                    rpcRunnable.get().accept(serverCall);

                    // Don't really care about client requests, just blackhole them
                    // this will prevent any invocations of the stub
                    @SuppressWarnings("unchecked")
                    ServerCall.Listener<ReqT> blackhole = Mockito.mock(ServerCall.Listener.class);
                    return blackhole;
                  }
                })
            .addService(new BigtableGrpc.BigtableImplBase() {})
            .buildAndStart();
  }

  @After
  public void teardown() {
    fakeServer.shutdownNow();
  }

  private Configuration createBaseConfig() {
    Configuration config = BigtableConfiguration.configure("fake-project", "fake-instance");
    config.set(BigtableOptionsFactory.PROJECT_ID_KEY, "fake-project");
    config.set(BigtableOptionsFactory.INSTANCE_ID_KEY, "fake-project");
    config.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    config.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + fakeServer.getPort());
    config.set(BigtableOptionsFactory.ADDITIONAL_RETRY_CODES, "ABORTED");

    config.setLong(BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY, DELAY.toMillis());

    setOperationTimeout(config, OPERATION_TIMEOUT);
    setAttemptTimeout(config, ATTEMPT_TIMEOUT);

    return config;
  }

  private static void setOperationTimeout(Configuration config, Duration timeout) {
    config.setLong(BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY, timeout.toMillis());
    config.setLong(BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY, timeout.toMillis());
    config.setLong(BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY, timeout.toMillis());
  }

  private static void setAttemptTimeout(Configuration config, Duration timeout) {
    config.setLong(BigtableOptionsFactory.BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY, timeout.toMillis());

    config.setLong(
        BigtableOptionsFactory.BIGTABLE_READ_RPC_ATTEMPT_TIMEOUT_MS_KEY, timeout.toMillis());

    config.setLong(
        BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_ATTEMPT_TIMEOUT_MS_KEY, timeout.toMillis());
  }

  @Test
  public void testRpcWillRetryOnAbort() throws Exception {
    rpcRunnable.set((call) -> call.close(Status.ABORTED, new Metadata()));

    try (Connection connection = ConnectionFactory.createConnection(createBaseConfig());
        Table table = connection.getTable(TableName.valueOf("fake-table"))) {

      Stopwatch stopwatch = Stopwatch.createStarted();
      // TODO: this should be an IOException, but scanner.next isnt wrapping exceptions properly
      Exception e = Assert.assertThrows(Exception.class, () -> testOp.runnable.run(table));

      Duration elapsed = stopwatch.elapsed();

      Status status = extractStatus(e);
      assertThat(status.getCode()).isEqualTo(Status.Code.ABORTED);

      // Server immediately errors out, so exponential backoff will be the bottleneck for retries
      // Maximum delays will be: 5, 10, 20, 40, 80, 160, 320, 640, 1m, 1m, ...
      // With 2s operation deadline, minimum number of attempts will be 9
      // Since exponential delay uses full jitter (which will decrease delay by 50% on average),
      // there will most likely be more attempts. However to avoid flakiness, we can only make
      // assertions about attempts that are guaranteed to happen. For example with 400ms remaining
      // in the operation timeout, the 10th delay can be 100ms, causing the attempt timeout to be
      // 300ms and the 11th delay could be 50ms causing the next attempt to have an attempt timeout
      // of 250ms.
      assertThat(callCount.get()).isAtLeast(9);
      // Ignoring the first attempt, because it will be unstable due to connection setup
      assertThat(remainingDurations.subList(1, 9))
          .comparingElementsUsing(MIN_DURATION_CORRESPONDENCE)
          .contains(ATTEMPT_TIMEOUT.minus(Duration.ofMillis(5)));

      // but should still be limited by the operation timeout with some jitter
      assertThat(elapsed).isAtMost(OPERATION_TIMEOUT.plus(ATTEMPT_TIMEOUT.dividedBy(2)));
    }
  }

  @Test
  public void testRpcWillRespectAttemptTimeout() throws IOException {
    rpcRunnable.set(
        (call) -> {
          /* hang indefinitely */
        });

    try (Connection connection = ConnectionFactory.createConnection(createBaseConfig());
        Table table = connection.getTable(TableName.valueOf("fake-table"))) {

      Stopwatch stopwatch = Stopwatch.createStarted();
      // TODO: this should be an IOException, but scan doesnt properly wrap it
      Exception e = Assert.assertThrows(Exception.class, () -> testOp.runnable.run(table));

      Duration elapsed = stopwatch.elapsed();

      Status status = extractStatus(e);
      assertThat(status.getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);

      // Server will hang for the attempt timeout and the client should retry the attempt
      // 5 * 450ms attempt timeouts > 2s operation timeout
      // 450 + 5 + 450 + 10 + 450 + 20 + 450 + 40 + 125
      assertThat(callCount.get()).isAtMost(5);
      // 3 * 450ms attempt timeouts + jitter < 2s operation timeout
      assertThat(callCount.get()).isAtLeast(3);
      assertThat(remainingDurations)
          .comparingElementsUsing(MAX_DURATION_CORRESPONDENCE)
          .contains(ATTEMPT_TIMEOUT);

      // Ignoring the first and last attempt, all attempts should be just under attempt timeout
      assertThat(remainingDurations.subList(1, remainingDurations.size() - 1))
          .comparingElementsUsing(MIN_DURATION_CORRESPONDENCE)
          .contains(ATTEMPT_TIMEOUT.minus(Duration.ofMillis(10)));

      // Altogether should still be limited by the operation timeout with some jitter
      assertThat(elapsed).isAtMost(OPERATION_TIMEOUT.plus(ATTEMPT_TIMEOUT));
    }
  }

  @Test
  public void testRpcWillNotRetryOnHangWithoutAttemptTimeout() throws IOException {
    rpcRunnable.set(
        (call) -> {
          /* hang indefinitely */
        });

    Configuration configuration = createBaseConfig();
    setAttemptTimeout(configuration, OPERATION_TIMEOUT);

    try (Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("fake-table"))) {

      Stopwatch stopwatch = Stopwatch.createStarted();
      // TODO: this should be an IOException, but scans dont wrap it properly
      Exception e = Assert.assertThrows(Exception.class, () -> testOp.runnable.run(table));

      Duration elapsed = stopwatch.elapsed();

      Status status = extractStatus(e);
      assertThat(status.getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);

      // Server will hang for the attempt timeout and the client should retry the attempt
      assertThat(callCount.get()).isEqualTo(1);
      assertThat(elapsed).isAtMost(OPERATION_TIMEOUT.plus(ATTEMPT_TIMEOUT.dividedBy(2)));
      assertThat(remainingDurations.get(0)).isGreaterThan(ATTEMPT_TIMEOUT);
      assertThat(remainingDurations.get(0)).isAtMost(OPERATION_TIMEOUT);
    }
  }

  private static Status extractStatus(Throwable t) {
    // RetriesExhaustedWithDetailsException hides its cause in a list
    if (t instanceof RetriesExhaustedWithDetailsException) {
      RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException) t;
      if (!rewde.getCauses().isEmpty()) {
        t = Objects.requireNonNull(Iterables.getLast(rewde.getCauses()));
      }
    }
    return Status.fromThrowable(t);
  }

  private static Get newGet(String key) {
    return new Get(key.getBytes());
  }

  private static Put newPut(String key) {
    return new Put(key.getBytes()).addColumn("cf".getBytes(), "q".getBytes(), "v".getBytes());
  }
}
