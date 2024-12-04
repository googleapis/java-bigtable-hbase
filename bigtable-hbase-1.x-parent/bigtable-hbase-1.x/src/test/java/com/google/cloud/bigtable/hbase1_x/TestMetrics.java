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
package com.google.cloud.bigtable.hbase1_x;

import static com.google.common.truth.Truth.assertAbout;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.auth.Credentials;
import com.google.bigtable.v2.*;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Counter;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.MetricRegistry;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.test.helper.TestServerBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
import com.google.common.truth.FailureMetadata;
import com.google.common.truth.LongSubject;
import com.google.common.truth.Subject;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMetrics {

  private final String TEST_PROJECT_ID = "fake-project-id";
  private final String TEST_INSTANCE_ID = "fake-instance-id";
  private final TableName TABLE_NAME = TableName.valueOf("fake-table");
  private Server server;
  private FakeMetricRegistry fakeMetricRegistry;

  private MetricRegistry originalMetricRegistry;
  private BigtableClientMetrics.MetricLevel originalLevelToLog;

  private static final FakeDataService fakeDataService = new FakeDataService();
  private BigtableConnection connection;

  private static final byte[] rowKey = Bytes.toBytes("row");
  private static final byte[] columnFamily = Bytes.toBytes("cf");
  private static final byte[] qualifier = Bytes.toBytes("q");
  private static final byte[] value = Bytes.toBytes("value");
  private static final ReadRowsResponse readRowsResponse =
      ReadRowsResponse.newBuilder()
          .addChunks(
              CellChunk.newBuilder()
                  .setRowKey(ByteString.copyFrom(rowKey))
                  .setFamilyName(StringValue.of("cf"))
                  .setQualifier(BytesValue.newBuilder().setValue(ByteString.copyFrom(qualifier)))
                  .setTimestampMicros(1_000)
                  .setValue(ByteString.copyFrom(value))
                  .setCommitRow(true))
          .build();
  private static final Status fakeErrorStatus = Status.UNAVAILABLE;
  private static final int fakeErrorCount = 3;

  @Before
  public void setUp() throws IOException {
    server = TestServerBuilder.newInstance().addService(fakeDataService).buildAndStart();

    originalLevelToLog = BigtableClientMetrics.getLevelToLog();
    originalMetricRegistry = BigtableClientMetrics.getMetricRegistry(originalLevelToLog);

    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + server.getPort());

    fakeMetricRegistry = new FakeMetricRegistry();
    BigtableClientMetrics.setMetricRegistry(fakeMetricRegistry);
    BigtableClientMetrics.setLevelToLog(MetricLevel.Trace);

    connection = new BigtableConnection(configuration);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
    }
    connection.close();
    BigtableClientMetrics.setMetricRegistry(originalMetricRegistry);
    BigtableClientMetrics.setLevelToLog(originalLevelToLog);
    fakeDataService.reset();
  }

  /*
   * This tests metric instrumentation by using a fake service to inject failures.
   * The fake service will fail the first 3 readrows requests, causing the client to start exponential retries.
   */
  @Test
  public void readRows() throws IOException, InterruptedException {
    final long methodInvocationLatency;
    try (Table table = connection.getTable(TABLE_NAME)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      table.get(new Get(new byte[2]));
      methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    fakeDataService.popLastRequest();

    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.table.get.latency", s -> s.isAtMost(methodInvocationLatency));

    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.grpc.method.ReadRow.retries.performed",
            s -> s.isEqualTo(fakeErrorCount));

    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.grpc.method.ReadRow.operation.latency",
            s -> s.isAtMost(methodInvocationLatency));

    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.grpc.errors." + fakeErrorStatus.getCode().name(),
            s -> s.isEqualTo(fakeErrorCount));
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.grpc.rpc.performed", s -> s.isEqualTo(fakeErrorCount + 1));

    // ReadRows sleeps 40 milliseconds before returning the response. Wait for response to return
    // and verify again there should have no more active rpc
    Thread.sleep(40);
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .hasMetric("google-cloud-bigtable.grpc.rpc.active", 0);
  }

  @Test
  public void rowMutations() throws IOException, InterruptedException {
    Table table = connection.getTable(TABLE_NAME);

    RowMutations row = new RowMutations(rowKey);
    row.add(new Put(rowKey).addColumn(columnFamily, qualifier, value));

    Stopwatch stopwatch = Stopwatch.createStarted();
    table.mutateRow(row);
    long methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);

    fakeDataService.popLastRequest();
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.grpc.method.MutateRow.rpc.latency",
            s ->
                s.isIn(
                    Range.closed(
                        fakeDataService.getMutateRowServerSideLatency(), methodInvocationLatency)));
  }

  @Test
  public void appendFailure() throws IOException, InterruptedException {
    final long methodInvocationLatency;

    try (Table table = connection.getTable(TABLE_NAME)) {
      Append append = new Append(rowKey);
      append.add(columnFamily, qualifier, value);

      Stopwatch stopwatch = Stopwatch.createStarted();
      assertThrows(Exception.class, () -> table.append(append));
      methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    fakeDataService.popLastRequest();

    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.grpc.method.ReadModifyWriteRow.failure", s -> s.isEqualTo(1));

    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.grpc.method.ReadModifyWriteRow.operation.latency",
            s ->
                s.isIn(
                    Range.closed(
                        fakeDataService.getReadModifyWriteRowServerSideLatency(),
                        methodInvocationLatency)));
  }

  @Test
  public void testScanMetrics() throws IOException, InterruptedException {
    Scan scan = new Scan().withStartRow(rowKey).withStopRow(rowKey, true);

    final long methodInvocationLatency;
    try (Table table = connection.getTable(TABLE_NAME)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      try (ResultScanner s = table.getScanner(scan)) {
        s.next();
      }
      methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    fakeDataService.popLastRequest();

    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat("google-cloud-bigtable.scanner.results", s -> s.isEqualTo(1));
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.scanner.results.latency",
            s -> s.isAtMost(methodInvocationLatency));
  }

  @Test
  public void testFirstResponseLatency() throws IOException, InterruptedException {
    Scan scan = new Scan().withStartRow(rowKey).withStopRow(rowKey, true);
    final long methodInvocationLatency;

    try (Table table = connection.getTable(TABLE_NAME)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      try (ResultScanner s = table.getScanner(scan)) {
        s.next();
        methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      }
    }

    fakeDataService.popLastRequest();

    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.grpc.method.ReadRows.firstResponse.latency",
            s -> s.isAtMost(methodInvocationLatency));
  }

  @Test
  public void testActiveSessionsAndChannels() throws IOException {
    // There should already be 1 active session and 1 channel from connection (connecting to
    // emulator will set pool size to 1)
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .hasMetric("google-cloud-bigtable.session.active", 1);
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .hasMetric("google-cloud-bigtable.grpc.channel.active", 1);

    // Create a new session
    int connectionCount = 10;
    Configuration configuration = new Configuration(false);
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + server.getPort());
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, String.valueOf(connectionCount));

    try (BigtableConnection ignored = new BigtableConnection(configuration)) {
      MetricsRegistrySubject.assertThat(fakeMetricRegistry)
          .hasMetric("google-cloud-bigtable.session.active", 2);
      MetricsRegistrySubject.assertThat(fakeMetricRegistry)
          .hasMetric("google-cloud-bigtable.grpc.channel.active", connectionCount + 1);
    }
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .hasMetric("google-cloud-bigtable.session.active", 1);
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .hasMetric("google-cloud-bigtable.grpc.channel.active", 1);
  }

  @Test
  public void testChannelPoolCachingActiveChannel() throws Exception {
    // Test channel pool caching
    int connectionCount = 10;
    Configuration configuration = new Configuration(false);
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + server.getPort());
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, String.valueOf(connectionCount));
    configuration.set(BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, "true");
    Credentials credentials = NoCredentialsProvider.create().getCredentials();
    configuration = BigtableConfiguration.withCredentials(configuration, credentials);

    try (BigtableConnection sharedConnection1 = new BigtableConnection(configuration)) {
      try (BigtableConnection sharedConnection2 = new BigtableConnection(configuration)) {
        // sharedConnection 1 and 2 should share channels, plus the 1 that's created in setup
        MetricsRegistrySubject.assertThat(fakeMetricRegistry)
            .hasMetric("google-cloud-bigtable.grpc.channel.active", connectionCount + 1);
      }
      // closing one shared bigtable connection shouldn't decrement shared channels count
      MetricsRegistrySubject.assertThat(fakeMetricRegistry)
          .hasMetric("google-cloud-bigtable.grpc.channel.active", connectionCount + 1);
    }
    // Active channels should be 1 after both shared bigtable connections are closed
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .hasMetric("google-cloud-bigtable.grpc.channel.active", 1);
  }

  @Test
  public void testBulkMutationMetrics() throws IOException, InterruptedException {
    final int entries = 20;

    try (Table table = connection.getTable(TABLE_NAME)) {
      List<Row> rows = new ArrayList<>();
      for (int i = 0; i < entries; i++) {
        rows.add(
            new Put(Bytes.toBytes(RandomStringUtils.random(8)))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("SomeValue")));
      }
      Object[] resultsOrErrors = new Object[rows.size()];
      table.batch(rows, resultsOrErrors);
    }
    MetricsRegistrySubject.assertThat(fakeMetricRegistry)
        .eventuallyHasMetricThat(
            "google-cloud-bigtable.bulk-mutator.mutations.added", s -> s.isEqualTo(entries));
  }

  private static class FakeDataService extends BigtableGrpc.BigtableImplBase {
    private final AtomicLong callCount = new AtomicLong(1);

    private final Stopwatch readRowsStopwatch = Stopwatch.createUnstarted();
    private final Stopwatch readModifyWriteRowStopwatch = Stopwatch.createUnstarted();
    private final Stopwatch mutateRowStopwatch = Stopwatch.createUnstarted();

    private long readRowServerSideLatency;
    private long readModifyWriteRowServerSideLatency;
    private long mutateRowServerSideLatency;

    private final Object lock = new Object();

    final ConcurrentLinkedQueue requests = new ConcurrentLinkedQueue();

    @SuppressWarnings("unchecked")
    <T> T popLastRequest() {
      return (T) requests.poll();
    }

    public long getReadRowServerSideLatency() {
      synchronized (lock) {
        return readRowServerSideLatency;
      }
    }

    public long getMutateRowServerSideLatency() {
      synchronized (lock) {
        return mutateRowServerSideLatency;
      }
    }

    public long getReadModifyWriteRowServerSideLatency() {
      synchronized (lock) {
        return readModifyWriteRowServerSideLatency;
      }
    }

    public void reset() {
      readRowsStopwatch.reset();
      readModifyWriteRowStopwatch.reset();
      mutateRowStopwatch.reset();
      callCount.set(1);
    }

    @Override
    public void pingAndWarm(
        PingAndWarmRequest request, StreamObserver<PingAndWarmResponse> responseObserver) {
      responseObserver.onNext(PingAndWarmResponse.getDefaultInstance());
    }

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      requests.add(request);

      synchronized (lock) {
        if (!readRowsStopwatch.isRunning()) {
          readRowsStopwatch.start();
        }
        if (callCount.getAndIncrement() < fakeErrorCount + 1) {
          responseObserver.onError(new StatusRuntimeException(fakeErrorStatus));
        } else {
          try {
            Thread.sleep(20);
          } catch (InterruptedException e) {
            responseObserver.onError(e);
            return;
          }
          responseObserver.onNext(readRowsResponse);
          // sleep after the response to test first response latency
          try {
            Thread.sleep(20);
          } catch (InterruptedException e) {
            responseObserver.onError(e);
            return;
          }
          readRowServerSideLatency = readRowsStopwatch.elapsed(TimeUnit.MILLISECONDS);
          responseObserver.onCompleted();
        }
      }
    }

    @Override
    public void readModifyWriteRow(
        ReadModifyWriteRowRequest request,
        StreamObserver<ReadModifyWriteRowResponse> responseObserver) {
      if (!readModifyWriteRowStopwatch.isRunning()) {
        readModifyWriteRowStopwatch.start();
      }
      requests.add(request);
      readModifyWriteRowServerSideLatency =
          readModifyWriteRowStopwatch.elapsed(TimeUnit.MILLISECONDS);
      responseObserver.onError(new StatusRuntimeException(Status.FAILED_PRECONDITION));
    }

    @Override
    public void mutateRow(
        MutateRowRequest request, StreamObserver<MutateRowResponse> responseObserver) {
      if (!mutateRowStopwatch.isRunning()) {
        mutateRowStopwatch.start();
      }
      requests.add(request);
      responseObserver.onNext(MutateRowResponse.newBuilder().build());
      mutateRowServerSideLatency = mutateRowStopwatch.elapsed(TimeUnit.MILLISECONDS);
      responseObserver.onCompleted();
    }

    @Override
    public void mutateRows(
        MutateRowsRequest request, StreamObserver<MutateRowsResponse> responseObserver) {
      MutateRowsResponse.Builder builder = MutateRowsResponse.newBuilder();
      for (int i = 0; i < request.getEntriesCount(); i++) {
        builder.addEntriesBuilder().setIndex(i);
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }
  }

  private static class FakeMetricRegistry implements MetricRegistry {
    private final Object lock = new Object();

    private final Map<String, AtomicLong> results = new HashMap<>();

    @Override
    public Counter counter(final String name) {
      // counter operations either increment or decrement a key's value
      return new Counter() {
        @Override
        public void inc() {
          synchronized (lock) {
            AtomicLong atomicLong = results.get(name);
            if (atomicLong == null) {
              AtomicLong value = new AtomicLong();
              results.put(name, value);
            }
          }
          results.get(name).getAndIncrement();
        }

        @Override
        public void dec() {
          synchronized (lock) {
            AtomicLong atomicLong = results.get(name);
            if (atomicLong == null) {
              AtomicLong value = new AtomicLong();
              results.put(name, value);
            }
          }
          results.get(name).getAndDecrement();
        }
      };
    }

    @Override
    public Timer timer(final String name) {
      // timer operations overwrite a key's value
      return new Timer() {
        final Stopwatch stopwatch = Stopwatch.createStarted();

        @Override
        public Context time() {
          return new Context() {
            @Override
            public void close() {
              synchronized (lock) {
                results.put(name, new AtomicLong(stopwatch.elapsed(TimeUnit.MILLISECONDS)));
              }
            }
          };
        }

        @Override
        public void update(long duration, TimeUnit unit) {
          // update operations overwrite a key's value
          synchronized (lock) {
            results.put(name, new AtomicLong(duration));
          }
        }
      };
    }

    @Override
    public Meter meter(final String name) {
      // meter operations increment the current key's value
      return new Meter() {
        @Override
        public void mark() {
          synchronized (lock) {
            AtomicLong atomicLong = results.get(name);
            if (atomicLong == null) {
              AtomicLong value = new AtomicLong();
              results.put(name, value);
            }
          }
          results.get(name).getAndIncrement();
        }

        // unless a size is specified, in which case it is overridden
        @Override
        public void mark(long size) {
          synchronized (lock) {
            results.put(name, new AtomicLong(size));
          }
        }
      };
    }
  }

  private static class MetricsRegistrySubject extends Subject {
    private final FakeMetricRegistry actual;

    public MetricsRegistrySubject(FailureMetadata metadata, @Nullable FakeMetricRegistry actual) {
      super(metadata, actual);
      this.actual = actual;
    }

    private static Factory<MetricsRegistrySubject, FakeMetricRegistry> registry() {
      return MetricsRegistrySubject::new;
    }

    static MetricsRegistrySubject assertThat(@Nullable FakeMetricRegistry actual) {
      return assertAbout(registry()).that(actual);
    }

    void eventuallyHasMetricThat(String name, Consumer<LongSubject> cb)
        throws InterruptedException {
      for (int i = 10; i >= 0; i--) {
        try {
          LongSubject valueSubject =
              check("results.get(%s)", name)
                  .that(
                      Optional.ofNullable(actual.results.get(name))
                          .map(AtomicLong::get)
                          .orElse(null));
          cb.accept(valueSubject);
        } catch (AssertionError e) {
          if (i == 0) {
            throw e;
          }
          Thread.sleep(100);
        }
      }
    }

    void hasMetric(String name, long value) {
      check("results.get(%s)", name)
          .that(Optional.ofNullable(actual.results.get(name)).map(AtomicLong::get).orElse(null))
          .isEqualTo(value);
    }
  }
}
