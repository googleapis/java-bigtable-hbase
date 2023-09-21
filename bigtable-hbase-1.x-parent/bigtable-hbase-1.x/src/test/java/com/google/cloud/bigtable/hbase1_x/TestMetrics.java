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

import static com.google.common.truth.Truth.assertThat;

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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
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
    Table table = connection.getTable(TABLE_NAME);

    Stopwatch stopwatch = Stopwatch.createStarted();
    Result result = table.get(new Get(new byte[2]));
    long methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);

    fakeDataService.popLastRequest();

    long tableGetLatencyMetric =
        fakeMetricRegistry.results.get("google-cloud-bigtable.table.get.latency").get();
    long retriesPerformedMetric =
        fakeMetricRegistry
            .results
            .get("google-cloud-bigtable.grpc.method.ReadRow.retries.performed")
            .get();
    long clientOperationLatencyMetric =
        fakeMetricRegistry
            .results
            .get("google-cloud-bigtable.grpc.method.ReadRow.operation.latency")
            .get();
    long rpcErrorsMetric =
        fakeMetricRegistry
            .results
            .get("google-cloud-bigtable.grpc.errors." + fakeErrorStatus.getCode().name())
            .get();
    long rpcPerformedMetric =
        fakeMetricRegistry.results.get("google-cloud-bigtable.grpc.rpc.performed").get();

    assertThat(retriesPerformedMetric).isEqualTo(fakeErrorCount);
    assertThat(tableGetLatencyMetric).isAtMost(methodInvocationLatency);
    assertThat(clientOperationLatencyMetric).isAtMost(methodInvocationLatency);
    assertThat(rpcErrorsMetric).isEqualTo(fakeErrorCount);
    assertThat(rpcPerformedMetric).isEqualTo(fakeErrorCount + 1);

    // ReadRows sleeps 40 milliseconds before returning the response. Wait for response to return
    // and verify again there should have no more active rpc
    Thread.sleep(40);
    long activeRpcMetric =
        fakeMetricRegistry.results.get("google-cloud-bigtable.grpc.rpc.active").get();
    assertThat(activeRpcMetric).isEqualTo(0);
  }

  @Test
  public void rowMutations() throws IOException {
    Table table = connection.getTable(TABLE_NAME);

    RowMutations row = new RowMutations(rowKey);
    row.add(new Put(rowKey).addColumn(columnFamily, qualifier, value));

    Stopwatch stopwatch = Stopwatch.createStarted();
    table.mutateRow(row);
    long methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);

    fakeDataService.popLastRequest();
    long latency =
        fakeMetricRegistry
            .results
            .get("google-cloud-bigtable.grpc.method.MutateRow.rpc.latency")
            .get();
    assertThat(latency)
        .isIn(
            Range.closed(fakeDataService.getMutateRowServerSideLatency(), methodInvocationLatency));
  }

  @Test
  public void appendFailure() throws IOException {
    Table table = connection.getTable(TABLE_NAME);
    Append append = new Append(rowKey);
    append.add(columnFamily, qualifier, value);
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    try {
      stopwatch.start();
      table.append(append);
      Assert.fail("operation should have failed");
    } catch (Exception e) {
      long methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      fakeDataService.popLastRequest();
      long failureCount =
          fakeMetricRegistry
              .results
              .get("google-cloud-bigtable.grpc.method.ReadModifyWriteRow.failure")
              .get();
      long operationLatency =
          fakeMetricRegistry
              .results
              .get("google-cloud-bigtable.grpc.method.ReadModifyWriteRow.operation.latency")
              .get();

      assertThat(failureCount).isEqualTo(1);
      assertThat(operationLatency)
          .isIn(
              Range.closed(
                  fakeDataService.getReadModifyWriteRowServerSideLatency(),
                  methodInvocationLatency));
    }
  }

  @Test
  public void testScanMetrics() throws IOException {
    Scan scan = new Scan().withStartRow(rowKey).withStopRow(rowKey, true);
    Table table = connection.getTable(TABLE_NAME);

    Stopwatch stopwatch = Stopwatch.createStarted();
    ResultScanner testScanner = table.getScanner(scan);
    testScanner.next();
    testScanner.close();
    long methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);

    fakeDataService.popLastRequest();

    long scannerResultsLatencyMetric =
        fakeMetricRegistry.results.get("google-cloud-bigtable.scanner.results.latency").get();
    long scannerResultsMetric =
        fakeMetricRegistry.results.get("google-cloud-bigtable.scanner.results").get();

    assertThat(scannerResultsMetric).isEqualTo(1);
    assertThat(scannerResultsLatencyMetric).isAtMost(methodInvocationLatency);
  }

  @Test
  public void testFirstResponseLatency() throws IOException {
    Scan scan = new Scan().withStartRow(rowKey).withStopRow(rowKey, true);
    Table table = connection.getTable(TABLE_NAME);

    Stopwatch stopwatch = Stopwatch.createStarted();
    ResultScanner testScanner = table.getScanner(scan);
    testScanner.next();
    testScanner.close();
    long methodInvocationLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);

    fakeDataService.popLastRequest();

    long firstResponseLatencyMetric =
        fakeMetricRegistry
            .results
            .get("google-cloud-bigtable.grpc.method.ReadRows.firstResponse.latency")
            .get();

    assertThat(firstResponseLatencyMetric).isAtMost(methodInvocationLatency);
  }

  @Test
  public void testActiveSessionsAndChannels() throws IOException {
    // There should already be 1 active session and 1 channel from connection (connecting to
    // emulator will set pool size to 1)
    long currentActiveSessions =
        fakeMetricRegistry.results.get("google-cloud-bigtable.session.active").get();
    long currentActiveChannels =
        fakeMetricRegistry.results.get("google-cloud-bigtable.grpc.channel.active").get();

    assertThat(currentActiveSessions).isEqualTo(1);
    assertThat(currentActiveChannels).isEqualTo(1);

    // Create a new session
    int connectionCount = 10;
    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, String.valueOf(connectionCount));

    BigtableConnection newConnection = new BigtableConnection(configuration);
    currentActiveSessions =
        fakeMetricRegistry.results.get("google-cloud-bigtable.session.active").get();
    currentActiveChannels =
        fakeMetricRegistry.results.get("google-cloud-bigtable.grpc.channel.active").get();
    assertThat(currentActiveSessions).isEqualTo(2);
    assertThat(currentActiveChannels).isEqualTo(connectionCount + 1);

    newConnection.close();
    currentActiveSessions =
        fakeMetricRegistry.results.get("google-cloud-bigtable.session.active").get();
    currentActiveChannels =
        fakeMetricRegistry.results.get("google-cloud-bigtable.grpc.channel.active").get();
    assertThat(currentActiveSessions).isEqualTo(1);
    assertThat(currentActiveChannels).isEqualTo(1);
  }

  @Test
  public void testChannelPoolCachingActiveChannel() throws Exception {
    // Test channel pool caching
    int connectionCount = 10;
    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, String.valueOf(connectionCount));
    configuration.set(BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, "true");
    Credentials credentials = NoCredentialsProvider.create().getCredentials();
    configuration = BigtableConfiguration.withCredentials(configuration, credentials);

    BigtableConnection sharedConnection1 = new BigtableConnection(configuration);
    BigtableConnection sharedConnection2 = new BigtableConnection(configuration);

    long currentActiveChannels =
        fakeMetricRegistry.results.get("google-cloud-bigtable.grpc.channel.active").get();
    // sharedConnection1 and 2 should share channels, plus the 1 that's created in setup
    assertThat(currentActiveChannels).isEqualTo(connectionCount + 1);

    // closing one shared bigtable connection shouldn't decrement shared channels count
    sharedConnection1.close();
    currentActiveChannels =
        fakeMetricRegistry.results.get("google-cloud-bigtable.grpc.channel.active").get();
    assertThat(currentActiveChannels).isEqualTo(connectionCount + 1);

    // Active channels should be 1 after both shared bigtable connections are closed
    sharedConnection2.close();
    currentActiveChannels =
        fakeMetricRegistry.results.get("google-cloud-bigtable.grpc.channel.active").get();
    assertThat(currentActiveChannels).isEqualTo(1);

    Thread.sleep(100);
  }

  @Test
  public void testBulkMutationMetrics() throws IOException, InterruptedException {
    Table table = connection.getTable(TABLE_NAME);
    List<Row> rows = new ArrayList<>();
    int entries = 20;
    for (int i = 0; i < entries; i++) {
      rows.add(
          new Put(Bytes.toBytes(RandomStringUtils.random(8)))
              .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("SomeValue")));
    }
    Object[] resultsOrErrors = new Object[rows.size()];
    table.batch(rows, resultsOrErrors);

    long mutationAdded =
        fakeMetricRegistry.results.get("google-cloud-bigtable.bulk-mutator.mutations.added").get();
    assertThat(mutationAdded).isEqualTo(entries);
    table.close();
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
      MutateRowsResponse.Builder response = MutateRowsResponse.newBuilder();
      for (int i = 0; i < request.getEntriesCount(); i++) {
        response.addEntries(MutateRowsResponse.Entry.newBuilder().setIndex(i).build());
      }
      responseObserver.onNext(response.build());
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
}
