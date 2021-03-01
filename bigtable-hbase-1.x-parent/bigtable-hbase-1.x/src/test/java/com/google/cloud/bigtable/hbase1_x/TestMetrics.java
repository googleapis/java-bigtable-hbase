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

import static org.junit.Assert.assertEquals;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Counter;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.MetricRegistry;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.collect.Queues;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMetrics {

  private static final String TEST_PROJECT_ID = "fake-project-id";
  private static final String TEST_INSTANCE_ID = "fake-instance-id";
  private static final TableName TABLE_NAME = TableName.valueOf("fake-table");
  private static final String FULL_TABLE_NAME =
      NameUtil.formatTableName(TEST_PROJECT_ID, TEST_INSTANCE_ID, TABLE_NAME.getNameAsString());
  private static Server server;
  private static int dataPort;
  private static final AtomicInteger callCount = new AtomicInteger(1);

  private static final FakeDataService fakeDataService = new FakeDataService();
  private BigtableConnection connection;

  @Before
  public void setUp() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    configuration.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + dataPort);
    configuration.set(BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT, "true");
    connection = new BigtableConnection(configuration);
  }

  @BeforeClass
  public static void setUpServer() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      dataPort = s.getLocalPort();
    }
    server = ServerBuilder.forPort(dataPort).addService(fakeDataService).build();
    server.start();
  }

  @AfterClass
  public static void tearDownServer() throws InterruptedException {
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
    }
  }

  @After
  public void tearDown() throws IOException {
    connection.close();
  }

  @Test
  public void readRows() throws IOException, InterruptedException {
    FakeMetricRegistry fakeMetricRegistry = new FakeMetricRegistry();

    BigtableClientMetrics.setMetricRegistry(fakeMetricRegistry);
    Table table = connection.getTable(TABLE_NAME);

    long readRowsStart = System.currentTimeMillis();
    Result result = table.get(new Get(new byte[2]));
    long readRowsTime = System.currentTimeMillis() - readRowsStart;

    ReadRowsRequest request = fakeDataService.popLastRequest();
    assertEquals(FULL_TABLE_NAME, request.getTableName());

    AtomicLong readRowFailure =
        fakeMetricRegistry.results.get("google-cloud-bigtable.grpc.method.ReadRow.failure");
    AtomicLong operationLatency =
        fakeMetricRegistry.results.get(
            "google-cloud-bigtable.grpc.method.ReadRow.operation.latency");
    AtomicLong tableGetLatency =
        fakeMetricRegistry.results.get("google-cloud-bigtable.table.get.latency");

    Assert.assertEquals(3, readRowFailure.get());
    Assert.assertTrue(
        "operation latency for ReadRow took longer than expected",
        operationLatency.get() <= readRowsTime + 50);
    Assert.assertTrue(
        "operation latency for table.get took longer than expected",
        tableGetLatency.get() <= readRowsTime + 50);
  }

  private static class FakeDataService extends BigtableGrpc.BigtableImplBase {

    final BlockingQueue<Object> requests = Queues.newLinkedBlockingDeque();

    @SuppressWarnings("unchecked")
    <T> T popLastRequest() throws InterruptedException {
      return (T) requests.poll(1, TimeUnit.SECONDS);
    }

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      requests.add(request);
      if (callCount.getAndIncrement() < 4) {
        responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
      } else {
        responseObserver.onNext(ReadRowsResponse.newBuilder().build());
        responseObserver.onCompleted();
      }
    }
  }

  private static class FakeMetricRegistry implements MetricRegistry {

    private final Map<String, AtomicLong> results = new HashMap<>();

    @Override
    public Counter counter(final String name) {
      return new Counter() {
        @Override
        public void inc() {
          AtomicLong atomicLong = results.get(name);
          if (atomicLong == null) {
            AtomicLong value = new AtomicLong();
            results.put(name, value);
          }
          results.get(name).getAndIncrement();
        }

        @Override
        public void dec() {
          AtomicLong atomicLong = results.get(name);
          if (atomicLong == null) {
            AtomicLong value = new AtomicLong();
            results.put(name, value);
          }
          results.get(name).getAndDecrement();
        }
      };
    }

    @Override
    public Timer timer(final String name) {
      return new Timer() {
        final long start = System.currentTimeMillis();

        @Override
        public Context time() {
          return new Context() {
            @Override
            public void close() {
              results.put(name, new AtomicLong(System.currentTimeMillis() - start));
            }
          };
        }

        @Override
        public void update(long duration, TimeUnit unit) {
          results.put(name, new AtomicLong(duration));
        }
      };
    }

    @Override
    public Meter meter(final String name) {
      return new Meter() {
        @Override
        public void mark() {
          AtomicLong atomicLong = results.get(name);
          if (atomicLong == null) {
            AtomicLong value = new AtomicLong();
            results.put(name, value);
          }
          results.get(name).getAndIncrement();
        }

        @Override
        public void mark(long size) {
          results.put(name, new AtomicLong(size));
        }
      };
    }
  }
}
