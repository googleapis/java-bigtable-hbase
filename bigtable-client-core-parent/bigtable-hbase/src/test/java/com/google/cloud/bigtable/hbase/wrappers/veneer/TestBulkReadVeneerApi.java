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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.client.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class TestBulkReadVeneerApi {

  private static final String TABLE_ID = "fake-table-id";
  private static final ByteString ROW_KEY = ByteString.copyFromUtf8("row-key");
  private FakeDataService fakeDataService;
  private BigtableDataSettings.Builder settingsBuilder;
  private BigtableDataClient dataClient;
  private Server server;

  @Before
  public void setUp() throws IOException {
    final int port;
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }

    fakeDataService = new FakeDataService();
    server = ServerBuilder.forPort(port).addService(fakeDataService).build();
    server.start();

    settingsBuilder =
        BigtableDataSettings.newBuilderForEmulator(port)
            .setProjectId("fake-project")
            .setInstanceId("fake-instance");
  }

  @After
  public void tearDown() throws InterruptedException {
    dataClient.close();
    if (server != null) {
      server.shutdown();
      server.awaitTermination();
    }
  }

  @Test
  public void testAdd() throws Exception {
    dataClient = BigtableDataClient.create(settingsBuilder.build());
    BulkReadWrapper bulkReadWrapper = new BulkReadVeneerApi(dataClient, TABLE_ID);

    ApiFuture<Result> resultFuture1_1 = bulkReadWrapper.add(ByteString.copyFromUtf8("one"), null);
    ApiFuture<Result> resultFuture1_2 = bulkReadWrapper.add(ByteString.copyFromUtf8("two"), null);
    assertFalse(resultFuture1_1.isDone());

    Filters.Filter filter = Filters.FILTERS.key().regex("cf");
    ApiFuture<Result> secBatchResult1 = bulkReadWrapper.add(ByteString.copyFromUtf8("1"), filter);
    ApiFuture<Result> secBatchResult2 = bulkReadWrapper.add(ByteString.copyFromUtf8("2"), filter);

    bulkReadWrapper.sendOutstanding();

    resultFuture1_1.get();
    resultFuture1_2.get();
    secBatchResult1.get();
    secBatchResult2.get();

    // If one entry of the batch is resolved then another should also be
    assertTrue(resultFuture1_1.isDone());
    assertTrue(resultFuture1_2.isDone());
    assertTrue(secBatchResult1.isDone());
    assertTrue(secBatchResult2.isDone());

    assertEquals(2, fakeDataService.getReadRowsBatchCount());
  }

  @Test
  public void testAddWithoutSendOutstanding() throws Exception {
    settingsBuilder
        .stubSettings()
        .bulkReadRowsSettings()
        .setBatchingSettings(
            BatchingSettings.newBuilder()
                .setDelayThreshold(Duration.ofMillis(100))
                .setElementCountThreshold(10L)
                .setRequestByteThreshold(10L * 1024L)
                .build());
    dataClient = BigtableDataClient.create(settingsBuilder.build());
    BulkReadWrapper bulkReadWrapper = new BulkReadVeneerApi(dataClient, TABLE_ID);

    ApiFuture<Result> row = bulkReadWrapper.add(ROW_KEY, Filters.FILTERS.key().regex("row"));
    row.get();
    assertTrue(row.isDone());
    assertEquals(1, fakeDataService.getReadRowsBatchCount());

    // To trigger closing of the batcher
    bulkReadWrapper.sendOutstanding();
  }

  @Test
  public void testSendOutstanding() throws Exception {
    Duration autoFlushTime = Duration.ofSeconds(30);
    settingsBuilder
        .stubSettings()
        .bulkReadRowsSettings()
        .setBatchingSettings(
            BatchingSettings.newBuilder()
                .setDelayThreshold(autoFlushTime)
                .setElementCountThreshold(100L)
                .setRequestByteThreshold(10L * 1024L)
                .build());
    dataClient = BigtableDataClient.create(settingsBuilder.build());
    BulkReadWrapper bulkReadWrapper = new BulkReadVeneerApi(dataClient, TABLE_ID);

    bulkReadWrapper.add(ByteString.copyFromUtf8("one"), null);
    bulkReadWrapper.sendOutstanding();

    bulkReadWrapper.add(ByteString.copyFromUtf8("two"), null);
    bulkReadWrapper.sendOutstanding();

    bulkReadWrapper.add(ByteString.copyFromUtf8("three"), null);
    bulkReadWrapper.sendOutstanding();

    bulkReadWrapper.add(ByteString.copyFromUtf8("four"), null);
    bulkReadWrapper.sendOutstanding();

    ApiFuture<Result> fifthBatchResult = bulkReadWrapper.add(ByteString.copyFromUtf8("five"), null);
    bulkReadWrapper.sendOutstanding();
    fifthBatchResult.get();

    assertEquals(5, fakeDataService.getReadRowsBatchCount());
  }

  @Test
  public void testWhenAutoFlushIsOff() throws Exception {
    Duration autoFlushTime = Duration.ofSeconds(10);
    settingsBuilder
        .stubSettings()
        .bulkReadRowsSettings()
        .setBatchingSettings(
            BatchingSettings.newBuilder()
                .setDelayThreshold(autoFlushTime)
                .setElementCountThreshold(10L)
                .setRequestByteThreshold(10L * 1024L)
                .build());
    dataClient = BigtableDataClient.create(settingsBuilder.build());
    BulkReadWrapper bulkReadWrapper = new BulkReadVeneerApi(dataClient, TABLE_ID);

    long startTime = System.currentTimeMillis();
    ApiFuture<Result> resultFuture = bulkReadWrapper.add(ROW_KEY, null);

    assertFalse(resultFuture.isDone());
    assertEquals(0, fakeDataService.getReadRowsBatchCount());

    // This does not guarantee instant result but it should take less time than autoFlush.
    bulkReadWrapper.sendOutstanding();
    resultFuture.get();

    long totalTime = System.currentTimeMillis() - startTime;

    assertTrue(totalTime < autoFlushTime.toMillis());
    assertTrue(resultFuture.isDone());
    assertEquals(1, fakeDataService.getReadRowsBatchCount());
  }

  private static class FakeDataService extends BigtableGrpc.BigtableImplBase {

    AtomicInteger readRowsCount = new AtomicInteger(0);

    int getReadRowsBatchCount() {
      return readRowsCount.intValue();
    }

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      readRowsCount.incrementAndGet();
      responseObserver.onNext(ReadRowsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
