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
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class TestBulkReadVeneerApi {

  private static final String TABLE_ID = "fake-table-id";
  private FakeDataService fakeDataService = new FakeDataService();
  private BigtableDataSettings.Builder settingsBuilder;
  private Server server;

  @Before
  public void setUp() throws IOException {
    final int port;
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }

    server = ServerBuilder.forPort(port).addService(fakeDataService).build();
    server.start();

    settingsBuilder =
        BigtableDataSettings.newBuilderForEmulator(port)
            .setProjectId("fake-project")
            .setInstanceId("fake-instance");
  }

  @Test
  public void tearDown() throws InterruptedException {
    if (server != null) {
      server.shutdown();
      server.awaitTermination();
    }
  }

  @Test
  public void testAdd() throws Exception {
    BulkReadVeneerApi bulkReadWrapper =
        new BulkReadVeneerApi(BigtableDataClient.create(settingsBuilder.build()), TABLE_ID);
    ApiFuture<Result> resultFuture = bulkReadWrapper.add(ByteString.copyFromUtf8("one"), null);
    assertFalse(resultFuture.isDone());
    // Here AutoFlush triggers the batch
    resultFuture.get();

    Filters.Filter filter = Filters.FILTERS.key().regex("cf");
    ApiFuture<Result> secBatchResult = bulkReadWrapper.add(ByteString.copyFromUtf8("1"), filter);
    secBatchResult.get();

    assertTrue(resultFuture.isDone());
    assertTrue(secBatchResult.isDone());

    assertEquals(2, fakeDataService.getReadRowsCount());
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
                .setElementCountThreshold(10L)
                .setRequestByteThreshold(10L * 1024L)
                .build());
    BulkReadVeneerApi bulkReadWrapper =
        new BulkReadVeneerApi(BigtableDataClient.create(settingsBuilder.build()), TABLE_ID);

    bulkReadWrapper.add(ByteString.copyFromUtf8("one"), null);
    bulkReadWrapper.sendOutstanding();

    bulkReadWrapper.add(ByteString.copyFromUtf8("two"), null);
    bulkReadWrapper.sendOutstanding();

    bulkReadWrapper.add(ByteString.copyFromUtf8("three"), null);
    bulkReadWrapper.sendOutstanding();

    bulkReadWrapper.add(ByteString.copyFromUtf8("four"), null);
    bulkReadWrapper.sendOutstanding();

    bulkReadWrapper.add(ByteString.copyFromUtf8("five"), null).get();

    assertEquals(5, fakeDataService.getReadRowsCount());
  }

  @Test
  public void testWhenAutoFlushIsOff() throws Exception {
    Duration autoFlushTime = Duration.ofSeconds(30);
    settingsBuilder
        .stubSettings()
        .bulkReadRowsSettings()
        .setBatchingSettings(
            BatchingSettings.newBuilder()
                .setDelayThreshold(autoFlushTime)
                .setElementCountThreshold(10L)
                .setRequestByteThreshold(10L * 1024L)
                .build());
    BulkReadVeneerApi bulkReadWrapper =
        new BulkReadVeneerApi(BigtableDataClient.create(settingsBuilder.build()), TABLE_ID);

    long startTime = System.currentTimeMillis();
    ApiFuture<Result> resultFuture = bulkReadWrapper.add(ByteString.copyFromUtf8("row-key"), null);
    assertFalse(resultFuture.isDone());

    // This does not guarantee instance result but it will take less time then autoFlush.
    bulkReadWrapper.sendOutstanding();

    // TODO: investigate if I am not adding this(another) entry then it is not resolving
    bulkReadWrapper.add(ByteString.copyFromUtf8("two"), null);

    resultFuture.get();
    long totalTime = System.currentTimeMillis() - startTime;
    assertTrue(totalTime < autoFlushTime.toMillis());
    assertTrue(resultFuture.isDone());
  }

  private static class FakeDataService extends BigtableGrpc.BigtableImplBase {

    int readRowsCount = 0;

    int getReadRowsCount() {
      return readRowsCount;
    }

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      readRowsCount++;
      responseObserver.onNext(ReadRowsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
