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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import static com.google.cloud.bigtable.admin.v2.internal.NameUtil.extractTableIdFromTableName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.bigtable.admin.v2.BigtableTableAdminGrpc;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.common.collect.Queues;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBigtableClassicApi {

  private static final String TEST_PROJECT_ID = "fake-project-id";
  private static final String TEST_INSTANCE_ID = "fake-instance-id";
  private static final String ROW_KEY = "test-row-key";
  private static final String TABLE_ID = "test-table-id";

  private static FakeDataService fakeDataService = new FakeDataService();
  private static FakeAdminService fakeAdminService = new FakeAdminService();
  private static Server server;
  private static int port;

  private BigtableHBaseSettings bigtableHBaseSettings;
  private BigtableApi bigtableApi;

  @BeforeClass
  public static void setUpServer() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    server =
        ServerBuilder.forPort(port)
            .addService(fakeDataService)
            .addService(fakeAdminService)
            .build();
    server.start();
  }

  @AfterClass
  public static void tearDownServer() throws InterruptedException {
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
    }
  }

  @Before
  public void setUp() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    configuration.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + port);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT, false);
    bigtableHBaseSettings = BigtableHBaseClassicSettings.create(configuration);
    bigtableApi = BigtableApi.create(bigtableHBaseSettings);
  }

  @After
  public void tearDown() throws Exception {
    bigtableApi.close();
  }

  @Test
  public void testWithAlreadyBuildBigtableSession() throws Exception {
    BigtableHBaseClassicSettings settings = (BigtableHBaseClassicSettings) bigtableHBaseSettings;

    try (BigtableSession session = new BigtableSession(settings.getBigtableOptions());
        BigtableApi bigtableApi = new BigtableClassicApi(settings, session)) {

      assertSame(settings, bigtableApi.getBigtableHBaseSettings());

      bigtableApi.getAdminClient().deleteTableAsync(TABLE_ID).get();
      DeleteTableRequest deleteTableRequest = fakeAdminService.popLastRequest();
      assertEquals(TABLE_ID, extractTableIdFromTableName(deleteTableRequest.getName()));

      Query query = Query.create(TABLE_ID).rowKey(ROW_KEY);
      bigtableApi.getDataClient().readRowsAsync(query);
      ReadRowsRequest request = fakeDataService.popLastRequest();
      assertEquals(ROW_KEY, request.getRows().getRowKeys(0).toStringUtf8());
    }
  }

  @Test
  public void testAdminClient() throws Exception {
    assertTrue(bigtableApi.getAdminClient() instanceof AdminClientClassicApi);

    bigtableApi.getAdminClient().deleteTableAsync(TABLE_ID).get();
    DeleteTableRequest deleteTableRequest = fakeAdminService.popLastRequest();
    assertEquals(TABLE_ID, extractTableIdFromTableName(deleteTableRequest.getName()));
  }

  @Test
  public void testDataClient() throws Exception {
    assertTrue(bigtableApi.getDataClient() instanceof DataClientClassicApi);

    Query query = Query.create(TABLE_ID).rowKey(ROW_KEY);
    bigtableApi.getDataClient().readRowsAsync(query);
    ReadRowsRequest request = fakeDataService.popLastRequest();
    assertEquals(ROW_KEY, request.getRows().getRowKeys(0).toStringUtf8());
  }

  @Test
  public void testBigtableHBaseSettings() {
    assertSame(bigtableHBaseSettings, bigtableApi.getBigtableHBaseSettings());
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
      responseObserver.onNext(ReadRowsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static class FakeAdminService extends BigtableTableAdminGrpc.BigtableTableAdminImplBase {

    final BlockingQueue<Object> requests = Queues.newLinkedBlockingDeque();

    @SuppressWarnings("unchecked")
    <T> T popLastRequest() throws InterruptedException {
      return (T) requests.poll(1, TimeUnit.SECONDS);
    }

    @Override
    public void deleteTable(DeleteTableRequest request, StreamObserver<Empty> responseObserver) {
      requests.add(request);
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
