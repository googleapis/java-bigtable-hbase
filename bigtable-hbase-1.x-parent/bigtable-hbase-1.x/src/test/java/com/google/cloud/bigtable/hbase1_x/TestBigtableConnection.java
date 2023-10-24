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
package com.google.cloud.bigtable.hbase1_x;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.test.helper.TestServerBuilder;
import com.google.common.collect.Queues;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBigtableConnection {

  private static final String TEST_PROJECT_ID = "fake-project-id";
  private static final String TEST_INSTANCE_ID = "fake-instance-id";
  private static final TableName TABLE_NAME = TableName.valueOf("test-table");
  private static final String FULL_TABLE_NAME =
      NameUtil.formatTableName(TEST_PROJECT_ID, TEST_INSTANCE_ID, TABLE_NAME.getNameAsString());
  private static Server server;

  private static FakeDataService fakeDataService = new FakeDataService();
  private Configuration configuration;
  private BigtableConnection connection;

  @BeforeClass
  public static void setUpServer() throws IOException {
    server = TestServerBuilder.newInstance().addService(fakeDataService).buildAndStart();
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
    configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + server.getPort());
    connection = new BigtableConnection(configuration);
  }

  @After
  public void tearDown() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  public void testOverloadedConstructor() throws IOException {
    try (Connection hbaseConnection =
        new BigtableConnection(configuration, false, Executors.newSingleThreadExecutor(), null)) {
      assertTrue(hbaseConnection.getAdmin() instanceof BigtableAdmin);
      assertTrue(hbaseConnection.getTable(TABLE_NAME) instanceof AbstractBigtableTable);
    }
  }

  @Test
  public void testGetters() throws IOException {
    assertTrue(connection.getAdmin() instanceof BigtableAdmin);
    assertTrue(
        connection.getTable(TABLE_NAME, Executors.newSingleThreadExecutor())
            instanceof AbstractBigtableTable);
    assertTrue(connection.getDisabledTables().isEmpty());
  }

  @Test
  public void testAllRegions() throws IOException, InterruptedException {
    List<HRegionInfo> regionInfoList = connection.getAllRegionInfos(TABLE_NAME);

    SampleRowKeysRequest request = fakeDataService.popLastRequest();
    assertEquals(FULL_TABLE_NAME, request.getTableName());

    assertEquals("", Bytes.toString(regionInfoList.get(0).getStartKey()));
    assertEquals("rowKey", Bytes.toString(regionInfoList.get(0).getEndKey()));
  }

  @Test
  public void testGetRegionLocation() throws IOException {
    HRegionLocation regionLocation =
        connection.getRegionLocator(TABLE_NAME).getRegionLocation("rowKey".getBytes());

    assertEquals("rowKey", Bytes.toString(regionLocation.getRegionInfo().getStartKey()));
    assertEquals("", Bytes.toString(regionLocation.getRegionInfo().getEndKey()));
  }

  private static class FakeDataService extends BigtableGrpc.BigtableImplBase {
    final BlockingQueue<Object> requests = Queues.newLinkedBlockingDeque();

    @SuppressWarnings("unchecked")
    <T> T popLastRequest() throws InterruptedException {
      return (T) requests.poll(1, TimeUnit.SECONDS);
    }

    @Override
    public void sampleRowKeys(
        SampleRowKeysRequest request, StreamObserver<SampleRowKeysResponse> responseObserver) {
      requests.add(request);
      responseObserver.onNext(
          SampleRowKeysResponse.newBuilder()
              .setRowKey(ByteString.copyFromUtf8("rowKey"))
              .setOffsetBytes(10000L)
              .build());
      responseObserver.onCompleted();
    }
  }
}
