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
package org.apache.hadoop.hbase.client;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.cloud.bigtable.hbase.wrappers.classic.BigtableClassicApi;
import com.google.cloud.bigtable.hbase.wrappers.classic.BigtableHBaseClassicSettings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestAbstractBigtableConnection {

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final String PROJECT_ID = "test-project-id";
  private static final String INSTANCE_ID = "test-instance-id";
  private static final String HOST_NAME = "localhost";
  private static final TableName TABLE_NAME = TableName.valueOf("test-table");
  private static final HRegionInfo regionInfo =
      new HRegionInfo(TABLE_NAME, "a".getBytes(), "z".getBytes());

  private static final FakeDataService fakeDataService = new FakeDataService();

  private static Server server;
  private static int port;

  @Mock private AbstractBigtableAdmin mockBigtableAdmin;

  @Mock private SampledRowKeysAdapter mockSampledAdapter;

  private Configuration configuration;
  private AbstractBigtableConnection connection;

  @BeforeClass
  public static void setUpServer() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    server = ServerBuilder.forPort(port).addService(fakeDataService).build();
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
    configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    configuration.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, HOST_NAME + ":" + port);
    connection = new TestBigtableConnectionImpl(configuration);
  }

  @After
  public void tearDown() throws IOException {
    connection.close();
  }

  @Test
  public void testGetter() throws IOException {
    assertEquals(configuration.size(), connection.getConfiguration().size());
    assertTrue(connection.getBigtableHBaseSettings() instanceof BigtableHBaseClassicSettings);
    assertEquals(connection.getOptions(), connection.getSession().getOptions());

    assertTrue(connection.getDisabledTables().isEmpty());

    assertEquals(mockBigtableAdmin, connection.getAdmin());
    assertEquals(mockSampledAdapter, connection.createSampledRowKeysAdapter(TABLE_NAME, null));

    assertEquals(TABLE_NAME, connection.getBufferedMutator(TABLE_NAME).getName());
    assertTrue(connection.getBigtableApi() instanceof BigtableClassicApi);
  }

  @Test
  public void testRegionLocator() throws IOException {
    assertEquals(1, connection.getAllRegionInfos(TABLE_NAME).size());
    assertEquals(regionInfo, connection.getAllRegionInfos(TABLE_NAME).get(0));

    List<HRegionLocation> expectedRegionLocations =
        ImmutableList.of(new HRegionLocation(regionInfo, ServerName.valueOf(HOST_NAME, port, 0)));

    Mockito.when(mockSampledAdapter.adaptResponse(Mockito.<List<KeyOffset>>any()))
        .thenReturn(expectedRegionLocations);
    RegionLocator regionLocator = connection.getRegionLocator(TABLE_NAME);

    assertEquals(expectedRegionLocations, regionLocator.getAllRegionLocations());
  }

  @Test
  public void testTable() throws IOException, InterruptedException {
    String rowKey = "test-row-key";
    String value = "mutation-value";

    Table table = connection.getTable(TABLE_NAME);
    table.put(
        new Put(Bytes.toBytes(rowKey))
            .addImmutable(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes(value)));

    MutateRowRequest request = fakeDataService.popLastRequest();

    assertEquals(rowKey, request.getRowKey().toStringUtf8());
    assertEquals(value, request.getMutations(0).getSetCell().getValue().toStringUtf8());
  }

  @Test
  public void testToString() {
    String abstractTableToStr = connection.toString();
    assertThat(abstractTableToStr, containsString("project=" + PROJECT_ID));
    assertThat(abstractTableToStr, containsString("instance=" + INSTANCE_ID));
    assertThat(abstractTableToStr, containsString("dataHost=" + HOST_NAME));
    assertThat(abstractTableToStr, containsString("tableAdminHost=" + HOST_NAME));
  }

  @Test
  public void testAbortAndClosed() {
    assertFalse(connection.isAborted());
    assertFalse(connection.isClosed());

    connection.abort("satat", new IOException(""));

    assertTrue(connection.isAborted());
    assertTrue(connection.isClosed());
  }

  private class TestBigtableConnectionImpl extends AbstractBigtableConnection {

    public TestBigtableConnectionImpl(Configuration conf) throws IOException {
      super(conf);
    }

    protected TestBigtableConnectionImpl(
        Configuration conf, boolean managed, ExecutorService pool, User user) throws IOException {
      super(conf, managed, pool, user);
    }

    @Override
    protected SampledRowKeysAdapter createSampledRowKeysAdapter(
        TableName tableName, ServerName serverName) {
      return mockSampledAdapter;
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService executorService) throws IOException {
      return new AbstractBigtableTable(this, createAdapter(tableName)) {};
    }

    @Override
    public Admin getAdmin() throws IOException {
      return mockBigtableAdmin;
    }

    @Override
    public List<HRegionInfo> getAllRegionInfos(TableName tableName) throws IOException {
      return ImmutableList.of(regionInfo);
    }
  }

  private static class FakeDataService extends BigtableGrpc.BigtableImplBase {
    final BlockingQueue<Object> requests = Queues.newLinkedBlockingDeque();

    @SuppressWarnings("unchecked")
    <T> T popLastRequest() throws InterruptedException {
      return (T) requests.poll(1, TimeUnit.SECONDS);
    }

    @Override
    public void mutateRow(
        MutateRowRequest request, StreamObserver<MutateRowResponse> responseObserver) {
      requests.add(request);
      responseObserver.onNext(MutateRowResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void sampleRowKeys(
        SampleRowKeysRequest request, StreamObserver<SampleRowKeysResponse> responseObserver) {

      requests.add(request);
      responseObserver.onNext(SampleRowKeysResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
