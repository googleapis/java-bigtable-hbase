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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.Version;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.BigtableHBaseVersion;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.cloud.bigtable.test.helper.TestServerBuilder;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.google.common.truth.Truth;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.internal.GrpcUtil;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.After;
import org.junit.Before;
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
  private ServerHeaderInterceptor headerInterceptor;

  @Mock private AbstractBigtableAdmin mockBigtableAdmin;

  @Mock private SampledRowKeysAdapter mockSampledAdapter;

  private AbstractBigtableConnection connection;

  @Before
  public void setUp() throws IOException {
    headerInterceptor = new ServerHeaderInterceptor();
    server =
        TestServerBuilder.newInstance()
            .intercept(headerInterceptor)
            .addService(fakeDataService)
            .buildAndStart();

    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, HOST_NAME + ":" + server.getPort());
    connection = new TestBigtableConnectionImpl(configuration);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    connection.close();
    if (server != null) {
      server.shutdownNow();
    }
  }

  @Test
  public void testRegionLocator() throws IOException {
    assertEquals(1, connection.getAllRegionInfos(TABLE_NAME).size());
    assertEquals(regionInfo, connection.getAllRegionInfos(TABLE_NAME).get(0));

    List<HRegionLocation> expectedRegionLocations =
        ImmutableList.of(
            new HRegionLocation(regionInfo, ServerName.valueOf(HOST_NAME, server.getPort(), 0)));

    when(mockSampledAdapter.adaptResponse(Mockito.<List<KeyOffset>>any()))
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
  public void testConnectionIsClosedWhenAborted() {
    assertFalse(connection.isAborted());
    assertFalse(connection.isClosed());

    connection.abort("Connection should be closed", new IOException());

    assertTrue(connection.isAborted());
    assertTrue(connection.isClosed());
  }

  @Test
  public void testHeaders() throws IOException {
    String rowKey = "test-row-key";
    String value = "mutation-value";

    Table table = connection.getTable(TABLE_NAME);
    table.put(
        new Put(Bytes.toBytes(rowKey))
            .addImmutable(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes(value)));

    Metadata metadata = headerInterceptor.receivedMetadata.get();
    String userAgent = metadata.get(Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER));
    Truth.assertThat(userAgent).contains("bigtable-java/" + Version.VERSION);
    Truth.assertThat(userAgent).contains("bigtable-hbase/" + BigtableHBaseVersion.getVersion());
    Truth.assertThat(userAgent).contains("hbase/" + VersionInfo.getVersion());
    Truth.assertThat(userAgent)
        .contains("grpc-java-netty/" + GrpcUtil.getGrpcBuildVersion().getImplementationVersion());

    String xGoogClient =
        metadata.get(Key.of("x-goog-api-client", Metadata.ASCII_STRING_MARSHALLER));
    Truth.assertThat(xGoogClient)
        .contains("gl-java/" + System.getProperty("java.specification.version"));
    Truth.assertThat(xGoogClient)
        .contains("grpc/" + GrpcUtil.getGrpcBuildVersion().getImplementationVersion());
    // Do we need this?
    // Truth.assertThat(xGoogClient).contains("cbt/" + BigtableHBaseVersion.getVersion());

    // Request must have exactly one of these
    String requestParams =
        metadata.get(Key.of("x-goog-request-params", Metadata.ASCII_STRING_MARSHALLER));
    String resourcePath =
        metadata.get(Key.of("google-cloud-resource-prefix", Metadata.ASCII_STRING_MARSHALLER));

    if (requestParams != null) {
      Truth.assertThat(requestParams)
          .contains(
              "table_name="
                  + Joiner.on("%2F")
                      .join(
                          "projects",
                          PROJECT_ID,
                          "instances",
                          INSTANCE_ID,
                          "tables",
                          TABLE_NAME.getNameAsString()));
      Truth.assertThat(resourcePath).isNull();
    } else {
      Truth.assertThat(resourcePath)
          .isEqualTo(
              String.format(
                  "projects/%s/instances/%s/tables/%s",
                  PROJECT_ID, INSTANCE_ID, TABLE_NAME.getNameAsString()));
    }
  }

  @Test
  public void testManagedConnectionOverride() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, INSTANCE_ID);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    configuration.setInt(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, 1);
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, HOST_NAME + ":" + server.getPort());
    configuration.setBoolean(BigtableOptionsFactory.MANAGED_CONNECTION_WARNING, true);
    try (Connection newConnection =
        new TestBigtableConnectionImpl(
            configuration, true, Executors.newSingleThreadExecutor(), null)) {
    } catch (IllegalArgumentException e) {
      fail("Should not throw IllegalArgumentException");
    }
  }

  private class TestBigtableConnectionImpl extends AbstractBigtableConnection {

    TestBigtableConnectionImpl(Configuration conf) throws IOException {
      super(conf);
    }

    TestBigtableConnectionImpl(Configuration conf, boolean managed, ExecutorService pool, User user)
        throws IOException {
      super(conf, managed, pool, user);
    }

    @Override
    protected SampledRowKeysAdapter createSampledRowKeysAdapter(
        TableName tableName, ServerName serverName) {
      return mockSampledAdapter;
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService executorService) {
      return new AbstractBigtableTable(this, createAdapter(tableName)) {
        @Override
        public void mutateRow(RowMutations rowMutations) throws IOException {
          mutateRowVoid(rowMutations);
        }
      };
    }

    @Override
    public Admin getAdmin() {
      return mockBigtableAdmin;
    }

    @Override
    public String getClusterId() throws IOException {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public List<HRegionInfo> getAllRegionInfos(TableName tableName) {
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

  private static class ServerHeaderInterceptor implements ServerInterceptor {
    private AtomicReference<Metadata> receivedMetadata = new AtomicReference<>();

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> serverCall,
        Metadata metadata,
        ServerCallHandler<ReqT, RespT> serverCallHandler) {
      receivedMetadata.set(metadata);
      return serverCallHandler.startCall(serverCall, metadata);
    }
  }
}
