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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

// TODO: consider turning this into an emulator test that targets the HBase api instead
// of SharedDataClientWrapperFactory. The pre-req would be to have a collection of tests
// for hbase 1x & hbase2x that hermetic. Currently the only HBase level tests we have,
// target the actual emulator, minicluster or prod.
/**
 * Test CACHED_DATA_CHANNEL_POOL feature by targeting an in process bigtable impl and counting the
 * unique client ip/port combinations.
 */
@RunWith(JUnit4.class)
public class SharedDataClientWrapperFactoryTest {
  private Server server;
  private int port;
  private List<SocketAddress> remoteCallers;

  @Before
  public void setUp() throws IOException {
    try (ServerSocket ss = new ServerSocket(0)) {
      port = ss.getLocalPort();
    }

    remoteCallers = Collections.synchronizedList(new ArrayList<SocketAddress>());

    server =
        ServerBuilder.forPort(port)
            .intercept(new RemoteCallerInterceptor(remoteCallers))
            .addService(new FakeBigtable())
            .build();
    server.start();
  }

  @After
  public void tearDown() {
    server.shutdown();
  }

  @Test
  public void testChannelsAreShared() throws Exception {
    int channelCount = 2;
    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, "my-project");
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, "my-instance");
    // primary paramters being tested
    configuration.setInt(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, channelCount);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, true);

    // Manually expand BIGTABLE_EMULATOR_HOST_KEY so that the channel settings are known
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, "localhost");
    configuration.setInt(BigtableOptionsFactory.BIGTABLE_PORT_KEY, port);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);

    // Build the settings
    BigtableHBaseVeneerSettings settings = BigtableHBaseVeneerSettings.create(configuration);

    // Create a bunch of clients and make multiple calls
    SharedDataClientWrapperFactory factory = new SharedDataClientWrapperFactory();
    List<DataClientWrapper> clients = new ArrayList<>();

    for (int i = 0; i < channelCount * 5; i++) {
      for (int j = 0; j < channelCount * 5; j++) {
        DataClientWrapper dataClient = factory.createDataClient(settings);
        clients.add(dataClient);
        dataClient.mutateRowAsync(RowMutation.create("fake-table", "fake-key").deleteRow()).get();
      }
    }

    // cleanup
    for (DataClientWrapper client : clients) {
      client.close();
    }

    // Despite having multiple client instances, there should only be `channelCount` remoteCallers
    Set<SocketAddress> uniqueRemoteCallers = new HashSet<>(remoteCallers);
    Assert.assertEquals(channelCount, uniqueRemoteCallers.size());
  }

  /**
   * Interceptor that will collect remote addresses of the callers.
   *
   * <p>The collection given to this interceptor must be threadsafe
   */
  private static class RemoteCallerInterceptor implements ServerInterceptor {
    private final Collection<SocketAddress> remoteAddrs;

    RemoteCallerInterceptor(Collection<SocketAddress> remoteAddrs) {
      this.remoteAddrs = remoteAddrs;
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> serverCall,
        Metadata metadata,
        ServerCallHandler<ReqT, RespT> serverCallHandler) {
      remoteAddrs.add(serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
      return serverCallHandler.startCall(serverCall, metadata);
    }
  }

  /** Minimal implementation of Bigtable service that will retur OK for any mutation */
  private static class FakeBigtable extends BigtableGrpc.BigtableImplBase {
    @Override
    public void mutateRow(
        MutateRowRequest request, StreamObserver<MutateRowResponse> responseObserver) {
      responseObserver.onNext(MutateRowResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
