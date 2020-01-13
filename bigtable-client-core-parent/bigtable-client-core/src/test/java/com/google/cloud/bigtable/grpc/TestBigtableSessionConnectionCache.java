/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import com.google.api.gax.rpc.ClientContext;
import com.google.bigtable.v2.BigtableGrpc.BigtableImplBase;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBigtableSessionConnectionCache {
  private static final ByteString ROW_KEY = ByteString.copyFromUtf8("testrow");
  private static final String FAMILY_NAME = "family";
  private static final ByteString QUALIFIER = ByteString.copyFromUtf8("qual");
  private static final ByteString VALUE = ByteString.copyFromUtf8("value");

  private List<BigtableSession> openedSessions = new ArrayList<>();
  private Server fakeServer;

  @Before
  public void setup() throws IOException {
    int port;
    try (ServerSocket ss = new ServerSocket(0)) {
      port = ss.getLocalPort();
    }

    fakeServer =
        ServerBuilder.forPort(port)
            .addService(
                new BigtableImplBase() {
                  @Override
                  public void readRows(
                      ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
                    responseObserver.onNext(
                        ReadRowsResponse.newBuilder()
                            .addChunks(
                                CellChunk.newBuilder()
                                    .setRowKey(ROW_KEY)
                                    .setFamilyName(StringValue.newBuilder().setValue(FAMILY_NAME))
                                    .setQualifier(BytesValue.newBuilder().setValue(QUALIFIER))
                                    .setTimestampMicros(10_000)
                                    .setValue(VALUE)
                                    .setCommitRow(true))
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build();
    fakeServer.start();
  }

  @After
  public void teardown() throws IOException {
    for (BigtableSession session : openedSessions) {
      session.close();
    }
    fakeServer.shutdownNow();
  }

  @Test
  public void testGcjConnectionsAreCaching() throws IOException {
    String[] connectionEndpoints = {"127.0.0.1", "127.0.0.2"};

    BigtableOptions.Builder optionsBuilder =
        BigtableOptions.builder()
            .setProjectId("fake-project")
            .setInstanceId("fake-instance")
            .setUsePlaintextNegotiation(true)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setUseGCJClient(true)
            .setUseCachedDataPool(true)
            .setPort(fakeServer.getPort())
            .setUserAgent("test");

    Map<String, ClientContext> context = getGcjContext(optionsBuilder, connectionEndpoints[0]);
    Assert.assertEquals(context.size(), 1);
    Assert.assertTrue(context.containsKey(connectionEndpoints[0]));
    ClientContext context0 = context.get(connectionEndpoints[0]);

    context = getGcjContext(optionsBuilder, connectionEndpoints[1]);
    Assert.assertEquals(context.size(), 2);
    Assert.assertTrue(context.containsKey(connectionEndpoints[1]));
    Assert.assertEquals(context0, context.get(connectionEndpoints[0]));
    ClientContext context1 = context.get(connectionEndpoints[1]);

    context = getGcjContext(optionsBuilder, connectionEndpoints[1]);
    Assert.assertEquals(context.size(), 2);
    Assert.assertEquals(context1, context.get(connectionEndpoints[1]));
    Assert.assertEquals(context0, context.get(connectionEndpoints[0]));

    context = getGcjContext(optionsBuilder, connectionEndpoints[0]);
    Assert.assertEquals(context.size(), 2);
    Assert.assertEquals(context1, context.get(connectionEndpoints[1]));
    Assert.assertEquals(context0, context.get(connectionEndpoints[0]));
  }

  private Map<String, ClientContext> getGcjContext(
      BigtableOptions.Builder optionsBuilder, String endpoint) throws IOException {
    optionsBuilder.setDataHost(endpoint);

    BigtableSession session = new BigtableSession(optionsBuilder.build());
    openedSessions.add(session);

    checkRows(session);
    return session.getCachedClientContexts();
  }

  private void checkRows(BigtableSession session) {
    List<FlatRow> rows =
        session.getDataClientWrapper().readFlatRowsList(Query.create("fake-table").rowKey(ROW_KEY));
    Assert.assertEquals(VALUE, rows.get(0).getCells().get(0).getValue());
  }
}
