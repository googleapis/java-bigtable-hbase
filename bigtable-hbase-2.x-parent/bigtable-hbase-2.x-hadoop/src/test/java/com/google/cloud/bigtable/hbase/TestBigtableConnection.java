/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.bigtable.hbase;

import com.google.bigtable.repackaged.com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.repackaged.com.google.bigtable.v2.PingAndWarmRequest;
import com.google.bigtable.repackaged.com.google.bigtable.v2.PingAndWarmResponse;
import com.google.bigtable.repackaged.io.grpc.Server;
import com.google.bigtable.repackaged.io.grpc.ServerBuilder;
import com.google.bigtable.repackaged.io.grpc.stub.StreamObserver;
import com.google.cloud.bigtable.hbase2_x.BigtableConnection;
import java.io.IOException;
import java.net.ServerSocket;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This is a test to ensure that {@link BigtableConfiguration} can find {@link BigtableConnection}
 */
@RunWith(JUnit4.class)
public class TestBigtableConnection {

  @Test
  public void testBigtableConnectionExists() {
    Assert.assertEquals(BigtableConnection.class, BigtableConfiguration.getConnectionClass());
  }

  @Test
  public void testConfig_Basic() {
    Configuration conf = BigtableConfiguration.configure("projectId", "instanceId");
    Assert.assertEquals("projectId", conf.get(BigtableOptionsFactory.PROJECT_ID_KEY));
    Assert.assertEquals("instanceId", conf.get(BigtableOptionsFactory.INSTANCE_ID_KEY));
    Assert.assertNull(conf.get(BigtableOptionsFactory.APP_PROFILE_ID_KEY));
    Assert.assertEquals(
        BigtableConfiguration.getConnectionClass().getName(),
        conf.get(BigtableConfiguration.HBASE_CLIENT_CONNECTION_IMPL));
  }

  @Test
  public void testConfig_AppProfile() {
    Configuration conf = BigtableConfiguration.configure("projectId", "instanceId", "appProfileId");
    Assert.assertEquals(conf.get(BigtableOptionsFactory.PROJECT_ID_KEY), "projectId");
    Assert.assertEquals(conf.get(BigtableOptionsFactory.INSTANCE_ID_KEY), "instanceId");
    Assert.assertEquals(conf.get(BigtableOptionsFactory.APP_PROFILE_ID_KEY), "appProfileId");
    Assert.assertEquals(
        BigtableConfiguration.getConnectionClass().getName(),
        conf.get(BigtableConfiguration.HBASE_CLIENT_CONNECTION_IMPL));
  }

  @Test
  public void testTable() throws IOException {
    Server server = createFakeServer();

    Configuration conf = BigtableConfiguration.configure("projectId", "instanceId", "appProfileId");
    conf.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + server.getPort());
    conf.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    conf.set(BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "false");

    try (BigtableConnection connection = new BigtableConnection(conf);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf("someTable"));
        BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("someTable"))) {}
  }

  static Server createFakeServer() throws IOException {
    Server server = null;
    for (int i = 10; i >= 0; i--) {
      int port;
      try (ServerSocket ss = new ServerSocket(0)) {
        port = ss.getLocalPort();
      }
      try {
        return ServerBuilder.forPort(port).addService(new FakeDataService()).build().start();
      } catch (IOException e) {
        if (i == 0) {
          throw e;
        }
      }
    }
    throw new IllegalStateException("This should never happen");
  }

  static class FakeDataService extends BigtableGrpc.BigtableImplBase {
    @Override
    public void pingAndWarm(
        PingAndWarmRequest request, StreamObserver<PingAndWarmResponse> responseObserver) {
      responseObserver.onNext(PingAndWarmResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
