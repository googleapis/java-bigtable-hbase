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

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.PingAndWarmRequest;
import com.google.bigtable.v2.PingAndWarmResponse;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.hbase2_x.BigtableConnection;
import com.google.common.collect.Queues;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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
    try (BigtableConnection connection = new BigtableConnection(conf)) {
      Admin admin = connection.getAdmin();
      Table table = connection.getTable(TableName.valueOf("someTable"));
      BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("someTable"));
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testHbckErrorDeferred() throws IOException {
    Server server = createFakeServer();

    Configuration conf = BigtableConfiguration.configure("projectId", "instanceId", "appProfileId");
    conf.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + server.getPort());
    conf.setInt(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, 1);
    conf.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    conf.set(BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "false");

    try (BigtableConnection c1 = new BigtableConnection(conf);
        BigtableConnection c2 = new BigtableConnection(conf)) {

      // Should not throw
      Hbck hbck1 = c1.getHbck();
      Hbck hbck2 = c2.getHbck();

      Assert.assertThrows(
          UnsupportedOperationException.class,
          () -> {
            try {
              hbck1.runHbckChore();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });

      Assert.assertEquals(hbck1, hbck1);
      Assert.assertNotEquals(hbck1, hbck2);

      // Make sure that the hashCode is stable
      Assert.assertEquals(hbck1.hashCode(), hbck1.hashCode());
      // And differs for different instances
      Assert.assertNotEquals(hbck1.hashCode(), hbck2.hashCode());

      Assert.assertEquals("UnsupportedHbck", hbck1.toString());
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testGetRegionLocation() throws IOException {
    Server server = createFakeServer();

    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, "project_id");
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, "instance_id");
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + server.getPort());

    try (BigtableConnection connection = new BigtableConnection(configuration)) {
      RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf("table_id"));

      // SampleRowKeys returns a, b, c ... z
      HRegionLocation regionLocation = regionLocator.getRegionLocation("1".getBytes());
      Assert.assertEquals("", Bytes.toString(regionLocation.getRegion().getStartKey()));
      Assert.assertEquals("a", Bytes.toString(regionLocation.getRegion().getEndKey()));

      regionLocation = regionLocator.getRegionLocation("a".getBytes());
      Assert.assertEquals("a", Bytes.toString(regionLocation.getRegion().getStartKey()));
      Assert.assertEquals("b", Bytes.toString(regionLocation.getRegion().getEndKey()));

      regionLocation = regionLocator.getRegionLocation("bbb".getBytes());
      Assert.assertEquals("b", Bytes.toString(regionLocation.getRegion().getStartKey()));
      Assert.assertEquals("c", Bytes.toString(regionLocation.getRegion().getEndKey()));

      regionLocation = regionLocator.getRegionLocation("d".getBytes());
      Assert.assertEquals("d", Bytes.toString(regionLocation.getRegion().getStartKey()));
      Assert.assertEquals("e", Bytes.toString(regionLocation.getRegion().getEndKey()));

      regionLocation = regionLocator.getRegionLocation("z".getBytes());
      Assert.assertEquals("z", Bytes.toString(regionLocation.getRegion().getStartKey()));
      Assert.assertEquals("", Bytes.toString(regionLocation.getRegion().getEndKey()));

      regionLocation = regionLocator.getRegionLocation("zzz".getBytes());
      Assert.assertEquals("z", Bytes.toString(regionLocation.getRegion().getStartKey()));
      Assert.assertEquals("", Bytes.toString(regionLocation.getRegion().getEndKey()));
    } finally {
      server.shutdown();
    }
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

  private static class FakeDataService extends BigtableGrpc.BigtableImplBase {
    final BlockingQueue<Object> requests = Queues.newLinkedBlockingDeque();

    @SuppressWarnings("unchecked")
    <T> T popLastRequest() throws InterruptedException {
      return (T) requests.poll(1, TimeUnit.SECONDS);
    }

    @Override
    public void pingAndWarm(
        PingAndWarmRequest request, StreamObserver<PingAndWarmResponse> responseObserver) {
      responseObserver.onNext(PingAndWarmResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void sampleRowKeys(
        SampleRowKeysRequest request, StreamObserver<SampleRowKeysResponse> responseObserver) {
      requests.add(request);
      long offset = 1000L;
      for (char i = 'a'; i <= 'z'; i++) {
        responseObserver.onNext(
            SampleRowKeysResponse.newBuilder()
                .setRowKey(ByteString.copyFromUtf8(String.valueOf(i)))
                .setOffsetBytes(offset)
                .build());
        offset += 1000;
      }
      responseObserver.onCompleted();
    }
  }
}
