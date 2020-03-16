/*
 * Copyright 2020 Google LLC.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.BigtableWrapper;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.ServerSocket;
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

  private static Server server;
  private static int port;

  private BigtableHBaseSettings bigtableHBaseSettings;
  private BigtableWrapper bigtableWrapper;

  @BeforeClass
  public static void setUpServer() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    server = ServerBuilder.forPort(port).addService(new BigtableGrpc.BigtableImplBase() {}).build();
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
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    configuration.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + port);
    bigtableHBaseSettings = BigtableHBaseClassicSettings.create(configuration);
    bigtableWrapper = BigtableWrapper.create(bigtableHBaseSettings);
  }

  @After
  public void tearDown() throws Exception {
    bigtableWrapper.close();
  }

  @Test
  public void testAdminClient() throws IOException {
    assertTrue(bigtableWrapper.getAdminClient() instanceof AdminClientClassicApi);
  }

  @Test
  public void testDataClient() {
    assertTrue(bigtableWrapper.getDataClient() instanceof DataClientClassicApi);
  }

  @Test
  public void testBigtableHBaseSettings() {
    assertEquals(bigtableHBaseSettings, bigtableWrapper.getBigtableHBaseSettings());
  }
}
