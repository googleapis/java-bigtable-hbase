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
package com.google.cloud.bigtable.hbase2_x;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.bigtable.admin.v2.BigtableTableAdminGrpc;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BigtableAdminTest {

  private Server fakeServer;
  private BigtableConnection connection;
  private Admin admin;

  @Before
  public void setup() throws IOException {
    fakeServer =
        ServerBuilder.forPort(1234)
            .addService(new BigtableTableAdminGrpc.BigtableTableAdminImplBase() {})
            .build();
    fakeServer.start();

    Configuration configuration = BigtableConfiguration.configure("fake-project", "fake-instance");
    configuration.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + fakeServer.getPort());
    connection = new BigtableConnection(configuration);

    admin = BigtableAdmin.createInstance(connection);
  }

  @After
  public void tearDown() throws IOException {
    connection.close();
    fakeServer.shutdown();
  }

  @Test
  public void testGetClusterStatus() throws IOException {
    // test to verify compatibility between 1x and 2x
    BigtableAdmin bigtableAdmin = mock(BigtableAdmin.class);
    Mockito.when(bigtableAdmin.getClusterStatus()).thenCallRealMethod();

    ClusterStatus status = bigtableAdmin.getClusterStatus();
    assertNotNull(status);
    assertEquals("hbaseVersion", status.getHBaseVersion());
    assertEquals("clusterid", status.getClusterId());
    assertTrue(status.getServersName().isEmpty());
    assertTrue(status.getDeadServerNames().isEmpty());
    assertNull(status.getMaster());
    assertTrue(status.getBackupMasterNames().isEmpty());
    assertTrue(status.getRegionStatesInTransition().isEmpty());
    assertEquals(0, status.getMasterCoprocessors().length);
    assertFalse(status.getBalancerOn());
    assertEquals(-1, status.getMasterInfoPort());
  }
}
