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
package com.google.cloud.bigtable.hbase2_x;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import java.io.IOException;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.junit.Test;
import org.mockito.Mockito;

public class BigtableAdminTest {

  @Test
  public void testGetClusterStatus() throws IOException {
    // test to verify compatibility between 1x and 2x
    AbstractBigtableConnection connectionMock = mock(AbstractBigtableConnection.class);
    BigtableApi bigtableApi = mock(BigtableApi.class);
    Mockito.doReturn(bigtableApi).when(connectionMock).getBigtableApi();
    BigtableAdmin bigtableAdmin = new BigtableAdmin(connectionMock);

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
