/*
 * Copyright 2016 Google LLC
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

import org.junit.Assert;
import org.junit.Test;

public class TestBigtableClusterName {

  @Test
  public void getInstanceId() {
    String clusterName = "projects/proj/instances/inst/clusters/cluster";
    Assert.assertEquals("inst", new BigtableClusterName(clusterName).getInstanceId());
  }

  @Test
  public void getClusterId() {
    String clusterName = "projects/proj/instances/inst/clusters/cluster1";
    Assert.assertEquals("cluster1", new BigtableClusterName(clusterName).getClusterId());
  }

  @Test
  public void createBackupName() throws Exception {
    String clusterName = "projects/proj/instances/inst/clusters/cluster1";
    Assert.assertEquals(
        clusterName + "/backups/backup",
        new BigtableClusterName(clusterName).toBackupName("backup"));
  }
}
