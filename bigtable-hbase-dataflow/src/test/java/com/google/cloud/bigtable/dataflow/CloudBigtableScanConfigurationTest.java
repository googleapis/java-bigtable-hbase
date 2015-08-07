/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflow;

import org.apache.hadoop.hbase.client.Scan;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;

import java.io.IOException;

/**
 * Tests for {@link CloudBigtableScanConfiguration}.
 */
public class CloudBigtableScanConfigurationTest {

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException{
    byte[] startRow = "aa".getBytes();
    byte[] stopRow = "zz".getBytes();
    String projectId = "project";
    String zone = "zone";
    String cluster = "cluster";
    String table = "table";
    CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
      .withProjectId(projectId)
      .withZoneId(zone)
      .withClusterId(cluster)
      .withTableId(table)
      .withScan(new Scan(startRow, stopRow))
      .build();

    CloudBigtableScanConfiguration serialized = SerializableUtils.ensureSerializable(config);

    Assert.assertEquals(projectId, serialized.getProjectId());
    Assert.assertEquals(zone, serialized.getZoneId());
    Assert.assertEquals(cluster, serialized.getClusterId());
    Assert.assertEquals(table, serialized.getTableId());
    Scan scan = serialized.getScan();
    Assert.assertArrayEquals(startRow, scan.getStartRow());
    Assert.assertArrayEquals(stopRow, scan.getStopRow());
  }
}

