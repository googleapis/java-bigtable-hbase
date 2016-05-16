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

/**
 * Tests for {@link CloudBigtableScanConfiguration}.
 */
public class CloudBigtableScanConfigurationTest {

  public static final String PROJECT = "project";
  public static final String ZONE = "zone";
  public static final String CLUSTER = "cluster";
  public static final String TABLE = "table";

  public static final byte[] START_ROW = "aa".getBytes();
  public static final byte[] STOP_ROW = "zz".getBytes();


  private static CloudBigtableScanConfiguration createConfiguration(Scan scan) {
    return new CloudBigtableScanConfiguration.Builder()
        .withProjectId(PROJECT)
        .withZoneId(ZONE)
        .withClusterId(CLUSTER)
        .withTableId(TABLE)
        .withScan(scan)
        .build();
  }

  @Test
  public void testSerialization() {
    CloudBigtableScanConfiguration config = createConfiguration(new Scan(START_ROW, STOP_ROW));

    CloudBigtableScanConfiguration serialized = SerializableUtils.ensureSerializable(config);

    Assert.assertEquals(PROJECT, serialized.getProjectId());
    Assert.assertEquals(ZONE, serialized.getZoneId());
    Assert.assertEquals(CLUSTER, serialized.getClusterId());
    Assert.assertEquals(TABLE, serialized.getTableId());
    Scan scan = serialized.getScan();
    Assert.assertArrayEquals(START_ROW, scan.getStartRow());
    Assert.assertArrayEquals(STOP_ROW, scan.getStopRow());
  }

  @Test
  public void testEquals() {
    Scan scan1 = new Scan();
    Scan scan2 = new Scan(START_ROW, STOP_ROW);
    CloudBigtableScanConfiguration underTest1 = createConfiguration(scan1);
    CloudBigtableScanConfiguration underTest2 = createConfiguration(scan1);
    CloudBigtableScanConfiguration underTest3 = createConfiguration(scan2);

    // Test CloudBigtableScanConfigurations that should be equal.
    Assert.assertEquals(underTest1, underTest2);

    // Test that CloudBigtableScanConfigurations with different scans should not be equal.
    Assert.assertNotEquals(underTest1, underTest3);
  }

  @Test
  public void testToBuilder() {
    CloudBigtableScanConfiguration underTest = createConfiguration(new Scan(START_ROW, STOP_ROW));
    CloudBigtableScanConfiguration copy = underTest.toBuilder().build();
    Assert.assertNotSame(underTest, copy);
    Assert.assertEquals(underTest, copy);
  }
}
