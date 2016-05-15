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

import com.google.bigtable.v1.RowRange;
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

  @Test
  public void testSerialization() {
    CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
      .withProjectId(PROJECT)
      .withZoneId(ZONE)
      .withClusterId(CLUSTER)
      .withTableId(TABLE)
      .withScan(new Scan(START_ROW, STOP_ROW))
      .build();

    CloudBigtableScanConfiguration serialized = SerializableUtils.ensureSerializable(config);

    Assert.assertEquals(PROJECT, serialized.getProjectId());
    Assert.assertEquals(ZONE, serialized.getZoneId());
    Assert.assertEquals(CLUSTER, serialized.getClusterId());
    Assert.assertEquals(TABLE, serialized.getTableId());
    final RowRange rowRange = serialized.getReadRowsRequest().getRowRange();
    Assert.assertArrayEquals(START_ROW, rowRange.getStartKey().toByteArray());
    Assert.assertArrayEquals(STOP_ROW, rowRange.getEndKey().toByteArray());
  }

  @Test
  public void testEquals() {
    Scan scan1 = new Scan();
    Scan scan2 = new Scan(START_ROW, STOP_ROW);
    CloudBigtableScanConfiguration underTest1 = createConfig(scan1);
    CloudBigtableScanConfiguration underTest2 = createConfig(scan1);
    CloudBigtableScanConfiguration underTest3 = createConfig(scan2);

    // Test CloudBigtableScanConfigurations that should be equal.
    Assert.assertEquals(underTest1, underTest2);

    // Test that CloudBigtableScanConfigurations with different scans should not be equal.
    Assert.assertNotEquals(underTest1, underTest3);
  }

  protected CloudBigtableScanConfiguration createConfig(Scan scan) {
    return new CloudBigtableScanConfiguration.Builder()
        .withProjectId(PROJECT)
        .withZoneId(ZONE)
        .withClusterId(CLUSTER)
        .withTableId(TABLE)
        .withScan(scan)
        .build();
  }

  @Test
  public void testToBuilder() {
    CloudBigtableScanConfiguration underTest = createConfig(new Scan(START_ROW, STOP_ROW));
    CloudBigtableScanConfiguration copy = underTest.toBuilder().build();
    Assert.assertNotSame(underTest, copy);
    Assert.assertEquals(underTest, copy);
  }
}

