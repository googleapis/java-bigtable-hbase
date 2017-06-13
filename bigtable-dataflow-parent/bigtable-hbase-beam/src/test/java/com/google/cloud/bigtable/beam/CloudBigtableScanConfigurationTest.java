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
package com.google.cloud.bigtable.beam;

import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;

/**
 * Tests for {@link CloudBigtableScanConfiguration}.
 */
public class CloudBigtableScanConfigurationTest {

  public static final String PROJECT = "project";
  public static final String INSTANCE = "instance";
  public static final String TABLE = "table";

  public static final byte[] START_ROW = "aa".getBytes();
  public static final byte[] STOP_ROW = "zz".getBytes();

  private static final CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
      .withProjectId(PROJECT)
      .withInstanceId(INSTANCE)
      .withTableId(TABLE)
      .withScan(new Scan(START_ROW, STOP_ROW))
      .build();

  @Test
  public void testSerialization() {
    CloudBigtableScanConfiguration serialized = SerializableUtils.ensureSerializable(config);

    Assert.assertEquals(PROJECT, serialized.getProjectId());
    Assert.assertEquals(INSTANCE, serialized.getInstanceId());
    Assert.assertEquals(TABLE, serialized.getTableId());
    Assert.assertArrayEquals(START_ROW, serialized.getZeroCopyStartRow());
    Assert.assertArrayEquals(STOP_ROW, serialized.getZeroCopyStopRow());
  }

  @Test
  public void testEquals() {
    Scan scan1 = new Scan();
    Scan scan2 = new Scan(START_ROW, STOP_ROW);
    CloudBigtableScanConfiguration underTest1 = config.toBuilder().withScan(scan1).build();
    CloudBigtableScanConfiguration underTest2 = config.toBuilder().withScan(scan1).build();
    CloudBigtableScanConfiguration underTest3 = config.toBuilder().withScan(scan2).build();

    // Test CloudBigtableScanConfigurations that should be equal.
    Assert.assertEquals(underTest1, underTest2);

    // Test that CloudBigtableScanConfigurations with different scans should not be equal.
    Assert.assertNotEquals(underTest1, underTest3);
  }

  @Test
  public void testToBuilder() {
    CloudBigtableScanConfiguration copy = config.toBuilder().build();
    Assert.assertNotSame(config, copy);
    Assert.assertEquals(config, copy);
  }
}

