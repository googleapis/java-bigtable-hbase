/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.coders;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.beam.hbasesnapshots.SnapshotTestHelper;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link RegionConfigCoder} to ensure correct serialization and deserialization. */
@RunWith(JUnit4.class)
public class RegionConfigCoderTest {

  /**
   * Tests that {@link RegionConfigCoder} correctly encodes and decodes a {@link RegionConfig}
   * object with empty region start and end keys.
   */
  @Test
  public void testEncodeDecode_emptyKeys() throws Exception {
    SnapshotConfig snapshotConfig = SnapshotTestHelper.newSnapshotConfig("my-restore-path");

    TableName tableName = TableName.valueOf("my-table");
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).build();

    RegionConfig originalConfig =
        RegionConfig.builder()
            .setSnapshotConfig(snapshotConfig)
            .setRegionInfo(regionInfo)
            .setTableDescriptor(tableDescriptor)
            .setRegionSize(12345L)
            .build();

    RegionConfigCoder coder = new RegionConfigCoder();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    coder.encode(originalConfig, outStream);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    RegionConfig decodedConfig = coder.decode(inStream);

    assertEquals(originalConfig, decodedConfig);
  }

  /**
   * Tests that {@link RegionConfigCoder} correctly encodes and decodes a {@link RegionConfig}
   * object containing region start and end keys which are populated with raw bytes.
   */
  @Test
  public void testEncodeDecode_populatedKeys() throws Exception {
    SnapshotConfig snapshotConfig = SnapshotTestHelper.newSnapshotConfig("my-restore-path");

    TableName tableName = TableName.valueOf("my-table");
    byte[] startKey = "start-key-abc".getBytes();
    byte[] endKey = "end-key-xyz".getBytes();
    RegionInfo regionInfo =
        RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey).build();
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).build();

    RegionConfig originalConfig =
        RegionConfig.builder()
            .setSnapshotConfig(snapshotConfig)
            .setRegionInfo(regionInfo)
            .setTableDescriptor(tableDescriptor)
            .setRegionSize(12345L)
            .build();

    RegionConfigCoder coder = new RegionConfigCoder();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    coder.encode(originalConfig, outStream);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    RegionConfig decodedConfig = coder.decode(inStream);

    assertEquals(originalConfig, decodedConfig);
    assertArrayEquals(startKey, decodedConfig.getRegionInfo().getStartKey());
    assertArrayEquals(endKey, decodedConfig.getRegionInfo().getEndKey());
  }
}
