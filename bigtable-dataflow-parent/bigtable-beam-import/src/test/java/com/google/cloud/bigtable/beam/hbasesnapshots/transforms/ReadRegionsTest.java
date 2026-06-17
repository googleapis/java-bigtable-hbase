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
package com.google.cloud.bigtable.beam.hbasesnapshots.transforms;

import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link ReadRegions} transform. */
@RunWith(JUnit4.class)
public class ReadRegionsTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Tests that constructor throws exception when numShards is set but shardIndex is missing. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidShardConfig_missingShardIndex() {
    new ReadRegions(
        true,
        100,
        false,
        0L,
        false,
        0,
        false,
        0,
        false, // filterWideRows
        2, // numShards
        null // shardIndex missing
        );
  }

  /** Tests that constructor throws exception when shardIndex is out of bounds. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidShardConfig_shardIndexTooHigh() {
    new ReadRegions(true, 100, false, 0L, false, 0, false, 0, false, 2, 2);
  }

  /** Tests the basic sharding logic math. */
  @Test
  public void testShardingLogic() {
    // Verify the logic used for sharding
    byte[] regionName = "someEncodedRegionName".getBytes();
    int numShards = 4;

    long remainder = new BigInteger(regionName).mod(BigInteger.valueOf(numShards)).longValue();
    assertTrue(remainder >= 0 && remainder < numShards);
  }

  /**
   * Tests that the sharding logic correctly selects or skips regions based on their encoded name,
   * numShards, and shardIndex using TestPipeline.
   */
  @Test
  public void testShardingWithPipeline() {
    // 1. Setup configuration and table descriptor
    SnapshotConfig snapshotConfig =
        SnapshotConfig.builder()
            .setProjectId("project")
            .setSourceLocation("source")
            .setSnapshotName("snapshot")
            .setTableName("table")
            .setRestoreLocation("restore")
            .setConfigurationDetails(Collections.emptyMap())
            .build();

    TableDescriptor td =
        TableDescriptorBuilder.newBuilder(TableName.valueOf("table"))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes()).build())
            .build();

    // 2. Create test regions
    RegionInfo ri1 =
        RegionInfoBuilder.newBuilder(TableName.valueOf("table")).setRegionId(1).build();

    RegionConfig rc1 =
        RegionConfig.builder()
            .setSnapshotConfig(snapshotConfig)
            .setRegionInfo(ri1)
            .setTableDescriptor(td)
            .setRegionSize(100L)
            .setName("region1")
            .build();

    RegionInfo ri2 =
        RegionInfoBuilder.newBuilder(TableName.valueOf("table")).setRegionId(2).build();

    RegionConfig rc2 =
        RegionConfig.builder()
            .setSnapshotConfig(snapshotConfig)
            .setRegionInfo(ri2)
            .setTableDescriptor(td)
            .setRegionSize(100L)
            .setName("region2")
            .build();

    // 3. Calculate expected shards for the regions to determine expected output
    int shard1 =
        new BigInteger(1, ri1.getEncodedNameAsBytes()).mod(BigInteger.valueOf(4)).intValue();
    int shard2 =
        new BigInteger(1, ri2.getEncodedNameAsBytes()).mod(BigInteger.valueOf(4)).intValue();

    int targetShard = shard1;

    // 4. Apply the sharding filter in a pipeline
    org.apache.beam.sdk.values.PCollection<RegionConfig> input =
        pipeline.apply(
            Create.of(rc1, rc2)
                .withCoder(
                    new com.google.cloud.bigtable.beam.hbasesnapshots.coders.RegionConfigCoder()));

    org.apache.beam.sdk.values.PCollection<RegionConfig> output =
        input.apply(Filter.by(rc -> ReadRegions.isRegionSelected(rc, 4, targetShard)));

    // 5. Verify that only regions belonging to the target shard are selected
    List<RegionConfig> expected = new ArrayList<>();
    expected.add(rc1);
    if (shard2 == targetShard) {
      expected.add(rc2);
    }

    PAssert.that(output).containsInAnyOrder(expected);

    // PAssert is added as a transform to the pipeline graph. We must call pipeline.run()
    // to actually execute the pipeline and evaluate the assertions.
    pipeline.run();
  }
}
