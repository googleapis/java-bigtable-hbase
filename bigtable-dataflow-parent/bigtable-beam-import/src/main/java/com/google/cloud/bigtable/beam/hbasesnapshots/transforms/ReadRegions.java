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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotKey;
import com.google.cloud.bigtable.beam.hbasesnapshots.dofn.CreateBigtableMutationsFn;
import com.google.cloud.bigtable.beam.hbasesnapshots.dofn.ReadSnapshotRegionFn;
import com.google.common.base.Preconditions;
import java.math.BigInteger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} for reading the records from each region and creates Hbase {@link Mutation}
 * instances. Each region will be split into configured size (512 MB) and pipeline option {@link
 * ImportSnapshots.ImportSnapshotsOptions#getUseDynamicSplitting() useDynamicSplitting} can be used
 * to control whether each split needs to be subdivided further or not.
 */
@InternalApi("For internal usage only")
public class ReadRegions
    extends PTransform<PCollection<RegionConfig>, PCollection<KV<String, Iterable<Mutation>>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadRegions.class);

  private final boolean useDynamicSplitting;

  private final int maxMutationsPerRequestThreshold;

  private final boolean filterLargeRows;
  private final long filterLargeRowThresholdBytes;

  private final boolean filterLargeCells;
  private final int filterLargeCellThresholdBytes;

  private final boolean filterLargeRowKeys;
  private final int filterLargeRowKeysThresholdBytes;
  private final boolean filterWideRows;

  private final Integer numShards;
  private final Integer shardIndex;

  public ReadRegions(
      boolean useDynamicSplitting,
      int maxMutationsPerRequestThreshold,
      boolean filterLargeRows,
      long filterLargeRowThresholdBytes,
      boolean filterLargeCells,
      int filterLargeCellThresholdBytes,
      boolean filterLargeRowKeys,
      int filterLargeRowKeysThresholdBytes,
      boolean filterWideRows,
      Integer numShards,
      Integer shardIndex) {
    this.useDynamicSplitting = useDynamicSplitting;
    this.maxMutationsPerRequestThreshold = maxMutationsPerRequestThreshold;

    this.filterLargeRows = filterLargeRows;
    this.filterLargeRowThresholdBytes = filterLargeRowThresholdBytes;

    this.filterLargeCells = filterLargeCells;
    this.filterLargeCellThresholdBytes = filterLargeCellThresholdBytes;

    this.filterLargeRowKeys = filterLargeRowKeys;
    this.filterLargeRowKeysThresholdBytes = filterLargeRowKeysThresholdBytes;
    this.filterWideRows = filterWideRows;

    if (numShards != null) {
      Preconditions.checkArgument(
          shardIndex != null, "if numShards is set, shardIndex must also be");
      Preconditions.checkArgument(
          shardIndex >= 0 && shardIndex < numShards, "shardIndex must be between [0, numShards)");
    }

    this.numShards = numShards;
    this.shardIndex = shardIndex;
  }

  static boolean isRegionSelected(RegionConfig rc, int numShards, int shardIndex) {
    byte[] regionName = rc.getRegionInfo().getEncodedNameAsBytes();
    // Use signum=1 to force the byte array to be interpreted as a positive magnitude,
    // avoiding negative numbers and potential sharding skews.
    long remainder = new BigInteger(1, regionName).mod(BigInteger.valueOf(numShards)).longValue();
    return remainder == shardIndex;
  }

  @Override
  public PCollection<KV<String, Iterable<Mutation>>> expand(
      PCollection<RegionConfig> regionConfig) {
    // 1. Resolve coders for the output KV type.
    Pipeline pipeline = regionConfig.getPipeline();
    SchemaCoder<SnapshotKey> snapshotKeySchemaCoder;
    Coder<Result> hbaseResultCoder;
    try {
      snapshotKeySchemaCoder = pipeline.getSchemaRegistry().getSchemaCoder(SnapshotKey.class);
      hbaseResultCoder = pipeline.getCoderRegistry().getCoder(TypeDescriptor.of(Result.class));
    } catch (CannotProvideCoderException | NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }

    // 2. Apply sharding if enabled. Encoded name is an MD5 hash, so it provides
    // good distribution across shards.
    PCollection<RegionConfig> maybeShardedRegions = regionConfig;
    if (numShards != null) {
      maybeShardedRegions =
          regionConfig.apply(
              "Select regions for shard",
              Filter.by(
                  rc -> {
                    boolean shouldTake = isRegionSelected(rc, numShards, shardIndex);
                    LOG.info(
                        "Region {} was {} due to sharding",
                        rc.getRegionInfo().getRegionNameAsString(),
                        shouldTake ? "taken" : "skipped");
                    return shouldTake;
                  }));
    }

    return maybeShardedRegions
        .apply("Read snapshot region", ParDo.of(new ReadSnapshotRegionFn(this.useDynamicSplitting)))
        .setCoder(KvCoder.of(snapshotKeySchemaCoder, hbaseResultCoder))
        .apply(
            "Create Mutation",
            ParDo.of(
                new CreateBigtableMutationsFn(
                    this.maxMutationsPerRequestThreshold,
                    this.filterLargeRows,
                    this.filterLargeRowThresholdBytes,
                    this.filterLargeCells,
                    this.filterLargeCellThresholdBytes,
                    this.filterLargeRowKeys,
                    this.filterLargeRowKeysThresholdBytes,
                    this.filterWideRows)));
  }
}
