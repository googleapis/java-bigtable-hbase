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
package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.SnapshotUtils;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotKey;
import com.google.cloud.bigtable.beam.hbasesnapshots.transforms.HbaseRegionSplitTracker;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Splittable {@link DoFn} for reading the records from each region. */
@InternalApi("For internal usage only")
public class ReadSnapshotRegionFn extends DoFn<RegionConfig, KV<SnapshotKey, Result>> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadSnapshotRegionFn.class);

  private static final long BYTES_PER_SPLIT = 512 * 1024 * 1024; // 512 MB
  private static final long BYTES_PER_GB = 1024 * 1024 * 1024;

  private final boolean useDynamicSplitting;
  private transient Map<Map<String, String>, Configuration> configCache;

  public ReadSnapshotRegionFn(boolean useDynamicSplitting) {
    this.useDynamicSplitting = useDynamicSplitting;
  }

  // Beam reuses DoFn instances for multiple elements. Initializing the cache in @Setup
  // ensures that we only create the cache once per DoFn instance lifecycle (per worker thread),
  // avoiding heavy XML parsing overhead for Configuration while also avoiding static state
  // and ensuring thread safety since Beam isolates DoFn instances.
  @Setup
  public void setup() {
    configCache = new HashMap<>();
  }

  @ProcessElement
  public void processElement(
      @Element RegionConfig regionConfig,
      OutputReceiver<KV<SnapshotKey, Result>> outputReceiver,
      RestrictionTracker<ByteKeyRange, ByteKey> tracker)
      throws Exception {

    Map<String, String> configDetails = regionConfig.getSnapshotConfig().getConfigurationDetails();
    Configuration configuration =
        configCache.computeIfAbsent(configDetails, SnapshotUtils::getHBaseConfiguration);

    SnapshotConfig sc = regionConfig.getSnapshotConfig();
    SnapshotKey snapshotKey = SnapshotKey.create(sc.getSnapshotName(), sc.getTableName());

    // Use try-with-resources to ensure scanner is closed and resources are released.
    try (HBaseRegionScanner scanner =
        newScanner(regionConfig, tracker.currentRestriction(), configuration)) {
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        if (tracker.tryClaim(ByteKey.copyFrom(result.getRow()))) {
          outputReceiver.output(KV.of(snapshotKey, result));
        } else {
          // According to Splittable DoFn contract, when tryClaim returns false,
          // we must terminate processing immediately and return.
          return;
        }
      }
      // Signal completion of the range. ByteKeyRangeTracker uses EMPTY to mark the end of the
      // range.
      // See:
      // https://github.com/apache/beam/blob/2c4d2c6de4dca6b5954c529c2d40c031a8b74f60/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/splittabledofn/ByteKeyRangeTracker.java#L37
      tracker.tryClaim(ByteKey.EMPTY);
    }
  }

  HBaseRegionScanner newScanner(
      RegionConfig regionConfig, ByteKeyRange byteKeyRange, Configuration configuration)
      throws Exception {
    // Create an HBase Scan bounded by the restriction's start and end keys.
    // Use READ_UNCOMMITTED for performance since snapshots are read-only.
    // Disable block caching as we are doing a full scan and reuse is unlikely.
    Scan scan =
        new Scan()
            .withStartRow(byteKeyRange.getStartKey().getBytes())
            .withStopRow(byteKeyRange.getEndKey().getBytes())
            .setIsolationLevel(IsolationLevel.READ_UNCOMMITTED)
            .setCacheBlocks(false);

    SnapshotConfig snapshotConfig = regionConfig.getSnapshotConfig();

    Path sourcePath = snapshotConfig.getSourcePath();
    Path restorePath = snapshotConfig.getRestorePath();
    FileSystem fileSystem = sourcePath.getFileSystem(configuration);

    return new HBaseRegionScanner(
        configuration,
        fileSystem,
        restorePath,
        regionConfig.getTableDescriptor(),
        regionConfig.getRegionInfo(),
        scan);
  }

  @GetInitialRestriction
  public ByteKeyRange getInitialRange(@Element RegionConfig regionConfig) {
    // The initial restriction is the full key range of the HBase region.
    return ByteKeyRange.of(
        ByteKey.copyFrom(regionConfig.getRegionInfo().getStartKey()),
        ByteKey.copyFrom(regionConfig.getRegionInfo().getEndKey()));
  }

  @GetSize
  public double getSize(@Element RegionConfig regionConfig) {
    // Return the default split size as the work estimate for this restriction.
    return BYTES_PER_SPLIT;
  }

  @NewTracker
  public HbaseRegionSplitTracker newTracker(
      @Element RegionConfig regionConfig, @Restriction ByteKeyRange range) {
    return new HbaseRegionSplitTracker(
        regionConfig.getSnapshotConfig().getSnapshotName(),
        regionConfig.getRegionInfo().getEncodedName(),
        range,
        useDynamicSplitting);
  }

  @SplitRestriction
  public void splitRestriction(
      @Element RegionConfig regionConfig,
      @Restriction ByteKeyRange range,
      OutputReceiver<ByteKeyRange> outputReceiver) {
    byte[] originalEndKey = regionConfig.getRegionInfo().getEndKey();
    if (originalEndKey == null || originalEndKey.length == 0) {
      LOG.info(
          "Skipping splitting for boundary region: {}",
          regionConfig.getRegionInfo().getEncodedName());
      outputReceiver.output(range);
      return;
    }

    try {
      int numSplits = getSplits(regionConfig.getRegionSize());
      LOG.info(
          "Splitting Initial Restriction for SnapshotName: {} - regionname:{} - regionsize(GB):{}"
              + " - Splits: {}",
          regionConfig.getSnapshotConfig().getSnapshotName(),
          regionConfig.getRegionInfo().getEncodedName(),
          (double) regionConfig.getRegionSize() / BYTES_PER_GB,
          numSplits);
      if (numSplits > 1) {
        RegionSplitter.UniformSplit uniformSplit = new RegionSplitter.UniformSplit();
        byte[] startKey = range.getStartKey().getBytes();
        byte[] endKey = range.getEndKey().getBytes();

        // Handle empty start key if it's the absolute first region
        if (startKey.length == 0) {
          startKey = new byte[endKey.length];
        }

        byte[][] splits = uniformSplit.split(startKey, endKey, numSplits, true);

        // Preserve the absolute start boundary if necessary
        if (range.getStartKey().isEmpty()) {
          splits[0] = new byte[0];
        }

        for (int i = 0; i < splits.length - 1; i++) {
          outputReceiver.output(
              ByteKeyRange.of(ByteKey.copyFrom(splits[i]), ByteKey.copyFrom(splits[i + 1])));
        }
      } else {
        outputReceiver.output(range);
      }
    } catch (Exception ex) {
      LOG.warn(
          "Unable to split range for region:{} in Snapshot:{}",
          regionConfig.getRegionInfo().getEncodedName(),
          regionConfig.getSnapshotConfig().getSnapshotName(),
          ex);
      outputReceiver.output(range);
    }
  }

  private int getSplits(long sizeInBytes) {
    return (int) Math.ceil((double) sizeInBytes / BYTES_PER_SPLIT);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(
        DisplayData.item("DynamicSplitting", useDynamicSplitting ? "Enabled" : "Disabled")
            .withLabel("Dynamic Splitting"));
  }
}
