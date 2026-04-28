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

// import com.google.cloud.bigtable.beam.hbasesnapshots.ImportSnapshots;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.wal.WAL;
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

  private static final long BYTES_PER_SPLIT = 512 * 1024 * 1024; // 512 MB

  private static final long BYTES_PER_GB = 1024 * 1024 * 1024;

  private final boolean useDynamicSplitting;

  private final int maxMutationsPerRequestThreshold;

  private final boolean filterLargeRows;
  private final long filterLargeRowThresholdBytes;

  private final boolean filterLargeCells;
  private final int filterLargeCellThresholdBytes;

  private final boolean filterLargeRowKeys;
  private final int filterLargeRowKeysThresholdBytes;

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

    if (numShards != null && shardIndex == null) {
      throw new IllegalArgumentException("if numShards is set, shardIndex must also be");
    }
    if (numShards != null && (shardIndex >= numShards || shardIndex < 0)) {
      throw new IllegalArgumentException("shardIndex must be between [0, numShards)");
    }

    this.numShards = numShards;
    this.shardIndex = shardIndex;
  }

  @Override
  public PCollection<KV<String, Iterable<Mutation>>> expand(
      PCollection<RegionConfig> regionConfig) {
    Pipeline pipeline = regionConfig.getPipeline();
    SchemaCoder<SnapshotConfig> snapshotConfigSchemaCoder;
    Coder<Result> hbaseResultCoder;
    try {
      snapshotConfigSchemaCoder = pipeline.getSchemaRegistry().getSchemaCoder(SnapshotConfig.class);
      hbaseResultCoder = pipeline.getCoderRegistry().getCoder(TypeDescriptor.of(Result.class));
    } catch (CannotProvideCoderException | NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }

    PCollection<RegionConfig> maybeShardedRegions = regionConfig;
    if (numShards != null) {
      maybeShardedRegions =
          regionConfig.apply(
              "Select regions for shard",
              Filter.by(
                  rc -> {
                    // encodedName is an MD5 hash of the region info and therefor should be well
                    // distributed
                    byte[] regionName = rc.getRegionInfo().getEncodedNameAsBytes();
                    long remainder =
                        new BigInteger(regionName).mod(BigInteger.valueOf(numShards)).longValue();
                    boolean shouldTake = remainder == shardIndex;
                    ReadRegionFn.LOG.info(
                        "Region {} was {} due to sharding",
                        rc.getRegionInfo().getRegionNameAsString(),
                        shouldTake ? "taken" : "skipped");
                    return shouldTake;
                  }));
    }

    return maybeShardedRegions
        .apply("Read snapshot region", ParDo.of(new ReadRegionFn(this.useDynamicSplitting)))
        .setCoder(KvCoder.of(snapshotConfigSchemaCoder, hbaseResultCoder))
        .apply(
            "Create Mutation",
            ParDo.of(
                new CreateMutationsFn(
                    this.maxMutationsPerRequestThreshold,
                    this.filterLargeRows,
                    this.filterLargeRowThresholdBytes,
                    this.filterLargeCells,
                    this.filterLargeCellThresholdBytes,
                    this.filterLargeRowKeys,
                    this.filterLargeRowKeysThresholdBytes)));
  }

  static class ReadRegionFn extends DoFn<RegionConfig, KV<SnapshotConfig, Result>> {
    private static final Logger LOG = LoggerFactory.getLogger(ReadRegionFn.class);

    private final boolean useDynamicSplitting;

    public ReadRegionFn(boolean useDynamicSplitting) {
      this.useDynamicSplitting = useDynamicSplitting;
    }

    @ProcessElement
    public void processElement(
        @Element RegionConfig regionConfig,
        OutputReceiver<KV<SnapshotConfig, Result>> outputReceiver,
        RestrictionTracker<ByteKeyRange, ByteKey> tracker)
        throws Exception {

      boolean hasSplit = false;
      try (HBaseRegionScanner scanner = newScanner(regionConfig, tracker.currentRestriction())) {
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
          //  if (flag==0 ) {
          if (tracker.tryClaim(ByteKey.copyFrom(result.getRow()))) {
            outputReceiver.output(KV.of(regionConfig.getSnapshotConfig(), result));
          } else {
            hasSplit = true;
            break;
          }
          // }
        }
      }
      // if (!hasSplit)
      tracker.tryClaim(ByteKey.EMPTY);
    }

    /**
     * Scans each region for given key range and constructs a ClientSideRegionScanner instance
     *
     * @param regionConfig - HBase Region Configuration
     * @param byteKeyRange - Key range covering start and end row key
     * @return
     * @throws Exception
     */
    private HBaseRegionScanner newScanner(RegionConfig regionConfig, ByteKeyRange byteKeyRange)
        throws Exception {
      Scan scan =
          new Scan()
              // Limit scan to split range
              .withStartRow(byteKeyRange.getStartKey().getBytes())
              .withStopRow(byteKeyRange.getEndKey().getBytes())
              .setIsolationLevel(IsolationLevel.READ_UNCOMMITTED)
              .setCacheBlocks(false);

      SnapshotConfig snapshotConfig = regionConfig.getSnapshotConfig();

      Path sourcePath = snapshotConfig.getSourcePath();
      Path restorePath = snapshotConfig.getRestorePath();
      Configuration configuration = snapshotConfig.getConfiguration();
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
      return ByteKeyRange.of(
          ByteKey.copyFrom(regionConfig.getRegionInfo().getStartKey()),
          ByteKey.copyFrom(regionConfig.getRegionInfo().getEndKey()));
    }

    @GetSize
    public double getSize(@Element RegionConfig regionConfig) {
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
          byte[][] splits =
              uniformSplit.split(
                  range.getStartKey().getBytes(),
                  range.getEndKey().getBytes(),
                  getSplits(regionConfig.getRegionSize()),
                  true);
          for (int i = 0; i < splits.length - 1; i++)
            outputReceiver.output(
                ByteKeyRange.of(ByteKey.copyFrom(splits[i]), ByteKey.copyFrom(splits[i + 1])));
        } else {
          outputReceiver.output(range);
        }
      } catch (Exception ex) {
        LOG.warn(
            "Unable to split range for region:{} in Snapshot:{}",
            regionConfig.getRegionInfo().getEncodedName(),
            regionConfig.getSnapshotConfig().getSnapshotName());
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

  /**
   * A {@link DoFn} class for converting Hbase {@link org.apache.hadoop.hbase.client.Result} to list
   * of Hbase {@link org.apache.hadoop.hbase.client.Mutation}s
   */
  static class CreateMutationsFn
      extends DoFn<KV<SnapshotConfig, Result>, KV<String, Iterable<Mutation>>> {
    private static final Logger LOG = LoggerFactory.getLogger(CreateMutationsFn.class);
    private final int maxMutationsPerRequestThreshold;

    private final boolean filterLargeRows;

    private final long filterLargeRowThresholdBytes;

    private final boolean filterLargeCells;

    private final int filterLargeCellThresholdBytes;

    private final boolean filterLargeRowKeys;

    private final int filterLargeRowKeysThresholdBytes;

    public CreateMutationsFn(
        int maxMutationsPerRequestThreshold,
        boolean filterLargeRows,
        long filterLargeRowThresholdBytes,
        boolean filterLargeCells,
        int filterLargeCellThresholdBytes,
        boolean filterLargeRowKeys,
        int filterLargeRowKeysThresholdBytes) {

      this.maxMutationsPerRequestThreshold = maxMutationsPerRequestThreshold;

      this.filterLargeRows = filterLargeRows;
      this.filterLargeRowThresholdBytes = filterLargeRowThresholdBytes;

      this.filterLargeCells = filterLargeCells;
      this.filterLargeCellThresholdBytes = filterLargeCellThresholdBytes;

      this.filterLargeRowKeys = filterLargeRowKeys;
      this.filterLargeRowKeysThresholdBytes = filterLargeRowKeysThresholdBytes;
    }

    @ProcessElement
    public void processElement(
        @Element KV<SnapshotConfig, Result> element,
        OutputReceiver<KV<String, Iterable<Mutation>>> outputReceiver)
        throws IOException {
      if (element.getValue().listCells().isEmpty()) return;
      String targetTable = element.getKey().getTableName();

      // Limit the number of mutations per Put (server will reject >= 100k mutations per Put)
      byte[] rowKey = element.getValue().getRow();
      List<Mutation> mutations = new ArrayList<>();

      boolean logAndSkipIncompatibleRowMutations =
          verifyRowMutationThresholds(rowKey, element.getValue().listCells(), mutations);

      if (!logAndSkipIncompatibleRowMutations && mutations.size() > 0) {
        outputReceiver.output(KV.of(targetTable, mutations));
      }
    }

    private boolean verifyRowMutationThresholds(
        byte[] rowKey, List<Cell> cells, List<Mutation> mutations) throws IOException {
      boolean logAndSkipIncompatibleRows = false;

      Put put = null;
      int cellCount = 0;
      long totalByteSize = 0L;

      // create mutations
      for (Cell cell : cells) {
        totalByteSize += cell.heapSize();

        // handle large cells
        if (filterLargeCells && cell.getValueLength() > filterLargeCellThresholdBytes) {
          // TODO add config name in log
          LOG.warn(
              "Dropping mutation, cell value length, "
                  + cell.getValueLength()
                  + ", exceeds filter length, "
                  + filterLargeCellThresholdBytes
                  + ", cell: "
                  + cell
                  + ", row key: "
                  + Bytes.toStringBinary(rowKey));
          continue;
        }

        // Split the row into multiple mutations if mutations exceeds threshold limit
        if (cellCount % maxMutationsPerRequestThreshold == 0) {
          cellCount = 0;
          put = new Put(rowKey);
          mutations.add(put);
        }
        put.add(cell);
        cellCount++;
      }

      // TODO add config name in log
      if (filterLargeRows && totalByteSize > filterLargeRowThresholdBytes) {
        logAndSkipIncompatibleRows = true;
        LOG.warn(
            "Dropping row, row length, "
                + totalByteSize
                + ", exceeds filter length threshold, "
                + filterLargeRowThresholdBytes
                + ", row key: "
                + Bytes.toStringBinary(rowKey));
      }

      // TODO add config name in log
      if (filterLargeRowKeys && rowKey.length > filterLargeRowKeysThresholdBytes) {
        logAndSkipIncompatibleRows = true;
        LOG.warn(
            "Dropping row, row key length, "
                + rowKey.length
                + ", exceeds filter length threshold, "
                + filterLargeRowKeysThresholdBytes
                + ", row key: "
                + Bytes.toStringBinary(rowKey));
      }

      return logAndSkipIncompatibleRows;
    }
  }

  /**
   * A workalike for {@link org.apache.hadoop.hbase.client.ClientSideRegionScanner}.
   *
   * <p>It serves the same purpose, but skips block and mobFile cache initialization. Those caches
   * dont appear to useful for the import job and leak threads on shutdown
   */
  static class HBaseRegionScanner implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseRegionScanner.class);

    private HRegion region;
    private RegionScanner scanner;
    private final List<Cell> values;
    boolean hasMore = true;

    public HBaseRegionScanner(
        Configuration conf,
        FileSystem fs,
        Path rootDir,
        TableDescriptor htd,
        RegionInfo hri,
        Scan scan)
        throws IOException {
      scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
      htd = TableDescriptorBuilder.newBuilder(htd).setReadOnly(true).build();
      this.region =
          HRegion.newHRegion(
              CommonFSUtils.getTableDir(rootDir, htd.getTableName()),
              (WAL) null,
              fs,
              conf,
              hri,
              htd,
              (RegionServerServices) null);
      this.region.setRestoredRegion(true);
      conf.set("hfile.block.cache.policy", "IndexOnlyLRU");
      conf.setIfUnset("hfile.onheap.block.cache.fixed.size", String.valueOf(33554432L));
      conf.unset("hbase.bucketcache.ioengine");

      this.region.initialize();
      this.scanner = this.region.getScanner(scan);
      this.values = new ArrayList();

      this.region.startRegionOperation();
    }

    public void close() {
      if (this.scanner != null) {
        try {
          this.scanner.close();
          this.scanner = null;
        } catch (IOException var3) {
          LOG.warn("Exception while closing scanner", var3);
        }
      }

      if (this.region != null) {
        try {
          this.region.closeRegionOperation();
          this.region.close(true);
          this.region = null;
        } catch (IOException var2) {
          LOG.warn("Exception while closing region", var2);
        }
      }
    }

    public Result next() throws IOException {
      do {
        if (!this.hasMore) {
          return null;
        }

        this.values.clear();
        this.hasMore = this.scanner.nextRaw(this.values);
      } while (this.values.isEmpty());

      Result result = Result.create(this.values);

      return result;
    }
  }
}
