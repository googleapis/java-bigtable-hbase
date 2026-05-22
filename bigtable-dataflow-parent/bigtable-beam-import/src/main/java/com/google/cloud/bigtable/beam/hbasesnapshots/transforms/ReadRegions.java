/*
 * Copyright 2024 Google LLC
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
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} for reading the records from each region and creates Hbase {@link Mutation}
 * instances. Each region will be split into configured size (512 MB) and pipeline option {@link
 * com.google.cloud.bigtable.beam.hbasesnapshots.ImportJobFromHbaseSnapshot.ImportOptions#getUseDynamicSplitting()
 * useDynamicSplitting} can be used to control whether each split needs to be subdivided further or
 * not.
 */
@InternalApi("For internal usage only")
public class ReadRegions
    extends PTransform<PCollection<RegionConfig>, PCollection<KV<String, Iterable<Mutation>>>> {

  private static final long BYTES_PER_SPLIT = 512 * 1024 * 1024; // 512 MB

  private static final long BYTES_PER_GB = 1024 * 1024 * 1024;

  private final boolean useDynamicSplitting;

  public ReadRegions(boolean useDynamicSplitting) {
    this.useDynamicSplitting = useDynamicSplitting;
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

    return regionConfig
        .apply("Read snapshot region", ParDo.of(new ReadRegionFn(this.useDynamicSplitting)))
        .setCoder(KvCoder.of(snapshotConfigSchemaCoder, hbaseResultCoder))
        .apply("Create Mutation", ParDo.of(new CreateMutationsFn()));
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
      try (ResultScanner scanner = newScanner(regionConfig, tracker.currentRestriction())) {
        for (Result result : scanner) {
          if (tracker.tryClaim(ByteKey.copyFrom(result.getRow()))) {
            outputReceiver.output(KV.of(regionConfig.getSnapshotConfig(), result));
          } else {
            hasSplit = true;
            break;
          }
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
    private ResultScanner newScanner(RegionConfig regionConfig, ByteKeyRange byteKeyRange)
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

      return new ClientSideRegionScanner(
          configuration,
          fileSystem,
          restorePath,
          regionConfig.getTableDescriptor(),
          regionConfig.getRegionInfo(),
          scan,
          null);
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
            "Splitting Initial Restriction for SnapshotName: {} - regionname:{} - regionsize(GB):{} - Splits: {}",
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
    private static final int MAX_MUTATIONS_PER_REQUEST = 999_999;

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

      int cellCount = 0;

      Put put = null;
      for (Cell cell : element.getValue().listCells()) {
        // Split the row into multiple mutations if mutations exceeds threshold limit
        if (cellCount % MAX_MUTATIONS_PER_REQUEST == 0) {
          cellCount = 0;
          put = new Put(rowKey);
          mutations.add(put);
        }
        put.add(cell);
        cellCount++;
      }

      outputReceiver.output(KV.of(targetTable, mutations));
    }
  }
}
