/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflow.seqio;

import com.google.bigtable.repackaged.com.google.cloud.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.ShardNameTemplate;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumLongFn;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData.Builder;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.Reshuffle;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Dataflow job to export a Bigtable table to a set of SequenceFiles.
 * Afterwards, the files can be either imported into another Bigtable or HBase table.
 * You can limit the rows and columns exported using the options in {@link HBaseExportOptions}.
 * Please note that the pipeline processes the rows in chunks rather than rows, so the element counts in
 * dataflow ui will be misleading. The resulting files sequence files will contain sorted rows.
 *
 * Example usage:
 * </p>
 *
 * <pre>
 * {@code mvn compile exec:java \
 *    -Dexec.mainClass=HBaseExportJob \
 *    -Dexec.args="--runner=BlockingDataflowPipelineRunner \
 *    --project=igorbernstein-dev \
 *    --stagingLocation=gs://igorbernstein-dev/dataflow-staging \
 *    --zone=us-east1-c \
 *    --bigtableInstanceId=instance1 \
 *    --bigtableTableId=table2 \
 *    --destination=gs://igorbernstein-dev/export55 \
 *    --maxNumWorkers=200"
 * }
 * </pre>
 *
 * @author igorbernstein2
 * @version $Id: $Id
 */
@SuppressWarnings("serial")
public class HBaseExportJob {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseExportJob.class);

  public static void main(String[] args) throws IOException {
    PipelineOptionsFactory.register(HBaseExportOptions.class);

    if (args.length == 0 || "--help".equals(args[0])) {
      PipelineOptionsFactory.printHelp(System.out, HBaseExportOptions.class);
      return;
    }

    final HBaseExportOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(HBaseExportOptions.class);

    final Pipeline pipeline = buildPipeline(options);
    pipeline.run();
  }

  /**
   * Builds a pipeline using the passed in options.
   */
  public static Pipeline buildPipeline(HBaseExportOptions options) throws IOException {
    if (options.getDiskSizeGb() == 0) {
      options.setDiskSizeGb(20);
    }
    if (options.getBigtableProjectId() == null) {
      options.setBigtableProjectId(options.getProject());
    }

    // Configure Bigtable scan parameters
    final Scan scan = new Scan()
        .setStartRow(options.getStartRow().getBytes())
        .setStopRow(options.getStopRow().getBytes())
        .setMaxVersions(options.getMaxVersions());

    if (!options.getFilter().isEmpty()) {
      scan.setFilter(new ParseFilter().parseFilterString(options.getFilter()));
    }

    final CloudBigtableScanConfiguration scanConfig = CloudBigtableScanConfiguration.fromCBTOptions(options, scan)
        .toBuilder()
        .withConfiguration(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "HBaseExportJob")
        .build();

    return buildPipeline(options, scanConfig, options.getDestination());
  }

  public static Pipeline buildPipeline(DataflowPipelineOptions pipelineOptions,
      CloudBigtableScanConfiguration scanConfig, String destination) {

    final BoundedSource<Result> source = CloudBigtableIO.read(scanConfig);

    // Build the pipeline
    final Pipeline pipeline = Pipeline.create(pipelineOptions);

    // Coders
    final SerializableCoder<BoundedSource<Result>> sourceCoder = SerializableCoder.of(
        new TypeDescriptor<BoundedSource<Result>>() {
        }
    );

    // Pipeline
    pipeline
        // Initialize with the source as the only element.
        // Note that we aren't reading the source's elements yet
        .apply("Initialize", Create.of(source).withCoder(sourceCoder))
        // Split the source into a bunch of shards
        // Note that we aren't reading the source's elements yet
        .apply("Shard", ParDo.of(new ShardScans<Result>(scanConfig)))
        // Make sure that each shard can be handled a different worker
        .apply("Fanout", Reshuffle.<Integer, ExportShard<Result>>of())
        // Now, read the actual rows and write out each shard's elements into a file
        .apply("Write", ParDo.of(new WriteResultsToSeq(destination, scanConfig)));

    return pipeline;
  }

  /**
   * Split Sources into multiple shards. It treats the Sources themselves as elements. It will output
   * a KV of a random unique key and a sharded Source.
   */
  static class ShardScans<T> extends DoFn<BoundedSource<T>, KV<Integer, ExportShard<T>>> {

    private static final long DESIRED_SHARD_SIZE = 1024 * 1024 * 1024;
    final Aggregator<Long, Long> totalShardCount;
    private CloudBigtableScanConfiguration scanConfig;

    public ShardScans(CloudBigtableScanConfiguration scanConfig) {
      totalShardCount = createAggregator("totalShards", new SumLongFn());
      this.scanConfig = scanConfig;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      final BoundedSource<T> source = c.element();

      List<? extends BoundedSource<T>> splitSources = source.splitIntoBundles(
          DESIRED_SHARD_SIZE, c.getPipelineOptions()
      );
      totalShardCount.addValue((long) splitSources.size());
      LOG.info("Split into {} shards", splitSources.size());

      // Assign the filenames now so that the output is stable
      final List<ExportShard<T>> shards = new ArrayList<>(splitSources.size());
      int i = 0;
      for (BoundedSource<T> splitSource : splitSources) {
        // i++ produces a 0 based file name. While that's not ideal, 0 base is used in many other
        // places we investigated.
        final String filename = IOChannelUtils.constructName(
            "part", ShardNameTemplate.INDEX_OF_MAX, "", i++, splitSources.size()
        );
        shards.add(new ExportShard<>(splitSource, filename));
      }

      // randomize order to make sure that scans are distributed across tablets
      Collections.shuffle(shards);

      i = 0;
      for (ExportShard<T> shard : shards) {
        c.output(KV.of(i++, shard));
      }
    }

    @Override
    public void populateDisplayData(Builder builder) {
      // TODO Auto-generated method stub
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("instanceId", scanConfig.getInstanceId()));
      builder.add(DisplayData.item("tableId", scanConfig.getTableId()));
    }
  }

  /**
   * Reads each shard and writes its elements into a SequenceFile with compressed blocks.
   */
  static class WriteResultsToSeq extends DoFn<KV<Integer, ExportShard<Result>>, Void> {
    private final String basePath;
    private final CloudBigtableScanConfiguration scanConfig;
    final Aggregator<Long, Long> itemCounter;
    final Aggregator<Long, Long> bytesWritten;
    final Aggregator<Long, Long> shardsCompleted;
    final Aggregator<Long, Long> shardsInProgress;


    WriteResultsToSeq(String basePath, CloudBigtableScanConfiguration scanConfig) {
      this.basePath = basePath;
      this.scanConfig = scanConfig;

      itemCounter = createAggregator("itemsProcessed", new SumLongFn());
      bytesWritten = createAggregator("bytesWritten", new SumLongFn());
      shardsCompleted = createAggregator("shardsProcessed", new SumLongFn());
      shardsInProgress = createAggregator("shardsInProgress", new SumLongFn());
    }

    @Override
    public void populateDisplayData(Builder builder) {
      // TODO Auto-generated method stub
      super.populateDisplayData(builder);
      builder.add(
        DisplayData.item("instanceId", scanConfig.getInstanceId()));
      builder.add(DisplayData.item("tableId", scanConfig.getTableId()));
      builder.add(DisplayData.item("destination", basePath));
      builder.add(DisplayData.item("Read Row Request", scanConfig.getRequest().toString()));
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      final ExportShard<Result> shard = c.element().getValue();

      shardsInProgress.addValue(1l);

      final String projectId = c.getPipelineOptions().as(DataflowPipelineOptions.class).getProject();
      final String finalFilePath = basePath + "/" + shard.filename;
      LOG.info("starting " + shard.filename);

      try (BoundedSource.BoundedReader<Result> reader = shard.source.createReader(c.getPipelineOptions())) {
        if (!reader.start()) {
          // don't bother creating empty SequenceFile for empty shards
          return;
        }

        try (SequenceFile.Writer writer = createWriter(projectId, finalFilePath)) {
          final ImmutableBytesWritable key = new ImmutableBytesWritable();
          long bytesReported = 0;

          do {
            Result value = reader.getCurrent();
            key.set(value.getRow());
            writer.append(key, value);

            // update metrics
            itemCounter.addValue(1L);
            bytesReported = incrementBytesReported(writer, bytesReported);
          } while (reader.advance());
        }
      } finally {
        shardsCompleted.addValue(1l);
        shardsInProgress.addValue(-1l);
        LOG.info("finished " + shard.filename);
      }
    }

    private long incrementBytesReported(SequenceFile.Writer writer, long bytesReported)
        throws IOException {
      long fileSize = writer.getLength();
      bytesWritten.addValue(fileSize - bytesReported);
      return fileSize;
    }

    private SequenceFile.Writer createWriter(String project, String path) throws IOException {
      final Configuration hadoopConfig = new Configuration(false);

      // Not using setStrings to avoid loading StringUtils which tries to access HADOOP_HOME
      hadoopConfig.set("io.serializations", Joiner.on(',').join(
          WritableSerialization.class.getName(),
          ResultSerialization.class.getName())
      );
      hadoopConfig.set("fs.gs.project.id", project);
      for (Map.Entry<String, String> entry : HBaseImportIO.CONST_FILE_READER_PROPERTIES.entrySet()) {
        hadoopConfig.set(entry.getKey(), entry.getValue());
      }

      final SequenceFile.Writer.Option[] writerOptions = new SequenceFile.Writer.Option[]{
          SequenceFile.Writer.file(new Path(path)),
          SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
          SequenceFile.Writer.valueClass(Result.class),
          SequenceFile.Writer.compression(CompressionType.BLOCK)
      };

      return SequenceFile.createWriter(hadoopConfig, writerOptions);
    }
  }

  @DefaultCoder(SerializableCoder.class)
  static class ExportShard<T> implements Serializable {
    final BoundedSource<T> source;
    final String filename;

    public ExportShard(BoundedSource<T> source, String filename) {
      this.source = source;
      this.filename = filename;
    }
  }
}
