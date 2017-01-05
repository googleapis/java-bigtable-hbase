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
package com.google.cloud.bigtable.dataflowimport;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumLongFn;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.util.Reshuffle;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
 *  Dataflow job to export a Bigtable table to a set of SequenceFiles.
 *  Afterwards, the files can be either imported into another Bigtable or HBase table.
 *  You can limit the rows and columns exported using the options in {@link HBaseExportOptions}.
 *  Please note that the pipeline processes the rows in chunks rather than rows, so the element counts in
 *  dataflow ui will be misleading.
 *
 *  Example usage:
 * </p>
 *
 * <pre>
 * {@code mvn compile exec:java \
 *    -Dexec.mainClass=com.google.cloud.bigtable.dataflowimport.HBaseExportJob \
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
 */
public class HBaseExportJob {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseExportJob.class);

  public static void main(String[] args) throws IOException {
    PipelineOptionsFactory.register(HBaseExportOptions.class);

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
    if(options.getDiskSizeGb() == 0) {
      options.setDiskSizeGb(20);
    }
    if(options.getBigtableProjectId() == null) {
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

    final String bigtableProject = Objects.firstNonNull(options.getBigtableProjectId(), options.getProject());

    final CloudBigtableScanConfiguration scanConfig = new CloudBigtableScanConfiguration.Builder()
        .withProjectId(bigtableProject)
        .withInstanceId(options.getBigtableInstanceId())
        .withTableId(options.getBigtableTableId())
        .withScan(scan)
        .build();

    final BoundedSource<Result> source = CloudBigtableIO.read(scanConfig);


    // Build the pipeline
    final Pipeline pipeline = Pipeline.create(options);

    // Coders
    final SerializableCoder<BoundedSource<Result>> sourceCoder = SerializableCoder.of(new TypeDescriptor<BoundedSource<Result>>() {});
    final KvCoder<String, BoundedSource<Result>> kvCoder = KvCoder.of(StringUtf8Coder.of(), sourceCoder);

    // Pipeline
    pipeline
        // Initialize with the source as the only element.
        // Note that we aren't reading the source's elements yet
        .apply("Initialize", Create.of(source).withCoder(sourceCoder))
        // Split the source into a bunch of shards
        // Note that we aren't reading the source's elements yet
        .apply("Shard", ParDo.of(new ShardScans<Result>())).setCoder(kvCoder)
        // Make sure that each shard can be handled a different worker
        .apply("Fanout", Reshuffle.<String, BoundedSource<Result>>of())
        // Now, read the actual rows and write out each shard's elements into a file
        .apply("Write", ParDo.of(new WriteResultsToSeq(options.getDestination())));

    return pipeline;
  }

  /**
   * Split Sources into multiple shards. It treats the Sources themselves as elements. It will output
   * a KV of a random unique key and a sharded Source. It will intentionally randomize the order of shards
   * to avoid hotspotting later on.
   */
  static class ShardScans<T> extends DoFn<BoundedSource<T>, KV<String, BoundedSource<T>>> {
    private static final long DESIRED_SHARD_SIZE = 1024 * 1024 * 1024;

    @Override
    public void processElement(ProcessContext c) throws Exception {
      final BoundedSource<T> source = c.element();

      final List<? extends BoundedSource<T>> boundedSources = source
          .splitIntoBundles(DESIRED_SHARD_SIZE, c.getPipelineOptions());

      LOG.info("Split into {} shards", boundedSources.size());

      // randomize order to make sure that scans are distributed across tablets
      Collections.shuffle(boundedSources);

      // emit each shard with a random uuid, note that uuids are used instead of counters to prevent misleading the
      // user into thinking that the filenames are ordered in any way
      for (BoundedSource<T> sourceBundle : boundedSources) {
        c.output(KV.of(UUID.randomUUID().toString(), sourceBundle));
      }
    }
  }

  /**
   * Reads each shard and writes its elements into a SequenceFile with compressed blocks.
   */
  static class WriteResultsToSeq extends DoFn<KV<String, BoundedSource<Result>>, Void> {
    private final String basePath;
    private final Aggregator<Long, Long> itemCounter;
    private final BatchCounter readDuration;
    private final BatchCounter writeDuration;
    private final BatchCounter totalDuration;
    private final BatchCounter bytesWritten;

    private transient Configuration hadoopConfig;

    WriteResultsToSeq(String basePath) {
      this.basePath = basePath;

      itemCounter = createAggregator("itemsProcessed", new SumLongFn());
      readDuration = new BatchCounter(createAggregator("readDuration(secs)", new SumLongFn()), 1_000_000_000);
      writeDuration = new BatchCounter(createAggregator("writeDuration(secs)", new SumLongFn()), 1_000_000_000);
      totalDuration = new BatchCounter(createAggregator("totalDuration(secs)", new SumLongFn()), 1_000_000_000);
      bytesWritten = new BatchCounter(createAggregator("bytesWritten(MB)", new SumLongFn()), 1024 * 1024);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
    }

    @Override
    public void startBundle(Context c) throws Exception {
      hadoopConfig = new Configuration(false);

      // Not using setStrings to avoid loading StringUtils which tries to access HADOOP_HOME
      hadoopConfig.set("io.serializations", Joiner.on(',').join(
          WritableSerialization.class.getName(), ResultSerialization.class.getName())
      );
      hadoopConfig.set("fs.gs.project.id", c.getPipelineOptions().as(DataflowPipelineOptions.class).getProject());
      for (Map.Entry<String, String> entry : HBaseImportIO.CONST_FILE_READER_PROPERTIES.entrySet()) {
        hadoopConfig.set(entry.getKey(), entry.getValue());
      }

      super.startBundle(c);
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      final String shardId = c.element().getKey();
      final BoundedSource<Result> source = c.element().getValue();

      LOG.info("Starting new shard");

      final String finalFilePath = basePath + "/" + shardId;

      final SequenceFile.Writer.Option[] writerOptions = new SequenceFile.Writer.Option[]{
          SequenceFile.Writer.file(new Path(finalFilePath)),
          SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
          SequenceFile.Writer.valueClass(Result.class),
          SequenceFile.Writer.compression(CompressionType.BLOCK)
      };

      final long start = System.nanoTime();
      long localStart = System.nanoTime();

      long reportedTotalDuration = 0;

      try (BoundedSource.BoundedReader<Result> reader = source.createReader(c.getPipelineOptions())) {
        boolean hasMore = reader.start();
        readDuration.addValue(System.nanoTime() - localStart);

        if (hasMore) {
          try (SequenceFile.Writer writer = SequenceFile.createWriter(hadoopConfig, writerOptions)) {
            final ImmutableBytesWritable key = new ImmutableBytesWritable();
            Result value;

            while (hasMore) {
              localStart = System.nanoTime();
              value = reader.getCurrent();
              key.set(value.getRow());
              writer.append(key, value);
              writeDuration.addValue(System.nanoTime() - localStart);

              localStart = System.nanoTime();
              hasMore = reader.advance();
              readDuration.addValue(System.nanoTime() - localStart);

              itemCounter.addValue(1L);
              // update totalDuration
              final long newTotalDuration = System.nanoTime() - start;
              totalDuration.addValue(newTotalDuration - reportedTotalDuration);
              reportedTotalDuration = newTotalDuration;
            }

            bytesWritten.addValue(writer.getLength());
          }
        }
      }
      final long newTotalDuration = System.nanoTime() - start;
      totalDuration.addValue(newTotalDuration - reportedTotalDuration);
    }
  }

  /**
   * Wrapper around dataflow's aggregator (aka counters). It's main purpose is allow us to use
   * different unit for benchmarking and display.  In other words it allows us to track counters in ns
   * but display the results in secs.
   */
  static class BatchCounter implements Serializable {
    private final Aggregator<Long, Long> aggregator;
    private final long batchSize;
    private long buffer = 0;

    BatchCounter(Aggregator<Long,Long> aggregator, long batchSize) {
      this.aggregator = aggregator;
      this.batchSize = batchSize;
    }
    void addValue(long inc) {
      buffer += inc;
      if (buffer >= batchSize) {
        aggregator.addValue(buffer / batchSize);
        buffer %= batchSize;
      }
    }
  }
}
