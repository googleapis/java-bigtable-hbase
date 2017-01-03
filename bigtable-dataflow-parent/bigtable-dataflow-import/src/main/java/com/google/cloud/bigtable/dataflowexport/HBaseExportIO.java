package com.google.cloud.bigtable.dataflowexport;

import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.dataflowimport.HBaseImportIO;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CoderFactories;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.repackaged.com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.ImmutableList;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseExportIO {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseExportIO.class);

  public static void main(String[] args) throws IOException {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    DataflowPipelineOptions options2 = options.as(DataflowPipelineOptions.class);
    options2.setProject("igorbernstein-dev");

    options2.setRunner(BlockingDataflowPipelineRunner.class);
    options2.setNumWorkers(90);
    options2.setStagingLocation("gs://igorbernstein-dev/dataflow-staging");
    options2.setZone("us-east1-c");
    options2.setDiskSizeGb(20);

    Pipeline p = Pipeline.create(options2);

    BoundedSource<Result> source = CloudBigtableIO.read(
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId("igorbernstein-dev")
            .withInstanceId("instance1")
            .withTableId("table1")
            .build()
    );

    SerializableCoder<BoundedSource<Result>> sourceCoder = SerializableCoder.of(new TypeDescriptor<BoundedSource<Result>>() {});
    KvCoder<String, BoundedSource<Result>> kvCoder = KvCoder.of(StringUtf8Coder.of(), sourceCoder);

    p
        .apply("Initialize source", Create.of(source).withCoder(sourceCoder))
        .apply("Shard", ParDo.of(new ShardScans())).setCoder(kvCoder)
        .apply("Prevent Fusion", GroupByKey.<String, BoundedSource<Result>>create())
        .apply("Write", ParDo.of(new WriteResultsToSeq("gs://igorbernstein-dev/export24")));

    p.run();
  }


  static class ShardScans extends DoFn<BoundedSource<Result>, KV<String, BoundedSource<Result>>> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      BoundedSource<Result> source = c.element();

      // Shard the bundles into max 4 gig shards
      List<? extends BoundedSource<Result>> boundedSources = source
          .splitIntoBundles(4L * 1024 * 1024 * 1024, c.getPipelineOptions());

      // randomize order to make sure that scans are distributed across tablets
      Collections.shuffle(boundedSources);

      for (BoundedSource<Result> sourceBundle : boundedSources) {
        c.output(KV.of(UUID.randomUUID().toString(), sourceBundle));
      }
    }
  }


  static class WriteResultsToSeq extends DoFn<KV<String, Iterable<BoundedSource<Result>>>, Void> {
    private final String basePath;

    public WriteResultsToSeq(String basePath) {
      this.basePath = basePath;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      String bundleId = c.element().getKey();
      BoundedSource<Result> source = c.element().getValue().iterator().next();

      String tempFilePath = basePath + "/_temporary/" + bundleId;
      String finalFilePath = basePath + "/" + bundleId;

      long now = System.currentTimeMillis();
      LOG.info("Starting bundle on " + InetAddress.getLocalHost());

      long items = 0;
      long openTime = 0;
      long readTime = 0;
      long writeTime = 0;

      long closeTime = 0;
      long totalTime = 0;

      long localStart = System.nanoTime();
      long start = localStart;

      GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(c.getPipelineOptions());

      try (BoundedSource.BoundedReader<Result> reader = source.createReader(c.getPipelineOptions())) {
        boolean hasMore = reader.start();

        if (hasMore) {
          LOG.info("Starting bundle with row {}", new String(reader.getCurrent().getRow()));
          Configuration conf = new Configuration(false);

          // Not using setStrings to avoid loading StringUtils which tries to access HADOOP_HOME
          conf.set("io.serializations", Joiner.on(',').join(
              WritableSerialization.class.getName(), ResultSerialization.class.getName())
          );
          conf.set("fs.gs.project.id", c.getPipelineOptions().as(DataflowPipelineOptions.class).getProject());
          for (Map.Entry<String, String> entry : HBaseImportIO.CONST_FILE_READER_PROPERTIES.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
          }

          SequenceFile.Writer.Option[] options = new SequenceFile.Writer.Option[]{
              SequenceFile.Writer.file(new Path(tempFilePath)),
              SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
              SequenceFile.Writer.valueClass(Result.class)
          };

          try (SequenceFile.Writer writer = SequenceFile.createWriter(conf, options)) {
            ImmutableBytesWritable key = new ImmutableBytesWritable();
            Result value;

            openTime = System.nanoTime() - localStart;

            while (hasMore) {
              items++;
              localStart = System.nanoTime();

              value = reader.getCurrent();
              key.set(value.getRow());
              writer.append(key, value);
              writeTime += System.nanoTime() - localStart;

              localStart = System.nanoTime();
              hasMore = reader.advance();
              readTime += System.nanoTime() - localStart;
            }

            localStart = System.nanoTime();
          }
        }
      }

      closeTime = System.nanoTime() - localStart;
      totalTime = System.nanoTime() - start;
      long div = 1_000_000_000;
      gcsUtil.copy(Collections.singletonList(tempFilePath), Collections.singletonList(finalFilePath));
      gcsUtil.remove(Collections.singletonList(tempFilePath));

      LOG.info("Finished bundle. count: {}, open: {}, read: {}, write: {}, close: {}, total: {}. Worker: {}",
          items, openTime / div, readTime / div, writeTime / div, closeTime / div, totalTime / div, InetAddress.getLocalHost());

    }
  }
}
