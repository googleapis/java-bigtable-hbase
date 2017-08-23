package com.google.cloud.bigtable.beam.sequencefiles;

import com.google.cloud.bigtable.beam.CloudBigtableConfiguration;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;

public class ExportJob {
  interface ExportOptions extends PipelineOptions {
    @Default.String("igorbernstein-dev")
    String getProjectId();
    void setProjectId(String projectId);

    @Default.String("instance1")
    String getInstanceId();
    void setInstanceId(String instanceId);

    @Default.String("table2")
    String getTableId();
    void setTableId(String tableId);

    @Default.String("/tmp/moo2")
    String getDestinationPath();
    void setDestinationPath(String destinationPath);

    @Default.String("part")
    String getFilenamePrefix();
    void setFilenamePrefix(String filenamePrefix);
  }

  public static void main(String[] args) {
    ExportOptions opts = PipelineOptionsFactory
        .fromArgs(args).withValidation()
        .as(ExportOptions.class);

    final Pipeline pipeline;

    if (false) {
      DirectOptions directOptions = opts.as(DirectOptions.class);
      directOptions.setTargetParallelism(1);
      pipeline = Pipeline.create(directOptions);
    } else {
      DataflowPipelineOptions dataflowPipelineOptions = opts.as(DataflowPipelineOptions.class);
      dataflowPipelineOptions.setRunner(DataflowRunner.class);

      dataflowPipelineOptions.setProject("igorbernstein-dev");
      dataflowPipelineOptions.setRegion("us-east1-c");
      dataflowPipelineOptions.setZone("us-east1-c");
      dataflowPipelineOptions.setStagingLocation("gs://igorbernstein-dev/dataflow-staging/");
      dataflowPipelineOptions.setGcpTempLocation("gs://igorbernstein-dev/dataflow-temp/");
      dataflowPipelineOptions.setMaxNumWorkers(200);
      dataflowPipelineOptions.setNumWorkers(200);

      pipeline = Pipeline.create(dataflowPipelineOptions);

      opts.setDestinationPath("gs://igorbernstein-dev/moo5/part");
    }

    Scan scan = new Scan();

    CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
        .withProjectId(opts.getProjectId())
        .withInstanceId(opts.getInstanceId())
        .withTableId(opts.getTableId())
        .withScan(scan)
        .build();

    StaticValueProvider<ResourceId> dest = StaticValueProvider
        .of(FileBasedSink.convertToFileResourceIfPossible(opts.getDestinationPath()));

    SequenceFileSink<ImmutableBytesWritable, Result> sink = new SequenceFileSink<>(
        dest,
        DefaultFilenamePolicy.constructUsingStandardParameters(
            dest,
            DefaultFilenamePolicy.DEFAULT_SHARD_TEMPLATE,
            ""
        ),
        ImmutableBytesWritable.class, WritableSerialization.class,
        Result.class, ResultSerialization.class
    );

    pipeline
        .apply("Read table", Read.from(CloudBigtableIO.read(config)))
        .apply("Mutate", MapElements.via(new ResultToKV()))
        .apply("Write", WriteFiles.to(sink));

    pipeline.run();
  }

  static class ResultToKV extends SimpleFunction<Result, KV<ImmutableBytesWritable, Result>> {
    @Override
    public KV<ImmutableBytesWritable, Result> apply(Result input) {
      return KV.of(new ImmutableBytesWritable(input.getRow()), input);
    }
  }
}
