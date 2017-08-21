package com.google.cloud.bigtable.beam.sequencefiles;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.beam.coders.HBaseResultCoder;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;

public class ImportJob {
  interface ImportOptions extends PipelineOptions {
    ValueProvider<String> getProjectId();
    ValueProvider<String> getInstanceId();
    ValueProvider<String> getTableId();
    ValueProvider<String> getSourcePath();

  }

  public static void main(String[] args) {
    ImportOptions opts = PipelineOptionsFactory
        .fromArgs(args).withValidation()
        .as(ImportOptions.class);

    Pipeline pipeline = Pipeline.create(opts);

    SequenceFileSource<ImmutableBytesWritable, Result> source = new SequenceFileSource<>(
        opts.getSourcePath(),
        ImmutableBytesWritable.class,
        WritableCoder.of(ImmutableBytesWritable.class),
        Result.class,
        HBaseResultCoder.of(),
        Arrays.<Class<? extends Serialization<?>>>asList(
            WritableSerialization.class,
            ResultSerialization.class
        )
    );

    CloudBigtableTableConfiguration config = new CloudBigtableTableConfiguration.Builder()
        .withProjectId(opts.getProjectId().get())
        .withInstanceId(opts.getInstanceId().get())
        .withTableId(opts.getTableId().get())
        .build();

    pipeline
        .apply("Read Sequence File", Read.from(source))
        .apply("Create Mutations", ParDo.of(new HBaseResultToMutationFn()))
        .apply("Write to Bigtable", CloudBigtableIO.writeToTable(config));

    pipeline.run();
  }
}
