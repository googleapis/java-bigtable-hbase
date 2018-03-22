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
package com.google.cloud.bigtable.beam.sequencefiles;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;

public class ImportJob {
  private static final Log LOG = LogFactory.getLog(ImportJob.class);

  static final long BUNDLE_SIZE = 100 * 1024 * 1024;

  public interface ImportOptions extends GcpOptions {
    //TODO: switch to ValueProviders

    @Description("This Bigtable App Profile id. (Replication alpha feature).")
    String getBigtableAppProfileId();
    @SuppressWarnings("unused")
    void setBigtableAppProfileId(String appProfileId);

    @Description("The project that contains the table to export. Defaults to --project.")
    @Default.InstanceFactory(Utils.DefaultBigtableProjectFactory.class)
    String getBigtableProject();
    @SuppressWarnings("unused")
    void setBigtableProject(String projectId);

    @Description("The Bigtable instance id that contains the table to export.")
    String getBigtableInstanceId();
    @SuppressWarnings("unused")
    void setBigtableInstanceId(String instanceId);

    @Description("The Bigtable table id to export.")
    String getBigtableTableId();
    @SuppressWarnings("unused")
    void setBigtableTableId(String tableId);

    @Description("The fully qualified file pattern to import. Should of the form '[destinationPath]/part-*'")
    ValueProvider<String> getSourcePattern();
    @SuppressWarnings("unused")
    void setSourcePattern(ValueProvider<String> sourcePath);

    @Description("Wait for pipeline to finish.")
    @Default.Boolean(true)
    boolean getWait();
    @SuppressWarnings("unused")
    void setWait(boolean wait);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(ImportOptions.class);

    ImportOptions opts = PipelineOptionsFactory
        .fromArgs(args).withValidation()
        .as(ImportOptions.class);

    Pipeline pipeline = buildPipeline(opts);

    PipelineResult result = pipeline.run();

    if (opts.getWait()) {
      State state = result.waitUntilFinish();
      LOG.info("Job finished with state: " + state.name());
      if (state != State.DONE) {
        System.exit(1);
      }
    }
  }

  @VisibleForTesting
  static Pipeline buildPipeline(ImportOptions opts) {
    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));

    pipeline
        .apply("Read Sequence File", Read.from(new ShuffledSource<>(createSource(opts))))
        .apply("Create Mutations", ParDo.of(new HBaseResultToMutationFn()))
        .apply("Write to Bigtable", createSink(opts));

    return pipeline;
  }

  static SequenceFileSource<ImmutableBytesWritable, Result> createSource(ImportOptions opts) {
    return new SequenceFileSource<>(
        opts.getSourcePattern(),
        ImmutableBytesWritable.class, WritableSerialization.class,
        Result.class, ResultSerialization.class,
        BUNDLE_SIZE
    );
  }

  static PTransform<PCollection<Mutation>, PDone> createSink(ImportOptions opts) {
    CloudBigtableTableConfiguration.Builder configBuilder = new CloudBigtableTableConfiguration.Builder()
        .withProjectId(opts.getBigtableProject())
        .withInstanceId(opts.getBigtableInstanceId())
        .withTableId(opts.getBigtableTableId());

    if (opts.getBigtableAppProfileId() != null) {
      configBuilder.withAppProfileId(opts.getBigtableAppProfileId());
    }

    return CloudBigtableIO.writeToTable(configBuilder.build());
  }
}
