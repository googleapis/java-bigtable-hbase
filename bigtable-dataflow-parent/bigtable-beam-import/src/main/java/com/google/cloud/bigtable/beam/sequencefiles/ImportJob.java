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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;

public class ImportJob {
  private static final Log LOG = LogFactory.getLog(ImportJob.class);

  interface ImportOptions extends PipelineOptions {
    //TODO: switch to ValueProviders

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

    @Description("The source path of the SequenceFiles to import.")
    ValueProvider<String> getSourcePath();
    @SuppressWarnings("unused")
    void setSourcePath(ValueProvider<String> sourcePath);

    @Description("Wait for pipeline to finish.")
    @Default.Boolean(false)
    boolean getWait();
    @SuppressWarnings("unused")
    void setWait(boolean wait);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(ImportOptions.class);

    ImportOptions opts = PipelineOptionsFactory
        .fromArgs(args).withValidation()
        .as(ImportOptions.class);

    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));

    SequenceFileSource<ImmutableBytesWritable, Result> source = new SequenceFileSource<>(
        opts.getSourcePath(),
        ImmutableBytesWritable.class, WritableSerialization.class,
        Result.class, ResultSerialization.class,
        100 * 1024 * 1024
    );

    CloudBigtableTableConfiguration config = new CloudBigtableTableConfiguration.Builder()
        .withProjectId(opts.getBigtableProject())
        .withInstanceId(opts.getBigtableInstanceId())
        .withTableId(opts.getBigtableTableId())
        .build();

    pipeline
        .apply("Read Sequence File", Read.from(source))
        .apply("Create Mutations", ParDo.of(new HBaseResultToMutationFn()))
        .apply("Write to Bigtable", CloudBigtableIO.writeToTable(config));

    PipelineResult result = pipeline.run();

    if (opts.getWait()) {
      State state = result.waitUntilFinish();
      LOG.info("Job finished with state: " + state.name());
      if (state != State.DONE) {
        System.exit(1);
      }
    }
  }
}
