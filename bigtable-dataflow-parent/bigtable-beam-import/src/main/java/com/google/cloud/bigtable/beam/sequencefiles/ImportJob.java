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
import com.google.cloud.bigtable.beam.TemplateUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;

/**
 * A job that imports data from Cloud Storage bucket with SequenceFile format into Cloud Bigtable. This job
 * can be run directly or as a Dataflow template.
 *
 * <p>Execute the following command to run the job directly:
 *
 * <pre>
 * mvn compile exec:java \
 *   -DmainClass=com.google.cloud.bigtable.beam.sequencefiles.ImportJob \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --stagingLocation=gs://$STAGING_PATH \
 *                --project=$PROJECT \
 *                --bigtableInstanceId=$INSTANCE \
 *                --bigtableTableId=$TABLE \
 *                --sourcePattern=gs://$SOURCE_PATTERN"
 * </pre>
 *
 * <p>Execute the following command to create the Dataflow template:
 *
 * <pre>
 * mvn compile exec:java \
 *   -DmainClass=com.google.cloud.bigtable.beam.sequencefiles.ImportJob \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --project=$PROJECT \
 *                --stagingLocation=gs://$STAGING_PATH \
 *                --templateLocation=gs://$TEMPLATE_PATH \
 *                --wait=false"
 * </pre>
 *
 * <p>There are a few ways to run the pipeline using the template. See Dataflow doc for details:
 * https://cloud.google.com/dataflow/docs/templates/executing-templates. Optionally, you can upload
 * a metadata file that contains information about the runtime parameters that can be used for
 * parameter validation purpose and more. A sample metadata file can be found at
 * "src/main/resources/ImportJob_metadata".
 *
 * <p>An example using gcloud command line:
 *
 * <pre>
 * gcloud beta dataflow jobs run $JOB_NAME \
 *   --gcs-location gs://$TEMPLATE_PATH \
 *   --parameters bigtableProject=$PROJECT,bigtableInstanceId=$INSTANCE,bigtableTableId=$TABLE,sourcePattern=gs://$SOURCE_PATTERN
 * </pre>
 */
public class ImportJob {
  static final long BUNDLE_SIZE = 100 * 1024 * 1024;

  public interface ImportOptions extends GcpOptions {
    @Description("This Bigtable App Profile id.")
    ValueProvider<String> getBigtableAppProfileId();
    @SuppressWarnings("unused")
    void setBigtableAppProfileId(ValueProvider<String> appProfileId);

    @Description("The project that contains the table to export. Defaults to --project.")
    @Default.InstanceFactory(Utils.DefaultBigtableProjectFactory.class)
    ValueProvider<String> getBigtableProject();
    @SuppressWarnings("unused")
    void setBigtableProject(ValueProvider<String> projectId);

    @Description("The Bigtable instance id that contains the table to export.")
    ValueProvider<String> getBigtableInstanceId();
    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @Description("The Bigtable table id to export.")
    ValueProvider<String> getBigtableTableId();
    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @Description("The fully qualified file pattern to import. Should of the form '[destinationPath]/part-*'")
    ValueProvider<String> getSourcePattern();
    @SuppressWarnings("unused")
    void setSourcePattern(ValueProvider<String> sourcePath);

    // When creating a template, this flag must be set to false.
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
      Utils.waitForPipelineToFinish(result);
    }
  }

  @VisibleForTesting
  static Pipeline buildPipeline(ImportOptions opts) {
    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));

    pipeline
        .apply(
            "Read Sequence File",
            Read.from(new ShuffledSource<>(createSource(opts.getSourcePattern()))))
        .apply("Create Mutations", ParDo.of(new HBaseResultToMutationFn()))
        .apply("Write to Bigtable", createSink(opts));

    return pipeline;
  }

  static SequenceFileSource<ImmutableBytesWritable, Result> createSource(
      ValueProvider<String> sourcePattern) {
    return new SequenceFileSource<>(
        sourcePattern,
        ImmutableBytesWritable.class,
        WritableSerialization.class,
        Result.class,
        ResultSerialization.class,
        BUNDLE_SIZE);
  }

  static PTransform<PCollection<Mutation>, PDone> createSink(ImportOptions opts) {
    CloudBigtableTableConfiguration config = TemplateUtils.BuildImportConfig(opts);
    return CloudBigtableIO.writeToTable(config);
  }
}
