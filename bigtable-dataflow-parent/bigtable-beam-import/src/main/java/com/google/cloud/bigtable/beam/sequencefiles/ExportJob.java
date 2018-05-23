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
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.beam.TemplateUtils;
import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;

/**
 * Beam job to export a Bigtable table to a set of SequenceFiles. Afterwards, the files can be
 * either imported into another Bigtable or HBase table. You can limit the rows and columns exported
 * using the options in {@link ExportOptions}. Please note that the rows in SequenceFiles will not
 * be sorted.
 *
 * Furthermore, you can export a subset of the data using a combination of --bigtableStartRow,
 * --bigtableStopRow and --bigtableFilter.
 *
 * <p>Execute the following command to run the job directly:
 *
 * <pre>
 * {@code mvn compile exec:java \
 *    -Dexec.mainClass=com.google.cloud.bigtable.beam.sequencefiles.ExportJob \
 *    -Dexec.args="--runner=dataflow \
 *    --project=[PROJECT_ID] \
 *    --tempLocation=gs://[BUCKET]/[TEMP_PATH] \
 *    --zone=[ZONE] \
 *    --bigtableInstanceId=[INSTANCE] \
 *    --bigtableTableId=[TABLE] \
 *    --destination=gs://[BUCLET]/[EXPORT_PATH] \
 *    --maxNumWorkers=[nodes * 10]"
 * }
 * </pre>
 *
 *  TODO: need update.....
 * <p>Execute the following command to create the Dataflow template:
 *
 * <pre>
 * mvn compile exec:java \
 *   -DmainClass=com.google.cloud.bigtable.beam.sequencefiles.ExportJob \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --project=[PROJECT_ID] \
 *                --stagingLocation=gs://[STAGING_PATH] \
 *                --templateLocation=gs://[TEMPLATE_PATH] \
 *                --wait=false"
 * </pre>
 *
 *
 *
 * <p>There are a few ways to run the pipeline using the template. See Dataflow doc for details:
 * https://cloud.google.com/dataflow/docs/templates/executing-templates. An example using gcloud
 * command line:
 *
 * <pre>
 * gcloud beta dataflow jobs run [JOB_NAME] \
 *   --gcs-location gs://[TEMPLATE_PATH] \
 *   --parameters bigtableProject=[PROJECT_ID],bigtableInstanceId=[INSTANCE],bigtableTableId=[TABLE],destinationPath=gs://[DESTINATION_PATH],filenamePrefix=[FILENAME_PREFIX]
 * </pre>
 *
 * @author igorbernstein2
 */
public class ExportJob {
  private static final Log LOG = LogFactory.getLog(ExportJob.class);

  public interface ExportOptions extends GcpOptions {
    @Description("This Bigtable App Profile id. (Replication alpha feature).")
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

    @Description("The row where to start the export from, defaults to the first row.")
    @Default.String("")
    ValueProvider<String> getBigtableStartRow();
    @SuppressWarnings("unused")
    void setBigtableStartRow(ValueProvider<String> startRow);

    @Description("The row where to stop the export, defaults to last row.")
    @Default.String("")
    ValueProvider<String> getBigtableStopRow();
    @SuppressWarnings("unused")
    void setBigtableStopRow(ValueProvider<String> stopRow);

    @Description("Maximum number of cell versions.")
    @Default.Integer(Integer.MAX_VALUE)
    ValueProvider<Integer> getBigtableMaxVersions();
    @SuppressWarnings("unused")
    void setBigtableMaxVersions(ValueProvider<Integer> maxVersions);

    @Description("Filter string. See: http://hbase.apache.org/book.html#thrift.")
    @Default.String("")
    ValueProvider<String> getBigtableFilter();
    @SuppressWarnings("unused")
    void setBigtableFilter(ValueProvider<String> filter);


    @Description("The destination directory")
    ValueProvider<String> getDestinationPath();
    @SuppressWarnings("unused")
    void setDestinationPath(ValueProvider<String> destinationPath);

    @Description("The prefix for each shard in destinationPath")
    @Default.String("part")
    ValueProvider<String> getFilenamePrefix();
    @SuppressWarnings("unused")
    void setFilenamePrefix(ValueProvider<String> filenamePrefix);

    @Description("Wait for pipeline to finish.")
    @Default.Boolean(true)
    boolean getWait();
    @SuppressWarnings("unused")
    void setWait(boolean wait);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(ExportOptions.class);

    ExportOptions opts = PipelineOptionsFactory
        .fromArgs(args).withValidation()
        .as(ExportOptions.class);

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

  static Pipeline buildPipeline(ExportOptions opts) {
    // Use the base target directory to stage bundles
    ValueProvider<ResourceId> destinationPath = NestedValueProvider
        .of(opts.getDestinationPath(), new StringToDirResourceId());

    // Concat the destination path & prefix for the final path
    FilePathPrefix filePathPrefix = new FilePathPrefix(destinationPath, opts.getFilenamePrefix());

    SequenceFileSink<ImmutableBytesWritable, Result> sink = new SequenceFileSink<>(
        destinationPath,
        DefaultFilenamePolicy.fromStandardParameters(
            filePathPrefix,
            null,
            "",
            false
        ),
        ImmutableBytesWritable.class, WritableSerialization.class,
        Result.class, ResultSerialization.class
    );

    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));

    CloudBigtableScanConfiguration config = TemplateUtils.BuildExportConfig(opts);
    pipeline
        .apply("Read table", Read.from(CloudBigtableIO.read(config)))
        .apply("Format results", MapElements.via(new ResultToKV()))
        .apply("Write", WriteFiles.to(sink));

    return pipeline;
  }

  static class ResultToKV extends SimpleFunction<Result, KV<ImmutableBytesWritable, Result>> {
    @Override
    public KV<ImmutableBytesWritable, Result> apply(Result input) {
      return KV.of(new ImmutableBytesWritable(input.getRow()), input);
    }
  }

  static class StringToDirResourceId implements SerializableFunction<String, ResourceId>, Serializable {
    @Override
    public ResourceId apply(String input) {
      return FileSystems.matchNewResource(input, true);
    }
  }

  static class FilePathPrefix implements ValueProvider<ResourceId>, Serializable {
    private final ValueProvider<ResourceId> destinationPath;
    private final ValueProvider<String> filenamePrefix;

    FilePathPrefix(ValueProvider<ResourceId> destinationPath,
        ValueProvider<String> filenamePrefix) {
      this.destinationPath = destinationPath;
      this.filenamePrefix = filenamePrefix;
    }

    @Override
    public ResourceId get() {
      return destinationPath.get().resolve(filenamePrefix.get(), StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public boolean isAccessible() {
      return destinationPath.isAccessible() && filenamePrefix.isAccessible();
    }
  }
}
