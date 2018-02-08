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
import com.google.cloud.bigtable.beam.sequencefiles.Utils.StringToDirectoryResourceId;
import java.nio.charset.CharacterCodingException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;

/**
 * <p>
 * Beam job to export a Bigtable table to a set of SequenceFiles.
 * Afterwards, the files can be either imported into another Bigtable or HBase table.
 * You can limit the rows and columns exported using the options in {@link ExportOptions}.
 * Please note that the rows in SequenceFiles will not be sorted.
 *
 * Example usage:
 * </p>
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
 * Furthermore, you can export a subset of the data using a combination of --bigtableStartRow,
 * --bigtableStopRow and --bigtableFilter.
 *
 * @author igorbernstein2
 */
public class ExportJob {
  private static final Log LOG = LogFactory.getLog(ExportJob.class);

  public interface ExportOptions extends GcpOptions {
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

    @Description("The row where to start the export from, defaults to the first row.")
    @Default.String("")
    String getBigtableStartRow();
    @SuppressWarnings("unused")
    void setBigtableStartRow(String startRow);

    @Description("The row where to stop the export, defaults to last row.")
    @Default.String("")
    String getBigtableStopRow();
    @SuppressWarnings("unused")
    void setBigtableStopRow(String stopRow);

    @Description("Maximum number of cell versions.")
    @Default.Integer(Integer.MAX_VALUE)
    int getBigtableMaxVersions();
    @SuppressWarnings("unused")
    void setBigtableMaxVersions(int maxVersions);

    @Description("Filter string. See: http://hbase.apache.org/book.html#thrift.")
    @Default.String("")
    String getBigtableFilter();
    @SuppressWarnings("unused")
    void setBigtableFilter(String filter);


    @Description("The destination path. Can either specify a directory ending with a / or a file prefix.")
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

  public static void main(String[] args) throws CharacterCodingException {
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

  static Pipeline buildPipeline(ExportOptions opts) throws CharacterCodingException {
    Scan scan = new Scan();

    if (!opts.getBigtableStartRow().isEmpty()) {
      scan.setStartRow(opts.getBigtableStartRow().getBytes());
    }
    if (!opts.getBigtableStopRow().isEmpty()) {
      scan.setStopRow(opts.getBigtableStopRow().getBytes());
    }

    scan.setMaxVersions(opts.getBigtableMaxVersions());

    if (!opts.getBigtableFilter().isEmpty()) {
      scan.setFilter(new ParseFilter().parseFilterString(opts.getBigtableFilter()));
    }

    CloudBigtableScanConfiguration.Builder configBuilder = new CloudBigtableScanConfiguration.Builder()
        .withProjectId(opts.getBigtableProject())
        .withInstanceId(opts.getBigtableInstanceId())
        .withTableId(opts.getBigtableTableId())
        .withScan(scan);

    if (opts.getBigtableAppProfileId() != null) {
      configBuilder.withAppProfileId(opts.getBigtableAppProfileId());
    }


    ValueProvider<ResourceId> dest = NestedValueProvider.of(
        opts.getDestinationPath(), new StringToDirectoryResourceId()
    );

    SequenceFileSink<ImmutableBytesWritable, Result> sink = new SequenceFileSink<>(
        dest,
        DefaultFilenamePolicy.constructUsingStandardParameters(
            LocalResources.fromString(opts.getFilenamePrefix(), false),
            null,
            "",
            false
        ),
        ImmutableBytesWritable.class, WritableSerialization.class,
        Result.class, ResultSerialization.class
    );

    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));

    pipeline
        .apply("Read table", Read.from(CloudBigtableIO.read(configBuilder.build())))
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

}
