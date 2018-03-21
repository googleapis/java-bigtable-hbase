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
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;

public class ImportJob {
  private static final Log LOG = LogFactory.getLog(ImportJob.class);

  private static final long BUNDLE_SIZE = 100 * 1024 * 1024;

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

    @Description("Create the target table using families specified by this list. Example: --newTableFamilies=family1,family2")
    @Nullable
    List<String> getNewTableFamilies();
    @SuppressWarnings("unused")
    void setNewTableFamilies(@Nullable List<String> enabled);
  }

  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(ImportOptions.class);

    ImportOptions opts = PipelineOptionsFactory
        .fromArgs(args).withValidation()
        .as(ImportOptions.class);

    FileSystems.setDefaultPipelineOptions(opts);

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
  static Pipeline buildPipeline(ImportOptions opts) throws Exception {
    System.out.println("building pipeline");

    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));

    SequenceFileSource<ImmutableBytesWritable, Result> source = new SequenceFileSource<>(
        opts.getSourcePattern(),
        ImmutableBytesWritable.class, WritableSerialization.class,
        Result.class, ResultSerialization.class,
        BUNDLE_SIZE
    );

    if (opts.getNewTableFamilies() != null && !opts.getNewTableFamilies().isEmpty()) {
      createPreSplitTable(source, opts);
    }

    CloudBigtableTableConfiguration.Builder configBuilder = new CloudBigtableTableConfiguration.Builder()
        .withProjectId(opts.getBigtableProject())
        .withInstanceId(opts.getBigtableInstanceId())
        .withTableId(opts.getBigtableTableId());

    if (opts.getBigtableAppProfileId() != null) {
      configBuilder.withAppProfileId(opts.getBigtableAppProfileId());
    }

    pipeline
        .apply("Read Sequence File", Read.from(new ShuffledSource<>(source)))
        .apply("Create Mutations", ParDo.of(new HBaseResultToMutationFn()))
        .apply("Write to Bigtable", CloudBigtableIO.writeToTable(configBuilder.build()));

    return pipeline;
  }

  static void createPreSplitTable(SequenceFileSource<ImmutableBytesWritable, Result> source, ImportOptions opts)
      throws Exception {
    LOG.info("Extracting splits from the source files");

    // Extract the splits from the sequence files
    List<? extends FileBasedSource<KV<ImmutableBytesWritable, Result>>> splitSources = source
        .split(BUNDLE_SIZE, opts);

    // Read the start key of each split
    byte[][] splits = splitSources.stream()
        .parallel()
        .map(splitSource -> {
          try (BoundedReader<KV<ImmutableBytesWritable, Result>> reader = splitSource.createReader(opts)) {
            if (reader.start()) {
              return reader.getCurrent().getKey();
            }
          } catch (Exception e) {
            Throwables.propagate(e);
          }
          return null;
        })
        .filter(Objects::nonNull)
        .sorted()
        .map(ImmutableBytesWritable::copyBytes)
        .toArray(byte[][]::new);

    LOG.info(String.format("Creating a new table with %d splits and the families: %s", splits.length, opts.getNewTableFamilies()));
    try (Connection connection = BigtableConfiguration.connect(opts.getBigtableProject(), opts.getBigtableInstanceId())) {
      HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(opts.getBigtableTableId()));
      for (String family : opts.getNewTableFamilies()) {
        descriptor.addFamily(
            new HColumnDescriptor(family)
        );
      }
      connection.getAdmin().createTable(descriptor, splits);
    }
  }
}
