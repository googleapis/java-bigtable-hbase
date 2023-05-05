/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots;

import com.google.api.core.InternalExtensionOnly;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.beam.TemplateUtils;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.HBaseSnapshotInputConfigBuilder;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.ImportConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.dofn.CleanupHBaseSnapshotRestoreFiles;
import com.google.cloud.bigtable.beam.hbasesnapshots.dofn.CleanupRestoredSnapshots;
import com.google.cloud.bigtable.beam.hbasesnapshots.dofn.RestoreSnapshot;
import com.google.cloud.bigtable.beam.hbasesnapshots.transforms.ListRegions;
import com.google.cloud.bigtable.beam.hbasesnapshots.transforms.ReadRegions;
import com.google.cloud.bigtable.beam.sequencefiles.HBaseResultToMutationFn;
import com.google.cloud.bigtable.beam.sequencefiles.ImportJob;
import com.google.cloud.bigtable.beam.sequencefiles.Utils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * A job that imports data from HBase snapshot exports hosted in Cloud Storage bucket into Cloud
 * Bigtable.
 *
 * <p>Example: If you have exported your HBase Snapshot to GCS bucket gs://$HBASE_EXPORT_ROOT_PATH
 * and want to import snapshot gs://$HBASE_EXPORT_ROOT_PATH/.hbase-snapshot/$SNAPSHOT_NAME into
 * Cloud Bigtable $TABLE in $INSTANCE, execute the following command to run the job directly:
 *
 * <pre>
 * mvn compile exec:java \
 *   -DmainClass=com.google.cloud.bigtable.beam.hbasesnapshots.ImportJobFromHbaseSnapshot \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --stagingLocation=gs://$STAGING_PATH \
 *                --project=$PROJECT \
 *                --bigtableInstanceId=$INSTANCE \
 *                --bigtableTableId=$TABLE \
 *                --hbaseSnapshotSourceDir=gs://$HBASE_EXPORT_ROOT_PATH \
 *                --snapshotName=$SNAPSHOT_NAME
 * </pre>
 *
 * Note that in the case of job failures, the temp files generated in the .restore-$JOB_NAME
 * directory under the snapshot export bucket will not get deleted. Hence one need to either launch
 * a replacement job with the same jobName to re-run the job or manually delete this directory.
 */
@InternalExtensionOnly
public class ImportJobFromHbaseSnapshot {
  private static final Log LOG = LogFactory.getLog(ImportJobFromHbaseSnapshot.class);

  @VisibleForTesting
  static final String MISSING_SNAPSHOT_SOURCEPATH =
      "Source Path containing hbase snapshots must be specified.";

  @VisibleForTesting
  static final String MISSING_SNAPSHOT_NAMES =
      "Snapshots must be specified. Allowed values are '*' (indicating all snapshots under source path) or "
          + "'prefix*' (snapshots matching certain prefix) or 'snapshotname1:tablename1,snapshotname2:tablename2' "
          + "(comma seperated list of snapshots)";

  public interface ImportOptions extends ImportJob.ImportOptions {
    @Description("The HBase root dir where HBase snapshot files resides.")
    String getHbaseSnapshotSourceDir();

    @SuppressWarnings("unused")
    void setHbaseSnapshotSourceDir(String hbaseSnapshotSourceDir);

    @Description("Snapshot name")
    String getSnapshotName();

    @SuppressWarnings("unused")
    void setSnapshotName(String snapshotName);

    @Description("Is importing Snappy compressed snapshot.")
    @Default.Boolean(false)
    Boolean getEnableSnappy();

    @SuppressWarnings("unused")
    void setEnableSnappy(Boolean enableSnappy);

    @Description("Path to config file containing snapshot source path/snapshot names.")
    String getImportConfigFilePath();

    void setImportConfigFilePath(String value);

    @Description(
        "Snapshots to be imported. Can be '*', 'prefix*' or 'snap1,snap2' or 'snap1:table1,snap2:table2'.")
    String getSnapshots();

    void setSnapshots(String value);

    @Description("Specifies whether to use dynamic splitting while reading hbase region.")
    @Default.Boolean(true)
    boolean getUseDynamicSplitting();

    void setUseDynamicSplitting(boolean value);
  }

  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(ImportOptions.class);

    ImportOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ImportOptions.class);

    // To determine the Google Cloud Storage file scheme (gs://)
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create().as(GcsOptions.class));

    LOG.info("Building Pipeline");
    Pipeline pipeline = null;
    // Maintain Backward compatibility until deprecation
    if (options.getSnapshotName() != null && !options.getSnapshotName().isEmpty()) {
      pipeline = buildPipeline(options);
    } else {
      ImportConfig importConfig =
          options.getImportConfigFilePath() != null
              ? buildImportConfigFromConfigFile(options.getImportConfigFilePath())
              : buildImportConfigFromPipelineOptions(options, options.as(GcsOptions.class));

      LOG.info(
          String.format(
              "SourcePath:%s, RestorePath:%s",
              importConfig.getSourcepath(), importConfig.getRestorepath()));
      pipeline = buildPipelineWithMultipleSnapshots(options, importConfig);
    }

    LOG.info("Running Pipeline");
    PipelineResult result = pipeline.run();

    if (options.getWait()) {
      Utils.waitForPipelineToFinish(result);
    }
  }

  @VisibleForTesting
  static ImportConfig buildImportConfigFromConfigFile(String configFilePath) throws Exception {
    Gson gson = new GsonBuilder().create();
    ImportConfig importConfig =
        gson.fromJson(SnapshotUtils.readFileContents(configFilePath), ImportConfig.class);
    Preconditions.checkNotNull(importConfig.getSourcepath(), MISSING_SNAPSHOT_SOURCEPATH);
    Preconditions.checkNotNull(importConfig.getSnapshots(), MISSING_SNAPSHOT_NAMES);
    SnapshotUtils.setRestorePath(importConfig);
    return importConfig;
  }

  @VisibleForTesting
  static ImportConfig buildImportConfigFromPipelineOptions(
      ImportOptions options, GcsOptions gcsOptions) throws IOException {
    Preconditions.checkArgument(
        options.getHbaseSnapshotSourceDir() != null, MISSING_SNAPSHOT_SOURCEPATH);
    Preconditions.checkArgument(options.getSnapshots() != null, MISSING_SNAPSHOT_NAMES);

    Map<String, String> snapshots =
        SnapshotUtils.isRegex(options.getSnapshots())
            ? SnapshotUtils.getSnapshotsFromSnapshotPath(
                options.getHbaseSnapshotSourceDir(),
                gcsOptions.getGcsUtil(),
                options.getSnapshots())
            : SnapshotUtils.getSnapshotsFromString(options.getSnapshots());

    ImportConfig importConfig = new ImportConfig();
    importConfig.setSourcepath(options.getHbaseSnapshotSourceDir());
    importConfig.setSnapshotsFromMap(snapshots);
    SnapshotUtils.setRestorePath(importConfig);
    return importConfig;
  }

  /**
   * Builds the pipeline that supports loading multiple snapshots to BigTable.
   *
   * @param options - Pipeline options
   * @param importConfig - Configuration representing snapshot source path, list of snapshots etc
   * @return
   * @throws Exception
   */
  static Pipeline buildPipelineWithMultipleSnapshots(
      ImportOptions options, ImportConfig importConfig) throws Exception {
    Map<String, String> configurations =
        SnapshotUtils.getConfiguration(
            options.getRunner().getSimpleName(),
            options.getProject(),
            importConfig.getSourcepath(),
            importConfig.getHbaseConfiguration());

    List<SnapshotConfig> snapshotConfigs =
        SnapshotUtils.buildSnapshotConfigs(
            importConfig.getSnapshots(),
            configurations,
            options.getProject(),
            importConfig.getSourcepath(),
            importConfig.getRestorepath());

    Pipeline pipeline = Pipeline.create(options);

    PCollection<SnapshotConfig> restoredSnapshots =
        pipeline
            .apply("Read Snapshot Configs", Create.of(snapshotConfigs))
            .apply("Restore Snapshots", ParDo.of(new RestoreSnapshot()));

    // Read records from hbase region files and write to Bigtable
    //    PCollection<RegionConfig> hbaseRecords = restoredSnapshots
    //            .apply("List Regions", new ListRegions());
    PCollection<KV<String, Iterable<Mutation>>> hbaseRecords =
        restoredSnapshots
            .apply("List Regions", new ListRegions())
            .apply("Read Regions", new ReadRegions(options.getUseDynamicSplitting()));

    options.setBigtableTableId(ValueProvider.StaticValueProvider.of("NA"));
    CloudBigtableTableConfiguration bigtableConfiguration =
        TemplateUtils.buildImportConfig(options, "HBaseSnapshotImportJob");
    if (importConfig.getBigtableConfiguration() != null) {
      CloudBigtableTableConfiguration.Builder builder = bigtableConfiguration.toBuilder();
      for (Map.Entry<String, String> entry : importConfig.getBigtableConfiguration().entrySet())
        builder = builder.withConfiguration(entry.getKey(), entry.getValue());
      bigtableConfiguration = builder.build();
    }

    hbaseRecords.apply(
        "Write to BigTable", CloudBigtableIO.writeToMultipleTables(bigtableConfiguration));

    // Clean up all the temporary restored snapshot HLinks after reading all the data
    restoredSnapshots
        .apply(Wait.on(hbaseRecords))
        .apply("Clean restored files", ParDo.of(new CleanupRestoredSnapshots()));

    return pipeline;
  }

  /**
   * Builds the pipeline that supports loading single snapshot to BigTable. Maintained for backward
   * compatiablity and will be deprecated merging the functionality to
   * buildPipelineWithMultipleSnapshots method.
   *
   * @param opts - Pipeline options
   * @return
   * @throws Exception
   */
  @VisibleForTesting
  static Pipeline buildPipeline(ImportOptions opts) throws Exception {
    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));
    HBaseSnapshotInputConfigBuilder configurationBuilder =
        new HBaseSnapshotInputConfigBuilder()
            .setProjectId(opts.getProject())
            .setHbaseSnapshotSourceDir(opts.getHbaseSnapshotSourceDir())
            .setSnapshotName(opts.getSnapshotName())
            .setRestoreDirSuffix(opts.getJobName());
    PCollection<KV<ImmutableBytesWritable, Result>> readResult =
        pipeline.apply(
            "Read from HBase Snapshot",
            HadoopFormatIO.<ImmutableBytesWritable, Result>read()
                .withConfiguration(configurationBuilder.build()));

    readResult
        .apply("Create Mutations", ParDo.of(new HBaseResultToMutationFn()))
        .apply(
            "Write to Bigtable",
            CloudBigtableIO.writeToTable(
                TemplateUtils.buildImportConfig(opts, "HBaseSnapshotImportJob")));

    final List<KV<String, String>> sourceAndRestoreFolders =
        Arrays.asList(
            KV.of(opts.getHbaseSnapshotSourceDir(), configurationBuilder.getRestoreDir()));
    pipeline
        .apply(Create.of(sourceAndRestoreFolders))
        .apply(Wait.on(readResult))
        .apply(ParDo.of(new CleanupHBaseSnapshotRestoreFiles()));

    return pipeline;
  }
}
