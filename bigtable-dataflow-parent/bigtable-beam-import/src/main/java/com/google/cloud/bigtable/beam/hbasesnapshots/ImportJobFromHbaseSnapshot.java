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

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.TemplateUtils;
<<<<<<< HEAD
import com.google.cloud.bigtable.beam.sequencefiles.*;
=======
import com.google.cloud.bigtable.beam.sequencefiles.CustomIndexTableTransform;
import com.google.cloud.bigtable.beam.sequencefiles.HBaseResultToMutationFn;
import com.google.cloud.bigtable.beam.sequencefiles.ImportJob;
import com.google.cloud.bigtable.beam.sequencefiles.Utils;
>>>>>>> origin/main
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.options.*;
<<<<<<< HEAD
import org.apache.beam.sdk.transforms.*;
=======
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
>>>>>>> origin/main
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
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
public class ImportJobFromHbaseSnapshot {
  private static final Log LOG = LogFactory.getLog(ImportJobFromHbaseSnapshot.class);

  public interface ImportOptions extends ImportJob.ImportOptions {
    @Description("The HBase root dir where HBase snapshot files resides.")
    String getHbaseSnapshotSourceDir();

    @SuppressWarnings("unused")
    void setHbaseSnapshotSourceDir(String hbaseSnapshotSourceDir);

    @Description("Snapshot name")
    String getSnapshotName();

    @SuppressWarnings("unused")
    void setSnapshotName(String snapshotName);

<<<<<<< HEAD

    // gs file location
    String getCountsMappingOutputLocation();
    void setCountsMappingOutputLocation(String countsMappingOutputLocation);

    String getImportSizeOutputLocation();
    void setImportSizeOutputLocation(String importSizeOutputLocation);

=======
>>>>>>> origin/main
    public interface CustomPipelineOptions extends PipelineOptions {
      @Description("Org Id")
      ValueProvider<String> getTenantId();
      void setTenantId(ValueProvider<String> tenantId);

      ValueProvider<String> getDelimiter();
      void setDelimiter(ValueProvider<String> delimiter);
    }
  }

  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(ImportOptions.class);

    ImportOptions opts =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(ImportOptions.class);
<<<<<<< HEAD
=======

    opts.getTenantId();
>>>>>>> origin/main

    LOG.info("Building Pipeline");
    Pipeline pipeline = buildPipeline(opts);
    LOG.info("Running Pipeline");
    PipelineResult result = pipeline.run();

    if (opts.getWait()) {
      Utils.waitForPipelineToFinish(result);
    }
  }

  @VisibleForTesting
  static Pipeline buildPipeline(ImportOptions opts) throws Exception {

    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));

    HBaseSnapshotInputConfigBuilder configurationBuilder =
            new HBaseSnapshotInputConfigBuilder()
                    .setProjectId(opts.getProject())
                    .setHbaseSnapshotSourceDir(opts.getHbaseSnapshotSourceDir())
                    .setSnapshotName(opts.getSnapshotName())
                    .setRestoreDirSuffix(opts.getJobName());

    PCollection<KV<ImmutableBytesWritable, Result>> originalResult =
            pipeline.apply(
                    "Read from HBase Snapshot",
                    HadoopFormatIO.<ImmutableBytesWritable, Result>read()
                            .withConfiguration(configurationBuilder.build()));

<<<<<<< HEAD
    originalResult
        .apply("Create Table", ParDo.of(new HBaseResultToMutationFn()))
        .apply(
                "Write to Bigtable",
                CloudBigtableIO.writeToTable(
                        TemplateUtils.buildImportConfig(opts, "HBaseSnapshotImportJob")));

    if (opts.getImportSizeOutputLocation() != null) {
      LOG.warn("Calculating import size...");

      PCollection<Integer> input = originalResult.apply("Calculate Import Size", ParDo.of(new ImportByteSizeSum()));
      PCollection<Integer> sum = input.apply(Sum.integersGlobally());
      sum.apply(MapElements.via(new ImportByteSizeSum.FormatAsTextFn()))
              .apply("Write Import Size", TextIO.write().to(opts.getImportSizeOutputLocation()));

    }

    if (opts.getCountsMappingOutputLocation() != null) {
      LOG.warn("Counts mapping output location: " + opts.getCountsMappingOutputLocation().toString());
      // testing tenant id extraction

      originalResult.apply("Extract TenantIds", ParDo.of(new TenantIdCountFn.ExtractTenantIdsFn()))
              .apply(Count.perElement())
              .apply(MapElements.via(new TenantIdCountFn.FormatAsTextFn()))
              .apply("Write TenantId Counts", TextIO.write().to(opts.getCountsMappingOutputLocation()));
    }

    if (opts.getReverseIndexTableId() != null) {
      LOG.warn("Reverse Index table id: " + opts.getReverseIndexTableId().toString());
      PCollection<Mutation> reverseIndexResult = originalResult
        .apply("Create Reverse Index Table", ParDo.of(new CustomIndexTableTransform()));

      reverseIndexResult
              .apply("Write Reverse Index Table to BigTable",
                      CloudBigtableIO.writeToTable(
                              TemplateUtils.buildIndexImportConfig(opts, "HBaseSnapshotImportJob")));
    }

=======
    PCollection<Mutation> indexResult = originalResult
            .apply("Create Mutations - Index", ParDo.of(new CustomIndexTableTransform()));

    originalResult
            .apply("Create Mutations - Original", ParDo.of(new HBaseResultToMutationFn()))
            .apply(
                    "Write Original to Bigtable",
                    CloudBigtableIO.writeToTable(
                            TemplateUtils.buildImportConfig(opts, "HBaseSnapshotImportJob")));

    indexResult
            .apply("Write Index Table to BigTable",
                    CloudBigtableIO.writeToTable(
                            TemplateUtils.buildIndexImportConfig(opts, "HBaseSnapshotImportJob")));

    // opts with one extra param for reversed table id
    // one set of opts for primary table, another for secondary table
    // one source, two outputs - create two partitions, one for reverse one for primary - shashank meeting
>>>>>>> origin/main

    final List<KV<String, String>> sourceAndRestoreFolders =
            Arrays.asList(
                    KV.of(opts.getHbaseSnapshotSourceDir(), configurationBuilder.getRestoreDir()));
    pipeline
            .apply(Create.of(sourceAndRestoreFolders))
            .apply(Wait.on(originalResult))
            .apply(ParDo.of(new CleanupHBaseSnapshotRestoreFilesFn()));

    return pipeline;
  }
}
