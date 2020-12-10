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

import com.google.bigtable.repackaged.com.google.api.core.InternalExtensionOnly;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.mapreduce.ComputeAndValidaeHashFromBigtableDoFn;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource.RangeHash;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A job that takes HBase HashTable output and compares the hashes from Cloud Bigtable table.
 *
 * <p>Execute the following command to run the job directly:
 *
 * <pre>
 *   mvn compile exec:java \                                                                                [20/11/18| 9:45PM]
 *      -DmainClass=com.google.cloud.bigtable.beam.sequencefiles.SyncTableJob \
 *      -Dexec.args="--runner=DataflowRunner \
 *            --project=$PROJECT \
 *            --bigtableInstanceId=$INSTANCE \
 *            --bigtableTableId=$TABLE \
 *            --hbaseRootDir=$HBASE_ROOT \
 *            --snapshotName=$SNAPSHOT_NAME  \
 *            --restoreDir=$RESTORE_DIR \
 *            --hashTableOutputDir=$HASHTABLE_OUTPUT_DIR \
 *            --outputPrefix=$OUtPUT_PREFIX \
 *            --defaultWorkerLogLevel=INFO \
 *            --stagingLocation=$STAGING_LOC \
 *            --tempLocation=$TMP_LOC \
 *            --region=$REGION \
 *            --workerZone=$WORKER_ZONE"
 * </pre>
 *
 * <p>Execute the following command to create the Dataflow template:
 *
 * <pre>
 *   TODO
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
 *
 * <p>Example
 *
 * <p>// TODO FIX this command
 *
 * <pre>
 *   $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/controller_service_account.json
 *   $ java -jar target/bigtable-beam-import-1.14.1-SNAPSHOT-shaded.jar importsnapshot \
 *   --runner=DataflowRunner --project=google.com:cloud-bigtable-dev \
 *   --bigtableInstanceId=lichng-test --bigtableTableId=books \
 *   --hbaseRootDir=gs://lichng-gcs/hbase-export \
 *   --snapshotName=validation_test_20200716_1635  \
 *   --restoreDir=gs://lichng-gcs/hbase --defaultWorkerLogLevel=DEBUG \
 *   --stagingLocation=gs://lichng-gcs/dataflow-test/staging \
 *   --tempLocation=gs://lichng-gcs/dataflow-test/temp
 *
 * </pre>
 */
@InternalExtensionOnly
public class SyncTableJob {
  private static final Log LOG = LogFactory.getLog(SyncTableJob.class);

  public interface SyncTableOptions extends ImportJob.ImportOptions {
    // Keep the snapshot params, we will need to restore the snapshot for cell by cell comparision.
    @Description("The HBase root dir where HBase snapshot files resides.")
    ValueProvider<String> getHbaseRootDir();

    @SuppressWarnings("unused")
    void setHbaseRootDir(ValueProvider<String> hbaseRootDir);

    @Description("Temp location for restoring snapshots")
    ValueProvider<String> getRestoreDir();

    @SuppressWarnings("unused")
    void setRestoreDir(ValueProvider<String> restoreDir);

    @Description("Snapshot name")
    ValueProvider<String> getSnapshotName();

    @SuppressWarnings("unused")
    void setSnapshotName(ValueProvider<String> snapshotName);

    @Description("HBase HashTable job output dir.")
    ValueProvider<String> getHashTableOutputDir();

    @SuppressWarnings("unused")
    void setHashTableOutputDir(ValueProvider<String> hashTableOutputDir);

    @Description("File pattern for files containing mismatched row ranges.")
    ValueProvider<String> getOutputPrefix();

    @SuppressWarnings("unused")
    void setOutputPrefix(ValueProvider<String> outputPrefix);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(SyncTableOptions.class);

    SyncTableOptions opts =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SyncTableOptions.class);

    LOG.info("DEBUG===> Building Pipeline");
    Pipeline pipeline = buildPipeline(opts);

    LOG.info("DEBUG===> Running Pipeline");
    PipelineResult result = pipeline.run();

    if (opts.getWait()) {
      Utils.waitForPipelineToFinish(result);
    }
  }

  @VisibleForTesting
  static Pipeline buildPipeline(SyncTableOptions opts) {
    // TODO: Make the snapshot loading optional, TBD after importSnapshot is checked-in
    HBaseSnapshotConfiguration conf =
        new HBaseSnapshotConfiguration(
            opts.getHbaseRootDir(), opts.getSnapshotName(), opts.getRestoreDir());
    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));

    pipeline
        .apply(
            "Read HBase HashTable output",
            Read.from(
                new HadoopHashTableSource(
                    new SerializableConfiguration(conf.getHbaseConf()),
                    // TODO: Check if we really need a ValueProvider here?
                    opts.getHashTableOutputDir())))
        .apply("Generate group by keys", ParDo.of(new GenerateGroupByKeyDoFn()))
        .apply(
            "group by and create granular workitems", GroupByKey.<String, List<RangeHash>>create())
        // TODO: Add counters for matches and mismatches.
        .apply("validate hash", ParDo.of(new ComputeAndValidaeHashFromBigtableDoFn(opts)))
        .apply("Serialize the ranges", MapElements.via(new RangeHashToString()))
        .apply("Write to file", TextIO.write().to(opts.getOutputPrefix()).withSuffix(".txt"));
    return pipeline;
  }

  static class RangeHashToString extends SimpleFunction<RangeHash, String> {
    @Override
    public String apply(RangeHash input) {
      return String.format(
          "[%s, %s)",
          Bytes.toStringBinary(input.startInclusive), Bytes.toStringBinary(input.endExclusive));
    }
  }
}
