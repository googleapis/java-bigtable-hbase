/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam.validation;

import com.google.bigtable.repackaged.com.google.api.core.InternalExtensionOnly;
import com.google.bigtable.repackaged.com.google.gson.Gson;
import com.google.cloud.bigtable.beam.sequencefiles.Utils;
import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.RangeHash;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A job that takes HBase HashTable output and compares the hashes from Cloud Bigtable table.
 *
 * <p>Execute the following command to run the job directly:
 *
 * <pre>
 *   mvn compile exec:java \
 *      -DmainClass=com.google.cloud.bigtable.beam.validation.SyncTableJob \
 *      -Dexec.args="--runner=DataflowRunner \
 *            --project=$PROJECT \
 *            --bigtableInstanceId=$INSTANCE \
 *            --bigtableTableId=$TABLE \
 *            --sourceHashDir=$SOURCE_HASH_DIR \
 *            --outputPrefix=$OUtPUT_PREFIX \
 *            --stagingLocation=$STAGING_LOC \
 *            --tempLocation=$TMP_LOC \
 *            --region=$REGION \
 *            --workerZone=$WORKER_ZONE"
 * </pre>
 *
 * <p>Execute the following command to create the Dataflow template:
 *
 * <pre>
 * mvn compile exec:java \
 *   -DmainClass=com.google.cloud.bigtable.beam.validation.SyncTableJob \
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
 * "src/main/resources/SyncTableJob_metadata".
 *
 * <p>An example using gcloud command line:
 *
 * <pre>
 * gcloud beta dataflow jobs run $JOB_NAME \
 *   --gcs-location gs://$TEMPLATE_PATH \
 *   --parameters bigtableProject=$PROJECT,bigtableInstanceId=$INSTANCE,bigtableTableId=$TABLE,sourceHashDir=gs://$SOURCE_HASH_DIR,outputPrefix=$OUTPUT_PREFIX
 * </pre>
 */
@InternalExtensionOnly
public class SyncTableJob {

  private static final Log LOG = LogFactory.getLog(SyncTableJob.class);

  public interface SyncTableOptions extends GcpOptions {

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

    @Description("HBase HashTable job output dir.")
    ValueProvider<String> getHashTableOutputDir();

    @SuppressWarnings("unused")
    // Rename it to sourceHashDir as in HBase sync table job.
    void setHashTableOutputDir(ValueProvider<String> hashTableOutputDir);

    @Description("File pattern for files containing mismatched row ranges.")
    ValueProvider<String> getOutputPrefix();

    @SuppressWarnings("unused")
    void setOutputPrefix(ValueProvider<String> outputPrefix);

    // When creating a template, this flag must be set to false.
    @Description("Wait for pipeline to finish.")
    @Default.Boolean(true)
    boolean getWait();

    @SuppressWarnings("unused")
    void setWait(boolean wait);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(SyncTableOptions.class);

    SyncTableOptions opts =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SyncTableOptions.class);

    LOG.info("===> Building Pipeline");
    Pipeline pipeline = buildPipeline(opts);

    LOG.info("===> Running Pipeline");
    PipelineResult result = pipeline.run();

    if (opts.getWait()) {
      Utils.waitForPipelineToFinish(result);
    }

    // Log all the counters for number of matches and number of mismatches.
    MetricQueryResults metrics = result.metrics().allMetrics();
    for (MetricResult<Long> counter : metrics.getCounters()) {
      LOG.warn(counter.getName() + ":" + counter.getAttempted());
    }
  }

  @VisibleForTesting
  public static Pipeline buildPipeline(SyncTableOptions opts) {
    Pipeline pipeline = Pipeline.create(Utils.tweakOptions(opts));
    pipeline
        .apply(
            "Read HBase HashTable output",
            Read.from(
                new BufferedHadoopHashTableSource(
                    new HadoopHashTableSource(
                        opts.getBigtableProject(), opts.getHashTableOutputDir()))))
        .apply(
            "group by and create granular workitems", GroupByKey.<String, List<RangeHash>>create())
        .apply("validate hash", ParDo.of(new ComputeAndValidateHashFromBigtableDoFn(opts)))
        .apply("Serialize the ranges", MapElements.via(new RangeHashToString()))
        .apply("Write to file", TextIO.write().to(opts.getOutputPrefix()).withSuffix(".txt"));
    return pipeline;
  }

  static class RangeHashToString extends SimpleFunction<RangeHash, String> {
    // TODO maybe explore a sequenceFile sink for RangeHash. Hadoop jobs using this output may be
    // easier to write for sequence file.
    private static final Gson GSON = new Gson();

    @Override
    public String apply(RangeHash input) {
      return GSON.toJson(input);
    }
  }
}
