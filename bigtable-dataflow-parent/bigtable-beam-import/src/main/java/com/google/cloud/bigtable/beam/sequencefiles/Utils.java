/*
 * Copyright 2017 Google Inc.
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

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import com.google.common.base.Strings;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@InternalApi
public class Utils {
  private static final Log LOG = LogFactory.getLog(Utils.class);

  /**
   * Helper to tweak default pipelineOptions for import/export jobs
   *
   * @param opts
   * @return PipelineOptions
   */
  public static PipelineOptions tweakOptions(PipelineOptions opts) {
    if (!DataflowRunner.class.isAssignableFrom(opts.getRunner())) {
      return opts;
    }
    DataflowPipelineOptions dataflowOpts = opts.as(DataflowPipelineOptions.class);

    // Region is a newly added requirement in newer versions of dataflow runner.
    // Make the pipeline backwards compatible by inferring the region from the zone.
    // This is done by chopping off the last dash
    if (Strings.isNullOrEmpty(dataflowOpts.getRegion())) {
      String zone = dataflowOpts.getWorkerZone();
      if (Strings.isNullOrEmpty(zone)) {
        zone = dataflowOpts.getZone();
      }

      if (!Strings.isNullOrEmpty(zone)) {
        String region = zone.replaceAll("-[^-]+]$", "");
        dataflowOpts.setRegion(region);
      }
    }

    // By default, dataflow allocates 250 GB local disks, thats not necessary. Lower it unless the
    // user requested an explicit size
    if (dataflowOpts.getDiskSizeGb() == 0) {
      dataflowOpts.setDiskSizeGb(25);
    }

    /**
     * Bigtable pipelines are very GC intensive, For each cell in Bigtable we create following
     * objects: 1. Row key 2. Column qualifier 3. Timestamp 4. Value 5. A cell object that contains
     * the above 4 objects.
     *
     * <p>So each cell has at least 5 objects. On top of that, each cell may represented by
     * different kinds of objects. For example, import job creates HBase Result object and Mutation
     * objects for all the cells. Same is the case with Snapshot related pipelines.
     *
     * <p>Given this abundance of objects, for cells with smaller values, the pipeline may lead to a
     * high GC overhead, but it does make progress. The MemoryMonitor on dataflow worker kills the
     * pipeline and results in wasted work.
     *
     * <p>The above is true for most dataflow pipeline, but this specific use case is different as
     * the pipeline does nothing else. CPU is only used for object transformation and GC. So, we
     * disable the memory monitor on Bigtable pipelines. If pipeline stalls, it will OOM and then
     * human intervention will be required. As a mitigation, users should choose a worker machine
     * with higher memory or reduce the parallelism on the workers (by setting
     * --numberOfWorkerHarnessThreads).
     */
    DataflowPipelineDebugOptions debugOptions = dataflowOpts.as(DataflowPipelineDebugOptions.class);
    debugOptions.setGCThrashingPercentagePerPeriod(100.00);

    return debugOptions;
  }

  /** A default project id provider for bigtable that reads the default {@link GcpOptions} */
  public static class DefaultBigtableProjectFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return options.as(GcpOptions.class).getProject();
    }
  }

  /** A simple converter to adapt strings representing directories to {@link ResourceId}s. */
  static class StringToDirectoryResourceId extends SimpleFunction<String, ResourceId> {
    @Override
    public ResourceId apply(String input) {
      return FileSystems.matchNewResource(input, true);
    }
  }

  /**
   * Wait for the pipeline to finish if we are not creating a template. Exit with error if the
   * pipeline finishes, but not in {@link State#DONE} state. Log a warning if creating a template.
   *
   * @param result
   */
  public static void waitForPipelineToFinish(PipelineResult result) {
    try {
      // Check to see if we are creating a template.
      // This should throw {@link UnsupportedOperationException} when creating a template.
      result.getState();

      State state = result.waitUntilFinish();
      LOG.info("Job finished with state: " + state.name());
      if (state != State.DONE) {
        System.exit(1);
      }
    } catch (UnsupportedOperationException e) {
      LOG.warn("Unable to wait for pipeline to finish.", e);
    }
  }
}
