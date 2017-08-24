package com.google.cloud.bigtable.beam.sequencefiles;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;

class Utils {
  public static PipelineOptions tweakOptions(PipelineOptions opts) {
    if (!DataflowRunner.class.isAssignableFrom(opts.getRunner())) {
      return opts;
    }
    DataflowPipelineOptions dataflowOpts = opts.as(DataflowPipelineOptions.class);

    // By default, dataflow allocates 250 GB local disks, thats not necessary. Lower it unless the
    // user requested an explicit size
    if (dataflowOpts.getDiskSizeGb() == 0) {
      dataflowOpts.setDiskSizeGb(25);
    }

    return dataflowOpts;
  }

  public static class DefaultBigtableProjectFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return options.as(GcpOptions.class).getProject();
    }
  }
}
