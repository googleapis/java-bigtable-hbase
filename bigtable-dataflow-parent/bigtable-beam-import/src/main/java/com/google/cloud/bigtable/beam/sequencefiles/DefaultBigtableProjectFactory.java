package com.google.cloud.bigtable.beam.sequencefiles;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;

public class DefaultBigtableProjectFactory implements DefaultValueFactory<String> {
  @Override
  public String create(PipelineOptions options) {
    return options.as(GcpOptions.class).getProject();
  }
}
