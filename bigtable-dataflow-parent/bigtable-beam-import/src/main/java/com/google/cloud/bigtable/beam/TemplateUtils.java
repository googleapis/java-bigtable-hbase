/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam;

import com.google.cloud.bigtable.beam.sequencefiles.ExportJob.ExportOptions;
import com.google.cloud.bigtable.beam.sequencefiles.ImportJob.ImportOptions;
import java.io.Serializable;
import java.nio.charset.CharacterCodingException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ParseFilter;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;

/**
 * !!! DO NOT TOUCH THIS CLASS !!!
 *
 * <p>Utility needed to help setting runtime parameters in Bigtable configurations. This is needed
 * because the methods that take runtime parameters are package private and not intended for direct
 * public consumption for now.
 */
public class TemplateUtils {
  /** Builds CloudBigtableTableConfiguration from input runtime parameters for import job. */
  public static CloudBigtableTableConfiguration BuildImportConfig(ImportOptions opts) {
    CloudBigtableTableConfiguration.Builder builder =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(opts.getBigtableProject())
            .withInstanceId(opts.getBigtableInstanceId())
            .withTableId(opts.getBigtableTableId());
    if (opts.getBigtableAppProfileId() != null) {
      builder.withAppProfileId(opts.getBigtableAppProfileId());
    }

    ValueProvider enableThrottling = ValueProvider.NestedValueProvider.of(
        opts.getMutationThrottleLatencyMs(), (Integer throttleMs) -> String.valueOf(throttleMs > 0));

    builder.withConfiguration(BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING, enableThrottling);
    builder.withConfiguration(BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS,
        ValueProvider.NestedValueProvider.of(opts.getMutationThrottleLatencyMs(), String::valueOf));

    return builder.build();
  }

  /** Builds CloudBigtableScanConfiguration from input runtime parameters for export job. */
  public static CloudBigtableScanConfiguration BuildExportConfig(ExportOptions options) {
    CloudBigtableScanConfiguration.Builder configBuilder =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .withAppProfileId(options.getBigtableAppProfileId())
            .withConfiguration("startRow", options.getBigtableStartRow())
            .withConfiguration("stopRow", options.getBigtableStopRow())
            .withMaxVersion(options.getBigtableMaxVersions())
            .withConfiguration("filter", options.getBigtableFilter());

    return configBuilder.build();
  }
}
