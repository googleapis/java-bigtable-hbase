/*
 * Copyright 2018 Google LLC
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

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.sequencefiles.ExportJob.ExportOptions;
import com.google.cloud.bigtable.beam.sequencefiles.ImportJob.ImportOptions;
import com.google.cloud.bigtable.beam.validation.SyncTableJob.SyncTableOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * !!! DO NOT TOUCH THIS CLASS !!!
 *
 * <p>Utility needed to help setting runtime parameters in Bigtable configurations. This is needed
 * because the methods that take runtime parameters are package private and not intended for direct
 * public consumption for now.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class TemplateUtils {
  /**
   * Builds CloudBigtableTableConfiguration from input runtime parameters for import job with with
   * custom user agent.
   */
  public static CloudBigtableTableConfiguration buildImportConfig(
      ImportOptions opts, String customUserAgent) {
    CloudBigtableTableConfiguration.Builder builder =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(opts.getBigtableProject())
            .withInstanceId(opts.getBigtableInstanceId())
            .withTableId(opts.getBigtableTableId())
            .withConfiguration(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, customUserAgent);
    if (opts.getBigtableAppProfileId() != null) {
      builder.withAppProfileId(opts.getBigtableAppProfileId());
    }

    ValueProvider enableThrottling =
        ValueProvider.NestedValueProvider.of(
            opts.getMutationThrottleLatencyMs(),
            (Integer throttleMs) -> String.valueOf(throttleMs > 0));

    builder.withConfiguration(
        BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING, enableThrottling);
    builder.withConfiguration(
        BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS,
        ValueProvider.NestedValueProvider.of(opts.getMutationThrottleLatencyMs(), String::valueOf));

    return builder.build();
  }

  /** Builds CloudBigtableTableConfiguration from input runtime parameters for import job. */
  public static CloudBigtableTableConfiguration buildSyncTableConfig(SyncTableOptions opts) {
    CloudBigtableTableConfiguration.Builder builder =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(opts.getBigtableProject())
            .withInstanceId(opts.getBigtableInstanceId())
            .withTableId(opts.getBigtableTableId())
            .withConfiguration(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "SyncTableJob");
    if (opts.getBigtableAppProfileId() != null) {
      builder.withAppProfileId(opts.getBigtableAppProfileId());
    }
    return builder.build();
  }

  /** Builds CloudBigtableScanConfiguration from input runtime parameters for export job. */
  public static CloudBigtableScanConfiguration buildExportConfig(ExportOptions options) {
    CloudBigtableScanConfiguration.Builder configBuilder =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .withAppProfileId(options.getBigtableAppProfileId())
            .withConfiguration(
                BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "SequenceFileExportJob")
            .withScan(
                new SerializableScan(
                    options.getBigtableStartRow(),
                    options.getBigtableStopRow(),
                    options.getBigtableMaxVersions(),
                    options.getBigtableFilter()));

    return configBuilder.build();
  }
}
