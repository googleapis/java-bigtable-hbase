/*
 * Copyright 2026 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.beam.sequencefiles.ImportJob.ImportOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link TemplateUtils} to ensure correct configuration building. */
@RunWith(JUnit4.class)
public class TemplateUtilsTest {

  /**
   * Tests that {@link TemplateUtils#buildImportConfig} sets correct default values for Bigtable
   * configuration.
   */
  @Test
  public void testBuildImportConfig_defaults() {
    ImportOptions opts = PipelineOptionsFactory.as(ImportOptions.class);
    opts.setBigtableProject(StaticValueProvider.of("my-project"));
    opts.setBigtableInstanceId(StaticValueProvider.of("my-instance"));
    opts.setBigtableTableId(StaticValueProvider.of("my-table"));

    CloudBigtableTableConfiguration config = TemplateUtils.buildImportConfig(opts, "my-agent");

    assertEquals("my-project", config.getProjectId());
    assertEquals("my-instance", config.getInstanceId());
    assertEquals("my-table", config.getTableId());

    Map<String, ValueProvider<String>> configMap = config.getConfiguration();
    assertEquals("my-agent", configMap.get(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY).get());
    assertTrue(configMap.containsKey(BigtableOptionsFactory.MAX_INFLIGHT_RPCS_KEY));
    assertTrue(
        configMap.containsKey(BigtableHBaseSettings.BULK_MUTATION_CLOSE_TIMEOUT_MILLISECONDS));
  }

  /**
   * Tests that {@link TemplateUtils#buildImportConfig} correctly overrides defaults with custom
   * values provided in options.
   */
  @Test
  public void testBuildImportConfig_customValues() {
    ImportOptions opts = PipelineOptionsFactory.as(ImportOptions.class);
    opts.setBigtableProject(StaticValueProvider.of("my-project"));
    opts.setBigtableInstanceId(StaticValueProvider.of("my-instance"));
    opts.setBigtableTableId(StaticValueProvider.of("my-table"));
    opts.setMaxInflightRpcs(StaticValueProvider.of(200));
    opts.setBulkMutationCloseTimeoutMinutes(StaticValueProvider.of(60));

    CloudBigtableTableConfiguration config = TemplateUtils.buildImportConfig(opts, "my-agent");

    Map<String, ValueProvider<String>> configMap = config.getConfiguration();
    assertEquals("200", configMap.get(BigtableOptionsFactory.MAX_INFLIGHT_RPCS_KEY).get());
    assertEquals(
        String.valueOf(TimeUnit.MINUTES.toMillis(60)),
        configMap.get(BigtableHBaseSettings.BULK_MUTATION_CLOSE_TIMEOUT_MILLISECONDS).get());
  }

  /**
   * Tests that {@link TemplateUtils#buildImportConfig} uses default value 30 when custom bulk
   * mutation timeout is null.
   */
  @Test
  public void testBuildImportConfig_nullBulkMutationTimeout() {
    ImportOptions opts = PipelineOptionsFactory.as(ImportOptions.class);
    opts.setBigtableProject(StaticValueProvider.of("my-project"));
    opts.setBigtableInstanceId(StaticValueProvider.of("my-instance"));
    opts.setBigtableTableId(StaticValueProvider.of("my-table"));
    opts.setBulkMutationCloseTimeoutMinutes(StaticValueProvider.<Integer>of(null));

    CloudBigtableTableConfiguration config = TemplateUtils.buildImportConfig(opts, "my-agent");

    Map<String, ValueProvider<String>> configMap = config.getConfiguration();
    assertEquals(
        String.valueOf(TimeUnit.MINUTES.toMillis(30)),
        configMap.get(BigtableHBaseSettings.BULK_MUTATION_CLOSE_TIMEOUT_MILLISECONDS).get());
  }
}
