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
import static org.junit.Assert.assertNull;

import com.google.cloud.bigtable.beam.sequencefiles.ExportJob.ExportOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class TemplateUtilsTest {

  @Test
  public void testBuildExportConfig_propagatesMaxAttempts() {
    ExportOptions options = Mockito.mock(ExportOptions.class);
    Mockito.when(options.getBigtableProject()).thenReturn(StaticValueProvider.of("my-project"));
    Mockito.when(options.getBigtableInstanceId()).thenReturn(StaticValueProvider.of("my-instance"));
    Mockito.when(options.getBigtableTableId()).thenReturn(StaticValueProvider.of("my-table"));
    Mockito.when(options.getBigtableMaxAttempts()).thenReturn(StaticValueProvider.of("15"));
    Mockito.when(options.getBigtableAppProfileId())
        .thenReturn(StaticValueProvider.of("my-app-profile"));

    CloudBigtableScanConfiguration config = TemplateUtils.buildExportConfig(options);
    Configuration hbaseConfig = config.toHBaseConfig();
    assertEquals("15", hbaseConfig.get(BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES));
  }

  @Test
  public void testBuildExportConfig_defaultMaxAttempts() {
    ExportOptions options = Mockito.mock(ExportOptions.class);
    Mockito.when(options.getBigtableProject()).thenReturn(StaticValueProvider.of("my-project"));
    Mockito.when(options.getBigtableInstanceId()).thenReturn(StaticValueProvider.of("my-instance"));
    Mockito.when(options.getBigtableTableId()).thenReturn(StaticValueProvider.of("my-table"));
    Mockito.when(options.getBigtableAppProfileId())
        .thenReturn(StaticValueProvider.of((String) null));
    Mockito.when(options.getBigtableMaxAttempts()).thenReturn(null);

    CloudBigtableScanConfiguration config = TemplateUtils.buildExportConfig(options);
    Configuration hbaseConfig = config.toHBaseConfig();
    assertNull(hbaseConfig.get(BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES));
  }
}
