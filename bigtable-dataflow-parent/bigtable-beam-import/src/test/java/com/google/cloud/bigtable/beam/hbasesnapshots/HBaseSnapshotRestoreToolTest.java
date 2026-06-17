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
package com.google.cloud.bigtable.beam.hbasesnapshots;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.ImportConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HBaseSnapshotRestoreToolTest {

  private Map<String, String> originalProperties = new HashMap<>();

  @Before
  public void setUp() {
    originalProperties.put("project", System.getProperty("project"));
    originalProperties.put("hbaseSnapshotSourceDir", System.getProperty("hbaseSnapshotSourceDir"));
    originalProperties.put("snapshots", System.getProperty("snapshots"));
    originalProperties.put("restorePath", System.getProperty("restorePath"));
  }

  @After
  public void tearDown() {
    for (Map.Entry<String, String> entry : originalProperties.entrySet()) {
      if (entry.getValue() != null) {
        System.setProperty(entry.getKey(), entry.getValue());
      } else {
        System.clearProperty(entry.getKey());
      }
    }
  }

  /**
   * Tests that {@link HBaseSnapshotRestoreTool#buildImportConfigFromArgs} correctly parses system
   * properties into an {@link ImportConfig}.
   */
  @Test
  public void testBuildImportConfigFromArgs() throws Exception {
    System.setProperty("project", "my-project");
    System.setProperty("hbaseSnapshotSourceDir", "gs://my-bucket/snapshots");
    System.setProperty("snapshots", "snap1:table1");
    System.setProperty("restorePath", "gs://my-bucket/restore");

    GcsOptions options = PipelineOptionsFactory.create().as(GcsOptions.class);
    ImportConfig config = HBaseSnapshotRestoreTool.buildImportConfigFromArgs(options);

    assertEquals("gs://my-bucket/snapshots", config.getSourcepath());
    assertEquals("gs://my-bucket/restore", config.getRestorepath());
    assertEquals(1, config.getSnapshots().size());
    assertEquals("snap1", config.getSnapshots().get(0).getSnapshotName());
    assertEquals("table1", config.getSnapshots().get(0).getbigtableTableName());
  }
}
