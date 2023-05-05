/*
 * Copyright 2021 Google LLC
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
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.HBaseSnapshotInputConfigBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.Test;

public class HBaseSnapshotInputConfigBuilderTest {

  private static final String TEST_PROJECT = "test_project";
  private static final String TEST_SNAPSHOT_DIR = "gs://test-bucket/hbase-export";
  private static final String TEST_SNAPSHOT_NAME = "test_snapshot";

  @Test
  public void testBuildingHBaseSnapshotInputConfigBuilder() {
    Configuration conf =
        new HBaseSnapshotInputConfigBuilder()
            .setProjectId(TEST_PROJECT)
            .setHbaseSnapshotSourceDir(TEST_SNAPSHOT_DIR)
            .setSnapshotName(TEST_SNAPSHOT_NAME)
            .createHBaseConfiguration();
    assertEquals(
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS", conf.get("fs.AbstractFileSystem.gs.impl"));
    assertEquals(TEST_PROJECT, conf.get("fs.gs.project.id"));
    assertEquals(TEST_SNAPSHOT_DIR, conf.get("hbase.rootdir"));
    assertEquals(
        TableSnapshotInputFormat.class,
        conf.getClass(
            "mapreduce.job.inputformat.class", TableSnapshotInputFormat.class, InputFormat.class));
  }

  @Test
  public void testInvalidProjectHBaseSnapshotInputConfig() {
    try {
      new HBaseSnapshotInputConfigBuilder()
          .setSnapshotName(TEST_SNAPSHOT_NAME)
          .setHbaseSnapshotSourceDir(TEST_SNAPSHOT_DIR)
          .build();
      fail("Expected unset project to fail");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Required value projectId must be set");
    }

    try {
      new HBaseSnapshotInputConfigBuilder()
          .setProjectId("")
          .setSnapshotName(TEST_SNAPSHOT_NAME)
          .setHbaseSnapshotSourceDir(TEST_SNAPSHOT_DIR)
          .build();
      fail("Expected empty project to fail");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Required value projectId must be set");
    }
  }

  @Test
  public void testInvalidSnapshotHBaseSnapshotInputConfig() {
    try {
      new HBaseSnapshotInputConfigBuilder()
          .setProjectId(TEST_PROJECT)
          .setHbaseSnapshotSourceDir(TEST_SNAPSHOT_DIR)
          .build();
      fail("Expected unset snapshot name to fail");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Required value snapshotName must be set");
    }

    try {
      new HBaseSnapshotInputConfigBuilder()
          .setProjectId(TEST_PROJECT)
          .setSnapshotName("")
          .setHbaseSnapshotSourceDir(TEST_SNAPSHOT_DIR)
          .build();
      fail("Expected empty snapshot name to fail");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Required value snapshotName must be set");
    }
  }

  @Test
  public void testInvalidSourceDirHBaseSnapshotInputConfig() {
    try {
      new HBaseSnapshotInputConfigBuilder()
          .setProjectId(TEST_PROJECT)
          .setSnapshotName(TEST_SNAPSHOT_NAME)
          .build();
      fail("Expected unset snapshot directory to fail");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Required value hbaseSnapshotSourceDir must be set");
    }

    try {
      new HBaseSnapshotInputConfigBuilder()
          .setProjectId(TEST_PROJECT)
          .setSnapshotName(TEST_SNAPSHOT_NAME)
          .setHbaseSnapshotSourceDir("")
          .build();
      fail("Expected empty snapshot directory to fail");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Required value hbaseSnapshotSourceDir must be set");
    }

    try {
      new HBaseSnapshotInputConfigBuilder()
          .setProjectId(TEST_PROJECT)
          .setSnapshotName(TEST_SNAPSHOT_NAME)
          .setHbaseSnapshotSourceDir("test-bucket/hbase-export")
          .build();
      fail("Expected snapshot directory without gs prefix to fail");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Snapshot folder must be hosted in a GCS bucket");
    }
  }
}
