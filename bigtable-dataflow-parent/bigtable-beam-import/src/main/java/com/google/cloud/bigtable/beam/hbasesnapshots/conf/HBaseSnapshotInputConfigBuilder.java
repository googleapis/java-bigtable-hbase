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
package com.google.cloud.bigtable.beam.hbasesnapshots.conf;

import com.google.common.base.Preconditions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * A {@link Configuration} that could be used in {@link HadoopFormatIO} for reading HBase snapshot
 * hosted in Google Cloud Storage(GCS) bucket via GCS connector. It uses {@link
 * TableSnapshotInputFormat} for reading HBase snapshots.
 */
public class HBaseSnapshotInputConfigBuilder {

  private static final Log LOG = LogFactory.getLog(HBaseSnapshotInputConfigBuilder.class);
  // Batch size used for HBase snapshot scans
  private static final int BATCH_SIZE = 1000;

  // a temp location to store metadata extracted from snapshot
  public static final String RESTORE_DIR = "/.restore";

  private String projectId;
  private String hbaseSnapshotSourceDir;
  private String snapshotName;
  private String restoreDirSuffix;

  public HBaseSnapshotInputConfigBuilder() {}

  /*
   * Set the project id use to access the GCS bucket with HBase snapshot data to be imported
   */
  public HBaseSnapshotInputConfigBuilder setProjectId(String projectId) {
    this.projectId = projectId;
    return this;
  }

  /*
   * Set the GCS path where the HBase snapshot data is located
   */
  public HBaseSnapshotInputConfigBuilder setHbaseSnapshotSourceDir(String hbaseSnapshotSourceDir) {
    this.hbaseSnapshotSourceDir = hbaseSnapshotSourceDir;
    return this;
  }

  /*
   * Set the name of the snapshot to be imported
   * e.g when importing snapshot 'gs://<your-gcs-path>/hbase-export/table_snapshot'
   * put 'table_snapshot' as the {@code snapshotName}
   * and 'gs://<your-gcs-path>/hbase-export' as {@code exportedSnapshotDir}
   */
  public HBaseSnapshotInputConfigBuilder setSnapshotName(String snapshotName) {
    this.snapshotName = snapshotName;
    return this;
  }

  /*
   * Set the unique suffix to be used for restore folder to avoid conflicts
   */
  public HBaseSnapshotInputConfigBuilder setRestoreDirSuffix(String suffix) {
    this.restoreDirSuffix = suffix;
    return this;
  }

  public String getRestoreDir() {
    return RESTORE_DIR + this.restoreDirSuffix;
  }

  public Configuration build() throws Exception {
    Preconditions.checkState(
        projectId != null && !projectId.isEmpty(), "Required value projectId must be set");
    Preconditions.checkState(
        hbaseSnapshotSourceDir != null && !hbaseSnapshotSourceDir.isEmpty(),
        "Required value hbaseSnapshotSourceDir must be set");
    Preconditions.checkState(
        snapshotName != null && !snapshotName.isEmpty(), "Required value snapshotName must be set");
    Preconditions.checkState(
        hbaseSnapshotSourceDir.startsWith(GcsPath.SCHEME),
        "Snapshot folder must be hosted in a GCS bucket");

    Configuration conf = createHBaseConfiguration();

    // Configuring a MapReduce Job base on HBaseConfiguration
    // and return the job Configuration
    conf.set(
        TableInputFormat.SCAN,
        TableMapReduceUtil.convertScanToString(new Scan().setBatch(BATCH_SIZE)));
    Job job = Job.getInstance(conf); // creates internal clone of hbaseConf
    // the restore folder need to under current bucket root so to be considered
    // within the same filesystem with the hbaseSnapshotSourceDir
    TableSnapshotInputFormat.setInput(job, snapshotName, new Path(getRestoreDir()));
    return job.getConfiguration(); // extract the modified clone
  }

  // separate static part for unit testing
  public Configuration createHBaseConfiguration() {
    Configuration conf = HBaseConfiguration.create();

    // Setup the input data location for HBase snapshot import
    // exportedSnapshotDir should be a GCS Bucket path.
    conf.set("hbase.rootdir", hbaseSnapshotSourceDir);
    conf.set("fs.defaultFS", hbaseSnapshotSourceDir);

    // Setup GCS connector to use GCS as Hadoop filesystem
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    conf.set("fs.gs.project.id", projectId);
    conf.setBoolean("google.cloud.auth.service.account.enable", true);

    // Setup MapReduce config for TableSnapshotInputFormat
    conf.setClass(
        "mapreduce.job.inputformat.class", TableSnapshotInputFormat.class, InputFormat.class);
    conf.setClass("key.class", ImmutableBytesWritable.class, Writable.class);
    conf.setClass("value.class", Result.class, Object.class);
    return conf;
  }
}
