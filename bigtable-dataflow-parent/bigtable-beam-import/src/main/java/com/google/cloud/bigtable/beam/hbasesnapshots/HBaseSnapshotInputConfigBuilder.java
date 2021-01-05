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

import static java.lang.System.*;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * A {@link Configuration} that could be used in {@link HadoopFormatIO} for reading HBase snapshot
 * hosted in Google Cloud Storage(GCS) bucket via GCS connector. It uses {@link
 * TableSnapshotInputFormat} for reading HBase snapshots.
 */
class HBaseSnapshotInputConfigBuilder {

  private static final Log LOG = LogFactory.getLog(HBaseSnapshotInputConfigBuilder.class);
  private static final int BATCH_SIZE = 1000;

  private String projectId;
  private String exportedSnapshotDir;
  private String snapshotName;
  private String restoreDir;

  public HBaseSnapshotInputConfigBuilder() {}

  public HBaseSnapshotInputConfigBuilder setProjectId(String projectId) {
    this.projectId = projectId;
    return this;
  }

  public HBaseSnapshotInputConfigBuilder setExportedSnapshotDir(String exportedSnapshotDir) {
    this.exportedSnapshotDir = exportedSnapshotDir;
    return this;
  }

  public HBaseSnapshotInputConfigBuilder setSnapshotName(String snapshotName) {
    this.snapshotName = snapshotName;
    return this;
  }

  public HBaseSnapshotInputConfigBuilder setRestoreDir(String restoreDir) {
    this.restoreDir = restoreDir;
    return this;
  }

  public Configuration build() {
    Preconditions.checkNotNull(projectId);
    Preconditions.checkNotNull(exportedSnapshotDir);
    Preconditions.checkNotNull(snapshotName);
    Preconditions.checkArgument(
        exportedSnapshotDir.startsWith("gs://"), "snapshot folder must be hosted in a GCS bucket ");

    Configuration conf = HBaseConfiguration.create();
    try {
      conf.set("hbase.rootdir", exportedSnapshotDir);
      conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
      conf.set("fs.gs.project.id", projectId);
      conf.set("fs.defaultFS", exportedSnapshotDir);
      conf.set("google.cloud.auth.service.account.enable", "true");
      conf.setClass(
          "mapreduce.job.inputformat.class", TableSnapshotInputFormat.class, InputFormat.class);
      conf.setClass("key.class", ImmutableBytesWritable.class, Writable.class);
      conf.setClass("value.class", Result.class, Object.class);
      ClientProtos.Scan proto = ProtobufUtil.toScan(new Scan().setBatch(BATCH_SIZE));
      conf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()));

      // LOG.debug(conf);
      Job job = Job.getInstance(conf); // creates internal clone of hbaseConf
      TableSnapshotInputFormat.setInput(job, snapshotName, new Path(restoreDir));
      return job.getConfiguration(); // extract the modified clone
    } catch (Exception e) {
      LOG.fatal(e);
    }
    return conf;
  }
}
