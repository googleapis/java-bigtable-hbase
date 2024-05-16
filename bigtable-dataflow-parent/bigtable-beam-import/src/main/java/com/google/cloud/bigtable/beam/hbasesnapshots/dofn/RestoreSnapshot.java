/*
 * Copyright 2024 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} for preprocessing the Snapshot files to restore HLinks & References prior to
 * reading the snapshot.
 */
@InternalApi("For internal usage only")
public class RestoreSnapshot extends DoFn<SnapshotConfig, SnapshotConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(RestoreSnapshot.class);

  @ProcessElement
  public void processElement(
      @Element SnapshotConfig snapshotConfig, OutputReceiver<SnapshotConfig> outputReceiver)
      throws IOException {
    restoreSnapshot(snapshotConfig);
    outputReceiver.output(snapshotConfig);
  }

  /**
   * Creates a copy of Snasphsot from the source path into restore path.
   *
   * @param snapshotConfig - Snapshot Configuration
   * @throws IOException
   */
  void restoreSnapshot(SnapshotConfig snapshotConfig) throws IOException {
    Path sourcePath = snapshotConfig.getSourcePath();
    Path restorePath = snapshotConfig.getRestorePath();
    Configuration configuration = snapshotConfig.getConfiguration();
    LOG.info("RestoreSnapshot - sourcePath:{} restorePath: {}", sourcePath, restorePath);
    FileSystem fileSystem = sourcePath.getFileSystem(configuration);
    RestoreSnapshotHelper.copySnapshotForScanner(
        configuration, fileSystem, sourcePath, restorePath, snapshotConfig.getSnapshotName());
  }
}
