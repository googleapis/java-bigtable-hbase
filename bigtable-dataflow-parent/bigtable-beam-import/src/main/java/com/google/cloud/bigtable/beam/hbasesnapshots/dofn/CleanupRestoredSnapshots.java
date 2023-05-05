package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} for cleaning up files from restore path generated during job run. */
@InternalApi("For internal usage only")
public class CleanupRestoredSnapshots extends DoFn<SnapshotConfig, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(CleanupRestoredSnapshots.class);

  @ProcessElement
  public void processElement(
      @Element SnapshotConfig snapshotConfig, OutputReceiver<Void> outputReceiver)
      throws IOException {
    try {
      cleanupSnapshot(snapshotConfig);
    } catch (Exception ex) {
      LOG.error(
          "Exception: {}\n StackTrace:{}", ex.getMessage(), Arrays.toString(ex.getStackTrace()));
    }
  }

  /**
   * Removes the snapshot files from restore path.
   *
   * @param snapshotConfig - Snapshot Configuration
   * @throws IOException
   */
  @VisibleForTesting
  void cleanupSnapshot(SnapshotConfig snapshotConfig) throws IOException {
    Path restorePath = snapshotConfig.getRestorePath();
    Configuration configuration = snapshotConfig.getConfiguration();
    FileSystem fileSystem = restorePath.getFileSystem(configuration);
    fileSystem.delete(restorePath, true);
  }
}
