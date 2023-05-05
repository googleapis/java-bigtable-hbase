package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import com.google.cloud.bigtable.beam.hbasesnapshots.SnapshotTestHelper;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests the {@link RestoreSnapshot} functionality. */
@RunWith(JUnit4.class)
public class RestoreSnapshotTest {

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testRestoreSnapshot() throws Exception {

    SnapshotConfig snapshotConfig =
        SnapshotTestHelper.newSnapshotConfig(
            tempFolder.newFolder().getAbsolutePath(), tempFolder.newFolder().getAbsolutePath());

    try (MockedStatic<RestoreSnapshotHelper> restoreSnapshotHelper =
        Mockito.mockStatic(RestoreSnapshotHelper.class)) {
      restoreSnapshotHelper
          .when(
              () ->
                  RestoreSnapshotHelper.copySnapshotForScanner(
                      snapshotConfig.getConfiguration(),
                      null,
                      snapshotConfig.getSourcePath(),
                      snapshotConfig.getRestorePath(),
                      snapshotConfig.getSnapshotName()))
          .thenReturn(null);

      new RestoreSnapshot().restoreSnapshot(snapshotConfig);
    }
  }
}
