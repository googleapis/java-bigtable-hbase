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
