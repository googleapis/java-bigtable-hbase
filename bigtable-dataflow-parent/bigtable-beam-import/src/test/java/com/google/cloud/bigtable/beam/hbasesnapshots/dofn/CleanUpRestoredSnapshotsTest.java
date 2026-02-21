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
import java.io.File;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests the {@link CleanupRestoredSnapshots} functionality. */
@RunWith(JUnit4.class)
public class CleanUpRestoredSnapshotsTest {
  private static final Logger LOG = LoggerFactory.getLogger(CleanUpRestoredSnapshotsTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testDeleteRestoredSnapshot() throws Exception {
    File restoreDir = tempFolder.newFolder();
    if (restoreDir.exists()) {
      LOG.info("Created temp folder: {}", restoreDir.getAbsolutePath());
      SnapshotConfig snapshotConfig =
          SnapshotTestHelper.newSnapshotConfig(restoreDir.getAbsolutePath());
      new CleanupRestoredSnapshots().cleanupSnapshot(snapshotConfig);
      Assert.assertFalse(restoreDir.exists());
    } else {
      LOG.warn(
          "Skipping CleanUpRestoredSnapshotsTest since temporary file was unable to be created in restore path: {}",
          restoreDir.getAbsolutePath());
    }
  }

  /**
   * Tests CleanupRestoredSnapshots with invalid path to verify exception is handled internally
   *
   * @throws Exception
   */
  @Test
  public void testDeleteRestoredSnapshotWithInvalidPath() throws Exception {
    pipeline
        .apply("CreateInput", Create.of(SnapshotTestHelper.newSnapshotConfig("invalid_path")))
        .apply("DeleteSnapshot", ParDo.of(new CleanupRestoredSnapshots()));
    pipeline.run();
  }
}
