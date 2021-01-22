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

import org.junit.Test;

public class CleanupHBaseSnapshotRestoreFilesFnTest {
  private static final String TEST_BUCKET_PATH = "gs://test-bucket";
  private static final String TEST_SNAPSHOT_PATH = TEST_BUCKET_PATH + "/hbase-export";
  private static final String TEST_RESTORE_PATH = HBaseSnapshotInputConfigBuilder.RESTORE_DIR;

  @Test
  public void testGetRestorePath() {
    assertEquals(
        "gs://test-bucket" + TEST_RESTORE_PATH + '/',
        CleanupHBaseSnapshotRestoreFilesFn.getRestoreDir(TEST_SNAPSHOT_PATH, TEST_RESTORE_PATH));

    assertEquals(
        "gs://test-bucket" + TEST_RESTORE_PATH + '/',
        CleanupHBaseSnapshotRestoreFilesFn.getRestoreDir(
            TEST_SNAPSHOT_PATH + '/', TEST_RESTORE_PATH));

    // These are not valid case as one could not use bucket root as hbase snapshot folder.
    assertEquals(
        "gs://test-bucket" + TEST_RESTORE_PATH + '/',
        CleanupHBaseSnapshotRestoreFilesFn.getRestoreDir(
            TEST_BUCKET_PATH + '/', TEST_RESTORE_PATH));

    assertEquals(
        "gs://test-bucket" + TEST_RESTORE_PATH + '/',
        CleanupHBaseSnapshotRestoreFilesFn.getRestoreDir(TEST_BUCKET_PATH, TEST_RESTORE_PATH));
  }
}
