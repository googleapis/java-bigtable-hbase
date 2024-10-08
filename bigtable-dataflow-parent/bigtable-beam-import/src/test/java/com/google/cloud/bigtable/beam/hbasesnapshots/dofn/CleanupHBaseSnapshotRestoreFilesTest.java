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
package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.HBaseSnapshotInputConfigBuilder;
import java.util.UUID;
import org.junit.Test;

public class CleanupHBaseSnapshotRestoreFilesTest {
  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final String TEST_SNAPSHOT_PATH = "gs://" + TEST_BUCKET_NAME + "/hbase-export";
  private static final String TEST_RESTORE_PATH =
      HBaseSnapshotInputConfigBuilder.RESTORE_DIR + UUID.randomUUID();
  private static final String TEST_RESTORE_PREFIX = TEST_RESTORE_PATH.substring(1);

  @Test
  public void testGetWorkingBucketName() {
    assertEquals(
        TEST_BUCKET_NAME,
        CleanupHBaseSnapshotRestoreFiles.getWorkingBucketName(TEST_SNAPSHOT_PATH));

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          CleanupHBaseSnapshotRestoreFiles.getWorkingBucketName(TEST_BUCKET_NAME);
        });
  }

  @Test
  public void testGetListPrefix() {
    assertEquals(
        TEST_RESTORE_PREFIX, CleanupHBaseSnapshotRestoreFiles.getListPrefix(TEST_RESTORE_PATH));

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          CleanupHBaseSnapshotRestoreFiles.getWorkingBucketName(TEST_RESTORE_PREFIX);
        });
  }
}
