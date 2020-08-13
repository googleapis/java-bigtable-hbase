/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.util;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.junit.Assert;
import org.junit.Test;

public class TestSnapshotDescriptionUtil {

  @Test
  public void testSnapshotDescription() {
    SnapshotDescription snapshotDescription =
        SnapshotDescription.newBuilder()
            .setName(
                "projects/fake-project-id/instances/fake-instance-id/clusters/fake-cluster-id/backups/fake-backup-id")
            .build();
    String snapshotId = SnapshotDescriptionUtil.getSnapshotId(snapshotDescription.getName());
    Assert.assertEquals("fake-backup-id", snapshotId);
  }

  @Test
  public void testInvalidSnapshot() {
    try {
      SnapshotDescription snapshotDescription =
          SnapshotDescription.newBuilder().setName("").build();
      SnapshotDescriptionUtil.getSnapshotId(snapshotDescription.getName());
      Assert.fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      SnapshotDescription missingBackup =
          SnapshotDescription.newBuilder()
              .setName(
                  "projects/fake-project-id/instances/fake-instance-id/clusters/fake-cluster-id/")
              .build();
      SnapshotDescriptionUtil.getSnapshotId(missingBackup.getName());
      Assert.fail();
    } catch (IllegalArgumentException expected) {
    }
  }
}
