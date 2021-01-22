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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CleanupHBaseSnapshotRestoreFilesFn extends DoFn<KV<String, String>, Boolean> {
  private static final Log LOG = LogFactory.getLog(CleanupHBaseSnapshotRestoreFilesFn.class);

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    KV<String, String> elem = context.element();

    String hbaseSnapshotDir = elem.getKey();
    String restorePath = elem.getValue();
    String restoreDir = getRestoreDir(hbaseSnapshotDir, restorePath);
    List<ResourceId> paths =
        FileSystems.match(restoreDir + "**").metadata().stream()
            .map(metadata -> metadata.resourceId())
            .collect(Collectors.toList());
    FileSystems.delete(paths, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
    FileSystems.delete(
        Collections.singletonList(FileSystems.matchSingleFileSpec(restoreDir).resourceId()),
        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
    context.output(true);
  }

  public static String getRestoreDir(String hbaseSnapshotDir, String restoreDir) {
    Preconditions.checkState(
        hbaseSnapshotDir.startsWith("gs://"), "snapshot folder must be hosted in a GCS bucket ");
    Preconditions.checkState(
        restoreDir.startsWith("/"),
        "restore folder must be an absolute path in current filesystem");
    int bucketNameEndIndex = hbaseSnapshotDir.indexOf('/', 5); // "offset gs://"
    String bucketName;
    if (bucketNameEndIndex > 0) {
      bucketName = hbaseSnapshotDir.substring(0, bucketNameEndIndex);
    } else {
      bucketName = hbaseSnapshotDir;
    }

    return String.format("%s%s", bucketName, restoreDir);
  }
}
