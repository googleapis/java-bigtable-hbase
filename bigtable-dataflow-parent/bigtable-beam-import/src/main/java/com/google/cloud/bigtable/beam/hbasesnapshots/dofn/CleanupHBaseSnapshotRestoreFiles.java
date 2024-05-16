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
import com.google.api.services.storage.model.Objects;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link DoFn} that could be used for cleaning up temp files generated during HBase snapshot
 * scans in Google Cloud Storage(GCS) bucket via GCS connector.
 */
@InternalApi("For internal usage only")
public class CleanupHBaseSnapshotRestoreFiles extends DoFn<KV<String, String>, Boolean> {
  private static final Log LOG = LogFactory.getLog(CleanupHBaseSnapshotRestoreFiles.class);

  public static String getWorkingBucketName(String hbaseSnapshotDir) {
    Preconditions.checkArgument(
        hbaseSnapshotDir.startsWith(GcsPath.SCHEME),
        "snapshot folder must be hosted in a GCS bucket ");

    return GcsPath.fromUri(hbaseSnapshotDir).getBucket();
  }

  // getListPrefix convert absolute restorePath in a Hadoop filesystem
  // to a match prefix in a GCS bucket
  public static String getListPrefix(String restorePath) {
    Preconditions.checkArgument(
        restorePath.startsWith("/"),
        "restore folder must be an absolute path in current filesystem");
    return restorePath.substring(1);
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    KV<String, String> elem = context.element();

    String hbaseSnapshotDir = elem.getKey();
    String restorePath = elem.getValue();
    String prefix = getListPrefix(restorePath);
    String bucketName = getWorkingBucketName(hbaseSnapshotDir);
    Preconditions.checkState(
        !prefix.isEmpty() && !hbaseSnapshotDir.contains(String.format("%s/%s", bucketName, prefix)),
        "restore folder should not be empty or a subfolder of hbaseSnapshotSourceDir");
    GcpOptions gcpOptions = context.getPipelineOptions().as(GcpOptions.class);
    GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(gcpOptions);

    String pageToken = null;
    List<String> results = new ArrayList<>();
    do {
      Objects objects = gcsUtil.listObjects(bucketName, prefix, pageToken);
      if (objects.getItems() == null) {
        break;
      }

      objects.getItems().stream()
          .map(storageObject -> GcsPath.fromObject(storageObject).toString())
          .forEach(results::add);
      pageToken = objects.getNextPageToken();
    } while (pageToken != null);
    gcsUtil.remove(results);
    context.output(true);
  }
}
