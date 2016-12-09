/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import com.google.common.base.Preconditions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class encapsulates a Snapshot name of the form
 * projects/(projectId)/instances/(instanceId)/clusters/(clusterId)
 */
public class BigtableSnapshotName {
  // Use a very loose pattern so we don't validate more strictly than the server.
  private static final Pattern PATTERN =
      Pattern.compile("projects/[^/]+/instances/([^/]+)/clusters/([^/]+)/snapshots/([^/]+)");

  private final String snapshotName;
  private final String instanceId;
  private final String clusterId;
  private final String snapshotId;

  public BigtableSnapshotName(String snapshotName) {
    this.snapshotName = snapshotName;
    Matcher matcher = PATTERN.matcher(snapshotName);
    Preconditions.checkArgument(matcher.matches(), "Malformed snapshot name");
    this.instanceId = matcher.group(1);
    this.clusterId = matcher.group(2);
    this.snapshotId = matcher.group(3);
  }

  /**
   * Returns the fully qualified Snapshot name. This method returns the same result as
   * {@link #getSnapshotName()}.
   */
  @Override
  public String toString() {
    return snapshotName;
  }

  /**
   * @return The id of the instance that contains this cluster. It's the second group in the
   *         Snapshot name:
   *         "projects/{projectId}/instances/{instanceId}/clusters/{clusterId}/snapshots/{snapshotId}".
   */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * @return The id of this cluster. It's the third group in the Snapshot name:
   *         "projects/{projectId}/instances/{instanceId}/clusters/{clusterId}/snapshots/{snapshotId}".
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * @return The id of this snapshot. It's the fourth group in the Snapshot name:
   *         "projects/{projectId}/instances/{instanceId}/clusters/{clusterId}/snapshots/{snapshotId}".
   */
  public String getSnapshotId() {
    return snapshotId;
  }

  /**
   * @return The name of this snapshot. It will look like the following
   *         "projects/{projectId}/instances/{instanceId}/clusters/{clusterId}/snapshots/{snapshotId}".
   */
  public String getSnapshotName() {
    return snapshotName;
  }
}
