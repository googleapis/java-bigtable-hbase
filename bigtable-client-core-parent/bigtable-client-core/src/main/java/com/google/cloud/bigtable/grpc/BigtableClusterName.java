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

import com.google.api.core.InternalApi;
import com.google.api.core.InternalExtensionOnly;
import com.google.common.base.Preconditions;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class encapsulates a cluster name of the form
 * projects/(projectId)/instances/(instanceId)/clusters/(clusterId)
 */
@InternalExtensionOnly
public class BigtableClusterName {
  // Use a very loose pattern so we don't validate more strictly than the server.
  private static final Pattern PATTERN =
      Pattern.compile("projects/[^/]+/instances/([^/]+)/clusters/([^/]+)");

  private final String clusterName;
  private final String instanceId;
  private final String clusterId;

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public BigtableClusterName(String clusterName) {
    this.clusterName = clusterName;
    Matcher matcher = PATTERN.matcher(clusterName);
    Preconditions.checkArgument(matcher.matches(), "Malformed cluster name");
    this.instanceId = matcher.group(1);
    this.clusterId = matcher.group(2);
  }

  /**
   * @return the fully qualified cluster name. This method returns the same result as {@link
   *     #getClusterName()}.
   */
  @Override
  public String toString() {
    return clusterName;
  }

  /**
   * @return The id of the instance that contains this cluster. It's the second group in the Cluster
   *     name: "projects/{projectId}/instances/{instanceId}/clusters/{clusterId}".
   */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * @return The name of this cluster. It will look like the following
   *     "projects/{projectId}/instances/{instanceId}/clusters/{clusterId}".
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * @return The id of this cluster. It's the third group in the Cluster name:
   *     "projects/{projectId}/instances/{instanceId}/clusters/{clusterId}".
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Create a fully qualified backup name based on the the clusterName and the backupId. Backup name
   * will look like:
   * "projects/{projectId}/instances/{instanceId}/clusters/{clusterId}/backups/{backupId}".
   *
   * @param backupId The id of the backup
   * @return A fully qualified backup name that contains the fully qualified cluster name as the
   *     parent and the backup name as the child.
   */
  public String toBackupName(String backupId) {
    return clusterName + "/backups/" + backupId;
  }
}
