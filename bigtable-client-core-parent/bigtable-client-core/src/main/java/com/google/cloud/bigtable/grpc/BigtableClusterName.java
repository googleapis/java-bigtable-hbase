package com.google.cloud.bigtable.grpc;

import com.google.common.base.Preconditions;

/**
 * This class encapsulates a cluster name of the form
 * projects/(projectId)/instances/(instanceId)/clusters/(clusterId)
 */
public class BigtableClusterName {
  private final String clusterName;

  public BigtableClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  @Override
  public String toString() {
    return clusterName;
  }

  /**
   * @return The id of the instance that contains this cluster.
   */
  public String getInstanceId() {
    String[] parts = clusterName.split("/");
    Preconditions.checkState(parts.length == 6, "Malformed cluster name: " + clusterName);
    return parts[3];
  }
}
