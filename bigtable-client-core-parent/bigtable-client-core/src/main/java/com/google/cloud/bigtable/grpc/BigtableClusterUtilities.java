/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.ListClustersRequest;
import com.google.bigtable.admin.v2.ListClustersResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

/**
 * This is a utility that can be used to resize a cluster. This is useful to use 20 minutes before a
 * large job to increase Cloud Bigtable capacity and 20 minutes after a large batch job to reduce
 * the size.
 */
public class BigtableClusterUtilities implements AutoCloseable {
  private static Logger logger = new Logger(BigtableClusterUtilities.class);

  /**
   * Creates a {@link BigtableClusterUtilities} for a projectId and an instanceId.
   *
   * @param projectId
   * @param instanceId
   *
   * @return a {@link BigtableClusterUtilities} for a specific projectId/instanceId.
   * @throws GeneralSecurityException if ssl configuration fails
   * @throws IOException if some aspect of the connection fails.
   */
  public static BigtableClusterUtilities forInstance(String projectId, String instanceId)
      throws IOException, GeneralSecurityException {
    return new BigtableClusterUtilities(
        new BigtableOptions.Builder().setProjectId(projectId).setInstanceId(instanceId).build());
  }

  /**
   * Creates a {@link BigtableClusterUtilities} for all instances in a projectId.
   *
   * @param projectId
   *
   * @return a {@link BigtableClusterUtilities} for a all instances in a projectId.
   * @throws GeneralSecurityException if ssl configuration fails
   * @throws IOException if some aspect of the connection fails.
   */
  public static BigtableClusterUtilities forAllInstances(String projectId)
      throws IOException, GeneralSecurityException {
    // '-' means all instanceids.
    return new BigtableClusterUtilities(
        new BigtableOptions.Builder().setProjectId(projectId).setInstanceId("-").build());
  }

  /**
   * @return The instance id associated with the given project, zone and cluster. We expect instance
   *         and cluster to have one-to-one relationship.
   *
   * @throws IllegalStateException if the cluster is not found
   */
  public static String lookupInstanceId(String projectId, String clusterId, String zoneId)
    throws IOException {
    BigtableClusterUtilities utils;
    try {
      utils = BigtableClusterUtilities.forAllInstances(projectId);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Could not initialize BigtableClusterUtilities", e);
    }

    try {
      Cluster cluster = utils.getCluster(clusterId, zoneId);
      return new BigtableClusterName(cluster.getName()).getInstanceId();
    } finally {
      try {
        utils.close();
      } catch (Exception e) {
        logger.warn("Error closing BigtableClusterUtilities: ", e);
      }
    }
  }

  /**
   * @return The cluster associated with the given project and instance. We expect instance and
   *         cluster to have one-to-one relationship.
   * @throws IllegalStateException if the cluster is not found or if there are many clusters in this
   *           instance.
   */
  public static Cluster lookupCluster(String projectId, String instanceId)
    throws IOException {
    BigtableClusterUtilities utils;
    try {
      utils = BigtableClusterUtilities.forInstance(projectId, instanceId);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Could not initialize BigtableClusterUtilities", e);
    }

    try {
      return utils.getSingleCluster();
    } finally {
      try {
        utils.close();
      } catch (Exception e) {
        logger.warn("Error closing BigtableClusterUtilities: ", e);
      }
    }
  }

  public static String getZoneId(Cluster cluster) {
    Preconditions.checkState(cluster != null, "Cluster doesn't exist");
    return getZoneId(cluster.getLocation());
  }

  @VisibleForTesting
  static String getZoneId(String name) {
    final String prefix = "/locations/";
    return name.substring(name.lastIndexOf(prefix) + prefix.length());
  }

  private final BigtableInstanceName instanceName;
  private final ManagedChannel channel;
  private final BigtableInstanceClient client;

  /**
   * Constructor for the utility. Prefer
   * {@link BigtableClusterUtilities#forInstance(String, String)} or
   * {@link BigtableClusterUtilities#forAllInstances(String)} rather than this method.
   * @param options that specify projectId, instanceId, credentials and retry options.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public BigtableClusterUtilities(final BigtableOptions options)
      throws IOException, GeneralSecurityException {
    this.instanceName =
        Preconditions.checkNotNull(
            options.getInstanceName(),
            "ProjectId and instanceId have to be set in the options.  Use '-' for all instanceIds.");
    channel = BigtableSession.createChannelPool(options.getInstanceAdminHost(), options);
    client = new BigtableInstanceGrpcClient(channel);
  }

  /**
   * Gets the serve node count of the cluster.
   * @param clusterId
   * @param zoneId
   * @return the {@link Cluster#getServeNodes()} of the clusterId.
   * @deprecated Use {@link #getCluster(String, String)} or {@link #getSingleCluster()} and then
   *             call {@link Cluster#getServeNodes()}.
   */
  @Deprecated
  public int getClusterSize(String clusterId, String zoneId) {
    Cluster cluster = getCluster(clusterId, zoneId);
    String message = String.format("Cluster %s/%s was not found.", clusterId, zoneId);
    Preconditions.checkNotNull(cluster, message);
    return cluster.getServeNodes();
  }

  /**
   * Gets the serve node count of an instance with a single cluster.
   * @return the {@link Cluster#getServeNodes()} of the clusterId.
   */
  public int getClusterSize() {
    return getSingleCluster().getServeNodes();
  }

  /**
   * Gets a {@link ListClustersResponse} that contains all of the clusters for the
   * projectId/instanceId configuration.
   * @return all clusters in the instance if the instance ID is provided; otherwise, all clusters in
   *         project are returned.
   */
  public ListClustersResponse getClusters() {
    logger.info("Reading clusters.");
    return client.listCluster(
      ListClustersRequest.newBuilder().setParent(instanceName.getInstanceName()).build());
  }

  /**
   * Sets a cluster size to a specific size.
   * @param clusterId
   * @param zoneId
   * @param newSize
   * @throws InterruptedException if the cluster is in the middle of updating, and an interrupt was
   *           received
   */
  public void setClusterSize(String clusterId, String zoneId, int newSize)
      throws InterruptedException {
    setClusterSize(instanceName.toClusterName(clusterId).getClusterName(), newSize);
  }

  /**
   * Sets a cluster size to a specific size in an instance with a single cluster
   * @throws InterruptedException if the cluster is in the middle of updating, and an interrupt was
   *           received
   */
  public void setClusterSize(int newSize) throws InterruptedException {
    setClusterSize(getSingleCluster().getName(), newSize);
  }

  /**
   * Update a specific cluster's server node count to the number specified
   */
  private void setClusterSize(String clusterName, int newSize)
      throws InterruptedException {
    Preconditions.checkArgument(newSize > 0, "Cluster size must be > 0");
    logger.info("Updating cluster %s to size %d", clusterName, newSize);
    Operation operation = client.updateCluster(Cluster.newBuilder()
        .setName(clusterName)
        .setServeNodes(newSize)
        .build());
    waitForOperation(operation.getName(), 60);
    logger.info("Done updating cluster %s.", clusterName);
  }

  /**
   * @return a Single Cluster for the project and instance.
   * @throws IllegalStateException for any project / instance combination that does not return
   *           exactly 1 cluster.
   */
  public Cluster getSingleCluster() {
    ListClustersResponse response = getClusters();
    Preconditions.checkState(response.getClustersCount() != 0, "The instance does not exist.");
    Preconditions.checkState(response.getClustersCount() == 1,
      "There can only be one cluster for this method to work.");
    return response.getClusters(0);
  }

  /**
   * Waits for an operation like cluster resizing to complete.
   * @param operationName The fully qualified name of the operation
   * @param maxSeconds The maximum amount of seconds to wait for the operation to complete.
   * @throws InterruptedException if a user interrupts the process, usually with a ^C.
   */
  public void waitForOperation(String operationName, int maxSeconds) throws InterruptedException {
    long endTimeMillis = TimeUnit.SECONDS.toMillis(maxSeconds) + System.currentTimeMillis();

    GetOperationRequest request = GetOperationRequest.newBuilder().setName(operationName).build();
    do {
      Thread.sleep(500);
      Operation response = client.getOperation(request);
      if (response.getDone()) {
        switch (response.getResultCase()) {
        case RESPONSE:
          return;
        case ERROR:
          throw new RuntimeException("Cluster could not be resized: " + response.getError());
        case RESULT_NOT_SET:
          throw new IllegalStateException(
              "System returned invalid response for Operation check: " + response);
        }
      }
    } while (System.currentTimeMillis() < endTimeMillis);

    throw new IllegalStateException(
        String.format("Waited %d seconds and cluster was not resized yet.", maxSeconds));
  }

  /**
   * Gets the current number of nodes allocated to the cluster.
   * @param clusterId
   * @param zoneId
   * @return the serveNode count of the cluster.
   */
  public int getClusterNodeCount(String clusterId, String zoneId) {
    return getCluster(clusterId, zoneId).getServeNodes();
  }

  /**
   * Gets the current configuration of the cluster as encapsulated by a {@link Cluster} object.
   *
   * @param clusterId
   * @param zoneId
   * @return the {@link Cluster} if it was set. If the cluster is not found, throw a {@link
   *     NullPointerException}.
   */
  public Cluster getCluster(String clusterId, String zoneId) {
    Cluster response = null;
    for (Cluster cluster : getClusters().getClustersList()) {
      if (cluster.getName().endsWith("/clusters/" + clusterId)
          && cluster.getLocation().endsWith("/locations/" + zoneId)) {
        if (response == null) {
          response = cluster;
        } else {
          throw new IllegalStateException(
              String.format("Got multiple clusters named %s in zone %z.", clusterId, zoneId));
        }
      }
    }
    return Preconditions.checkNotNull(response,
      String.format("Cluster %s in zone %s was not found.", clusterId, zoneId));
  }

  /**
   * Shuts down the connection to the admin API.
   */
  @Override
  public void close() {
    channel.shutdownNow();
  }
}
