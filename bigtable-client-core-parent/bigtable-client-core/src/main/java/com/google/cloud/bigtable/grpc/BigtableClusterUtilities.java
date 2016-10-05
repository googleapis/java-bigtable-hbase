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
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.common.base.Preconditions;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
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
  private final BigtableInstanceName instanceName;
  private final ChannelPool channelPool;
  private final BigtableInstanceClient client;

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
    channelPool = BigtableSession.createChannelPool(options.getInstanceAdminHost(), options);
    client = new BigtableInstanceGrpcClient(channelPool);
  }

  /**
   * Gets the serve node count of the cluster.
   * @param clusterId
   * @param zoneId
   * @return the {@link Cluster#getServeNodes()} of the clusterId.
   */
  public int getClusterSize(String clusterId, String zoneId) {
    String message = String.format("Cluster %s/%s was not found.", clusterId, zoneId);
    Cluster cluster = Preconditions.checkNotNull(getCluster(clusterId, zoneId), message);
    return cluster.getServeNodes();
  }

  /**
   * Gets a {@link ListClustersResponse} that contains all of the clusters for the
   * projectId/instanceId configuration.
   * @return the current state of the instance
   */
  public synchronized ListClustersResponse getClusters() {
    logger.info("Reading clusters.");
    return client.listCluster(
      ListClustersRequest.newBuilder().setParent(instanceName.getInstanceName()).build());
  }

  /**
   * Sets a cluster size to a specific size.
   * @param clusterId
   * @param zoneId
   * @param newSize
   * @throws InterruptedException
   */
  public synchronized void setClusterSize(String clusterId, String zoneId, int newSize)
      throws InterruptedException {
    Preconditions.checkArgument(newSize > 0, "Cluster size must be > 0");
    Cluster cluster = getCluster(clusterId, zoneId);
    int currentSize = cluster.getServeNodes();
    if (currentSize == newSize) {
      logger.info("Cluster %s already has %d nodes.", clusterId, newSize);
    } else {
      updateClusterSize(cluster.getName(), newSize);
    }
  }

  /**
   * @param clusterId
   * @param zoneId
   * @param incrementCount a positive or negative number to add to the current node count
   * @return the new size of the cluster.
   * @throws InterruptedException
   */
  public synchronized int incrementClusterSize(String clusterId, String zoneId, int incrementCount)
      throws InterruptedException {
    Preconditions.checkArgument(incrementCount != 0,
      "Cluster size cannot be incremented by 0 nodes. incrementCount has to be either positive or negative");
    Cluster cluster = getCluster(clusterId, zoneId);
    if (incrementCount > 0) {
      logger.info("Adding %d nodes to cluster %s", incrementCount, clusterId);
    } else {
      logger.info("Removing %d nodes from cluster %s", -incrementCount, clusterId);
    }
    int newSize = incrementCount + cluster.getServeNodes();
    updateClusterSize(cluster.getName(), newSize);
    return newSize;
  }

  /**
   * Update a cluster to have a specific size
   *
   * @param clusterName The fully qualified clusterName
   * @param newSize The size to update the cluster to.
   * @throws InterruptedException
   */
  private void updateClusterSize(String clusterName, int newSize) throws InterruptedException {
    logger.info("Updating cluster %s to size %d", clusterName, newSize);
    Operation operation = client
        .updateCluster(Cluster.newBuilder().setName(clusterName).setServeNodes(newSize).build());
    waitForOperation(operation.getName(), 60);
    logger.info("Done updating cluster %s.", clusterName);
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
  public synchronized int getClusterNodeCount(String clusterId, String zoneId) {
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
  public synchronized Cluster getCluster(String clusterId, String zoneId) {
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
   * @throws Exception
   */
  @Override
  public synchronized void close() throws Exception {
    channelPool.shutdownNow();
  }

  /**
   * @return The instance id associated with the given project, zone and cluster.
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
}
