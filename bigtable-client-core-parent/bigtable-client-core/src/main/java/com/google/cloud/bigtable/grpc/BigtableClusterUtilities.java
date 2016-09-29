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

/**
 * This is a utility to that can be used to resize a cluster. This is useful to use 20 minutes
 * before a large job to increase Cloud Bigtable capacity and 20 minutes after a large batch job to
 * reduce the size.
 */
public class BigtableClusterUtilities implements AutoCloseable {
  private static Logger logger = new Logger(BigtableClusterUtilities.class);
  private final BigtableInstanceName instanceName;
  private final ChannelPool channelPool;
  private final BigtableInstanceClient client;

  /**
   * Constructor for the utility.
   *
   * @param projectId The projectId string
   * @param instanceId The instanceId. Could be '-' for all instances.
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public BigtableClusterUtilities(String projectId, String instanceId)
      throws IOException, GeneralSecurityException {
    this(new BigtableOptions.Builder().setProjectId(projectId).setInstanceId(instanceId).build());
  }

  /**
   * Constructor for the utility.
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
   * Gets the server node count of the cluster.
   * @param clusterId
   * @param zoneId
   * @return the {@link Cluster#getServeNodes()} of the clusterId.
   */
  public int getClusterSize(String clusterId, String zoneId) {
    Cluster cluster =
        Preconditions.checkNotNull(
            getCluster(clusterId, zoneId), "Cluster " + clusterId + " was not found");
    return cluster.getServeNodes();
  }

  /**
   * Gets a {@link ListClustersResponse} that contains all of the clusters in this instance.
   *
   * @return the current state of the instance
   */
  public synchronized ListClustersResponse getClusters() {
    logger.info("Reading clusters.");
    String instanceNameStr = instanceName.getInstanceName();
    ListClustersRequest listRequest =
        ListClustersRequest.newBuilder().setParent(instanceNameStr).build();
    return client.listCluster(listRequest);
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
    waitForOperation(operation.getName(), 30);
    logger.info("Done updating cluster %s.", clusterName);
  }

  /**
   * Waits for an operation like cluster resizing to complete.
   * @param operationName The fully qualified name of the operation
   * @param maxSeconds The maximum amount of seconds to wait for the operation to complete.
   * @throws InterruptedException if a user interrupts the process, usually with a ^C.
   */
  private void waitForOperation(String operationName, int maxSeconds) throws InterruptedException {
    GetOperationRequest request = GetOperationRequest.newBuilder().setName(operationName).build();
    for (int i = 0; i < maxSeconds * 2; i++) {
      Thread.sleep(500);
      Operation response = client.getOperation(request);
      if (response.getDone()) {
        switch (response.getResultCase()) {
        case ERROR:
          throw new RuntimeException("Cluster could not be created: " + response.getError());
        case RESPONSE:
          return;
        case RESULT_NOT_SET:
          throw new IllegalStateException(
              "System returned invalid response for Operation check: " + response);
        }
      }
    }
    throw new IllegalStateException(
        String.format("Waited %d seconds and operation was not complete", maxSeconds));
  }

  /**
   * Gets the current number of nodes allocated to the cluster.
   * @param clusterId
   * @param zoneId
   * @return the serverNode of the cluster.
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
              String.format("Got multiple clusters named %s in the %z zone.", clusterId, zoneId));
        }
      }
    }
    return Preconditions.checkNotNull(response, "Cluster " + clusterId + " was not found");
  }

  /**
   * Shuts down the connection to the admin API.
   * @throws Exception
   */
  @Override
  public synchronized void close() throws Exception {
    channelPool.shutdownNow();
  }
}
