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
 * before and after a large batch job.
 */
public class BigtableClusterUtilities implements AutoCloseable {
  private static Logger logger = new Logger(BigtableClusterUtilities.class);
  private final BigtableInstanceName instanceName;
  private final ChannelPool channelPool;
  private final BigtableInstanceClient client;
  private ListClustersResponse clusters;

  /**
   * Constructor for the utility.
   * @param instanceName The {@link BigtableInstanceName} for the instance for the cluster to manage.
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public BigtableClusterUtilities(BigtableInstanceName instanceName)
      throws IOException, GeneralSecurityException {
    this(new BigtableOptions.Builder().setProjectId(instanceName.getProjectId())
        .setInstanceId(instanceName.getInstanceId()).build());
  }

  /**
   * Constructor for the utility.
   * @param options that specify projectId, instanceId, credentials and retry options.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public BigtableClusterUtilities(final BigtableOptions options)
      throws IOException, GeneralSecurityException {
    this.instanceName = options.getInstanceName();
    channelPool = BigtableSession.createChannelPool(options.getInstanceAdminHost(), options);
    client = new BigtableInstanceGrpcClient(channelPool);
  }

  /**
   * Gets the size of the cluster
   * @param clusterId
   * @return the {@link Cluster#getServeNodes()} of the clusterId.
   */
  public int getClusterSize(String clusterId) {
    Cluster cluster = Preconditions.checkNotNull(getCluster(clusterId),
      "Cluster " + clusterId + " was not found");
    return cluster.getServeNodes();
  }

  /**
   * Gets a {@link ListClustersResponse} that contains all of the clusters in this instance.
   * @return
   */
  public synchronized ListClustersResponse getClusters() {
    if (this.clusters == null) {
      logger.info("Reading clusters.");
      String instanceNameStr = instanceName.getInstanceName();
      ListClustersRequest listRequest =
          ListClustersRequest.newBuilder().setParent(instanceNameStr).build();
      this.clusters = client.listCluster(listRequest);
    }
    return this.clusters;
  }

  /**
   * Sets a cluster size to a specific size.
   * @param clusterId
   * @param newSize
   * @throws InterruptedException
   */
  public synchronized void setClusterSize(String clusterId, int newSize)
      throws InterruptedException {
    Preconditions.checkArgument(newSize > 0, "Cluster size must be > 0");
    Cluster cluster = getCluster(clusterId);
    int currentSize = cluster.getServeNodes();
    if (currentSize == newSize) {
      logger.info("Cluster %s already has %d nodes.", clusterId, newSize);
    } else {
      updateClusterSize(cluster.getName(), newSize);
    }
  }

  /**
   * @param clusterId
   * @param incrementCount a positive or negative number to add to the current node count
   * @return the new size of the cluster.
   * @throws InterruptedException
   */
  public synchronized int incrementClusterSize(String clusterId, int incrementCount)
      throws InterruptedException {
    Preconditions.checkArgument(incrementCount != 0,
      "Cluster size cannot be incremented by 0 nodes. incrementCount has to be either positive or negative");
    Cluster cluster = getCluster(clusterId);
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
    this.clusters = null;
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
   * @return
   */
  public synchronized int getClusterNodeCount(String clusterId) {
    return getCluster(clusterId).getServeNodes();
  }

  /**
   * Gets the current configuration of the cluster as encapsulated by a {@link Cluster} object.
   * @param clusterId
   * @return
   */
  public synchronized Cluster getCluster(String clusterId) {
    for (Cluster cluster : getClusters().getClustersList()) {
      if (cluster.getName().endsWith("/clusters/" + clusterId)){
        return cluster;
      }
    }
    throw new IllegalArgumentException("Cluster " + clusterId + " was not found");
  }

  /**
   * Shuts down the connection to the admin API.
   * @throws Exception
   */
  @Override
  public synchronized void close() throws Exception {
    clusters = null;
    channelPool.shutdownNow();
  }
}
