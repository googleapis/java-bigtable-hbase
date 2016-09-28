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
import com.google.cloud.bigtable.grpc.io.CredentialInterceptorCache;
import com.google.cloud.bigtable.grpc.io.HeaderInterceptor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.security.GeneralSecurityException;

/** Sizes a Cluster to a specific node count. */
public class BigtableClusterUtilities implements AutoCloseable {
  private static final BigtableOptions DEFAULT_OPTIONS = new BigtableOptions.Builder().build();
  private static Logger logger = new Logger(BigtableClusterUtilities.class);
  private final BigtableInstanceName instanceName;
  private final ChannelPool channelPool;
  private final BigtableInstanceClient client;
  private ListClustersResponse clusters;

  public BigtableClusterUtilities(BigtableInstanceName instanceName)
      throws IOException, GeneralSecurityException {
    this(instanceName, DEFAULT_OPTIONS);
  }

  /**
   * @param options that specify credentials or retry options.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public BigtableClusterUtilities(BigtableInstanceName instanceName, final BigtableOptions options)
      throws IOException, GeneralSecurityException {
    this.instanceName = instanceName;
    HeaderInterceptor interceptor =
        CredentialInterceptorCache.getInstance()
            .getCredentialsInterceptor(options.getCredentialOptions(), options.getRetryOptions());
    channelPool =
        new ChannelPool(
            ImmutableList.<HeaderInterceptor>of(interceptor),
            new ChannelPool.ChannelFactory() {
              @Override
              public ManagedChannel create() throws IOException {
                return BigtableSession.createNettyChannel(options.getInstanceAdminHost(), options);
              }
            });
    client = new BigtableInstanceGrpcClient(channelPool);
  }

  public int getClusterSize(String clusterId){
    Cluster cluster =
    Preconditions.checkNotNull(getCluster(clusterId), "Cluster " + clusterId + " was not found");
    return cluster.getServeNodes();
  }

  public synchronized ListClustersResponse getClusters() {
    if (this.clusters == null) {
      logger.info("Reading clusters."); 
      this.clusters =
          client.listCluster(
              ListClustersRequest.newBuilder().setParent(instanceName.getInstanceName()).build());
    }
    return this.clusters;
  }
 
  public synchronized void setClusterSize(String clusterId, int newSize)
      throws InterruptedException {
    Cluster cluster = getCluster(clusterId);
    updateClusterSize(cluster, newSize);
  }

  /**
   * @param clusterId
   * @param incrementCount a positive or negative number to add to the current node count
   * @return the new size of the cluster.
   * @throws InterruptedException
   */
  public synchronized int incrementClusterSize(String clusterId, int incrementCount)
      throws InterruptedException {
    Cluster cluster = getCluster(clusterId);
    int newSize = incrementCount + cluster.getServeNodes();
    updateClusterSize(cluster, newSize);
    return newSize;
  }

  private void updateClusterSize(Cluster cluster, int newSize) throws InterruptedException {
    logger.info("Updating cluster " + cluster.getName() + " to size: " + newSize); 
    Operation operation =
        client.updateCluster(
            Cluster.newBuilder().setName(cluster.getName()).setServeNodes(newSize).build());
    waitForOperation(operation.getName(), 30);
    logger.info("Done updating cluster " + cluster.getName() + "."); 
    this.clusters = null;
  }

  private void waitForOperation(String operationName,
      int maxSeconds) throws InterruptedException {
    GetOperationRequest request = GetOperationRequest.newBuilder().setName(operationName).build();
    for (int i = 0; i < maxSeconds; i++) {
      Thread.sleep(1000);
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
    throw new IllegalStateException(String.format(
      "Waited %d seconds and operation was not complete", maxSeconds));
  }

  public synchronized int getClusterNodeCount(String clusterId) {
    return getCluster(clusterId).getServeNodes();
  }

  public synchronized Cluster getCluster(String clusterId) {
    for (Cluster cluster : getClusters().getClustersList()) {
      if (cluster.getName().endsWith("/clusters/" + clusterId)){
        return cluster;
      }
    }
    throw new IllegalArgumentException("Cluster " + clusterId + " was not found");
  }

  @Override
  public synchronized void close() throws Exception {
    clusters = null;
    channelPool.shutdownNow();
  }

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(
        args.length == 4,
        "Usage: BigtableClusterUtilities [projectId] [instanceId] [clusterId] [size]");
    BigtableInstanceName instanceName = new BigtableInstanceName(args[0], args[1]);
    try (BigtableClusterUtilities util = new BigtableClusterUtilities(instanceName)) {
      String clusterId = args[2];
      logger.info(
          "Size of "
              + clusterId
              + " before the resize: "
              + util.getCluster(clusterId).getServeNodes());
      Integer newSize = Integer.valueOf(args[3]);
      util.setClusterSize(clusterId, newSize);
      logger.info(
        "Size of "
            + clusterId
            + " after the resize: "
            + util.getCluster(clusterId).getServeNodes());
    }
  }
}
