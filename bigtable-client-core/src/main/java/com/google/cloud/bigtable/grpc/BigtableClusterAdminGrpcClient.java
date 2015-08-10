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

import com.google.bigtable.admin.cluster.v1.BigtableClusterServiceGrpc;
import com.google.bigtable.admin.cluster.v1.Cluster;
import com.google.bigtable.admin.cluster.v1.CreateClusterRequest;
import com.google.bigtable.admin.cluster.v1.DeleteClusterRequest;
import com.google.bigtable.admin.cluster.v1.GetClusterRequest;
import com.google.bigtable.admin.cluster.v1.ListClustersRequest;
import com.google.bigtable.admin.cluster.v1.ListClustersResponse;
import com.google.bigtable.admin.cluster.v1.ListZonesRequest;
import com.google.bigtable.admin.cluster.v1.ListZonesResponse;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsGrpc;
import com.google.longrunning.OperationsGrpc.OperationsBlockingStub;

import io.grpc.Channel;

/**
 * A gRPC client for accessing the Bigtable Cluster Admin API.
 */
public class BigtableClusterAdminGrpcClient implements BigtableClusterAdminClient{

  private final BigtableClusterServiceGrpc.BigtableClusterServiceBlockingStub blockingStub;
  private final OperationsBlockingStub operationsStub;

  public BigtableClusterAdminGrpcClient(Channel channel) {
    blockingStub = BigtableClusterServiceGrpc.newBlockingStub(channel);
    operationsStub = OperationsGrpc.newBlockingStub(channel);
  }

  @Override
  public ListZonesResponse listZones(ListZonesRequest request) {
    return blockingStub.listZones(request);
  }

  @Override
  public ListClustersResponse listClusters(ListClustersRequest request) {
    return blockingStub.listClusters(request);
  }

  @Override
  public Cluster getCluster(GetClusterRequest request) {
    return blockingStub.getCluster(request);
  }

  @Override
  public Operation getOperation(GetOperationRequest request) {
    return operationsStub.getOperation(request);
  }

  @Override
  public Cluster createCluster(CreateClusterRequest request) {
    return blockingStub.createCluster(request);
  }

  @Override
  public Cluster updateCluster(Cluster cluster) {
    return blockingStub.updateCluster(cluster);
  }

  @Override
  public void deleteCluster(DeleteClusterRequest request) {
    blockingStub.deleteCluster(request);
  }
  
  @Override
  public void close() throws Exception {
  }

}
