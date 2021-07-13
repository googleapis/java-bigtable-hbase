/*
 * Copyright 2015 Google LLC
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
import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.BigtableInstanceAdminGrpc;
import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.CreateAppProfileRequest;
import com.google.bigtable.admin.v2.CreateClusterRequest;
import com.google.bigtable.admin.v2.CreateInstanceRequest;
import com.google.bigtable.admin.v2.DeleteAppProfileRequest;
import com.google.bigtable.admin.v2.DeleteClusterRequest;
import com.google.bigtable.admin.v2.DeleteInstanceRequest;
import com.google.bigtable.admin.v2.GetAppProfileRequest;
import com.google.bigtable.admin.v2.GetClusterRequest;
import com.google.bigtable.admin.v2.Instance;
import com.google.bigtable.admin.v2.ListAppProfilesRequest;
import com.google.bigtable.admin.v2.ListAppProfilesResponse;
import com.google.bigtable.admin.v2.ListClustersRequest;
import com.google.bigtable.admin.v2.ListClustersResponse;
import com.google.bigtable.admin.v2.ListInstancesRequest;
import com.google.bigtable.admin.v2.ListInstancesResponse;
import com.google.bigtable.admin.v2.PartialUpdateInstanceRequest;
import com.google.bigtable.admin.v2.UpdateAppProfileRequest;
import com.google.cloud.bigtable.util.OperationUtil;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsGrpc;
import com.google.protobuf.Empty;
import io.grpc.Channel;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * BigtableInstanceGrpcClient class.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableInstanceGrpcClient implements BigtableInstanceClient {

  private final BigtableInstanceAdminGrpc.BigtableInstanceAdminBlockingStub instanceClient;
  private final OperationUtil operationUtil;

  /**
   * Constructor for BigtableInstanceGrpcClient.
   *
   * @param channel a {@link io.grpc.Channel} object.
   */
  public BigtableInstanceGrpcClient(Channel channel) {
    this.instanceClient = BigtableInstanceAdminGrpc.newBlockingStub(channel);
    operationUtil = new OperationUtil(OperationsGrpc.newBlockingStub(channel));
  }

  /** {@inheritDoc} */
  @Override
  public Operation createInstance(CreateInstanceRequest request) {
    return instanceClient.createInstance(request);
  }

  /** {@inheritDoc} */
  @Override
  public Operation getOperation(GetOperationRequest request) {
    return operationUtil.getOperation(request);
  }

  /** {@inheritDoc} */
  @Override
  public void waitForOperation(Operation operation) throws IOException, TimeoutException {
    operationUtil.waitForOperation(operation, 10, TimeUnit.MINUTES);
  }

  /** {@inheritDoc} */
  @Override
  public void waitForOperation(Operation operation, long timeout, TimeUnit timeUnit)
      throws TimeoutException, IOException {
    operationUtil.waitForOperation(operation, timeout, timeUnit);
  }

  /** {@inheritDoc} */
  @Override
  public ListInstancesResponse listInstances(ListInstancesRequest request) {
    return instanceClient.listInstances(request);
  }

  /** {@inheritDoc} */
  @Override
  public Instance updateInstance(Instance instance) {
    return instanceClient.updateInstance(instance);
  }

  /** {@inheritDoc} */
  @Override
  public Empty deleteInstance(DeleteInstanceRequest request) {
    return instanceClient.deleteInstance(request);
  }

  /** {@inheritDoc} */
  @Override
  public Cluster getCluster(GetClusterRequest request) {
    return instanceClient.getCluster(request);
  }

  /** {@inheritDoc} */
  @Override
  public ListClustersResponse listCluster(ListClustersRequest request) {
    return instanceClient.listClusters(request);
  }

  /** {@inheritDoc} */
  @Override
  public Operation updateCluster(Cluster cluster) {
    return instanceClient.updateCluster(cluster);
  }

  /** {@inheritDoc} */
  @Override
  public Empty deleteCluster(DeleteClusterRequest request) {
    return instanceClient.deleteCluster(request);
  }

  /** {@inheritDoc} */
  @Override
  public Operation partialUpdateInstance(PartialUpdateInstanceRequest request) {
    return instanceClient.partialUpdateInstance(request);
  }

  /** {@inheritDoc} */
  @Override
  public AppProfile createAppProfile(CreateAppProfileRequest request) {
    return instanceClient.createAppProfile(request);
  }

  /** {@inheritDoc} */
  @Override
  public AppProfile getAppProfile(GetAppProfileRequest request) {
    return instanceClient.getAppProfile(request);
  }

  /** {@inheritDoc} */
  @Override
  public ListAppProfilesResponse listAppProfiles(ListAppProfilesRequest request) {
    return instanceClient.listAppProfiles(request);
  }

  /** {@inheritDoc} */
  @Override
  public Operation updateAppProfile(UpdateAppProfileRequest request) {
    return instanceClient.updateAppProfile(request);
  }

  /** {@inheritDoc} */
  @Override
  public Empty deleteAppProfile(DeleteAppProfileRequest request) {
    return instanceClient.deleteAppProfile(request);
  }

  /** {@inheritDoc} */
  @Override
  public Policy getIamPolicy(GetIamPolicyRequest request) {
    return instanceClient.getIamPolicy(request);
  }

  /** {@inheritDoc} */
  @Override
  public com.google.iam.v1.Policy setIamPolicy(SetIamPolicyRequest request) {
    return instanceClient.setIamPolicy(request);
  }

  /** {@inheritDoc} */
  @Override
  public TestIamPermissionsResponse testIamPermissions(TestIamPermissionsRequest request) {
    return instanceClient.testIamPermissions(request);
  }

  /** {@inheritDoc} */
  @Override
  public Operation createCluster(CreateClusterRequest request) {
    return instanceClient.createCluster(request);
  }
}
