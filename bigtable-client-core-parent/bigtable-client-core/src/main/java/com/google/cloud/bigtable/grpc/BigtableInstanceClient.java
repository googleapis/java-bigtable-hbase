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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.CreateAppProfileRequest;
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
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import com.google.protobuf.Empty;

/**
 * BigtableInstanceClient manages instances and clusters.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public interface BigtableInstanceClient {
  /**
   * Create an instance within a project.
   *
   * @param request a {@link com.google.bigtable.admin.v2.CreateInstanceRequest} object.
   * @return a {@link com.google.longrunning.Operation} object.
   */
  Operation createInstance(CreateInstanceRequest request);

  /**
   * Gets the latest state of a long-running operation. Clients may use this method to poll the
   * operation result at intervals as recommended by the API service.
   *
   * <p>{@link #createInstance(CreateInstanceRequest)} and {@link #updateCluster(Cluster)} will
   * return a {@link com.google.longrunning.Operation}. Use this method and pass in the {@link com.google.longrunning.Operation}'s name in the
   * request to see if the Operation is done via {@link com.google.longrunning.Operation#getDone()}. The instance will not
   * be available until that happens.
   *
   * @param request a {@link com.google.longrunning.GetOperationRequest} object.
   * @return a {@link com.google.longrunning.Operation} object.
   */
  Operation getOperation(GetOperationRequest request);


  /**
   * Waits for the long running operation to complete by polling with exponential backoff.
   * A default timeout of 10 minutes is used.
   * @param operation
   * @throws IOException
   * @throws TimeoutException If the timeout is exceeded.
   */
  void waitForOperation(Operation operation) throws TimeoutException, IOException;

  /**
   * Waits for the long running operation to complete by polling with exponential backoff.
   * @param operation
   * @param timeout
   * @param timeUnit
   * @throws IOException
   * @throws TimeoutException If the timeout is exceeded.
   */
  void waitForOperation(Operation operation, long timeout, TimeUnit timeUnit)
      throws IOException, TimeoutException;

  /**
   * Lists all instances in the given project.
   *
   * @param request a {@link com.google.bigtable.admin.v2.ListInstancesRequest} object.
   * @return a {@link com.google.bigtable.admin.v2.ListInstancesResponse} object.
   */
  ListInstancesResponse listInstances(ListInstancesRequest request);

  /**
   * Updates an instance within a project.
   *
   * @param instance a {@link com.google.bigtable.admin.v2.Instance} object.
   * @return a {@link com.google.bigtable.admin.v2.Instance} object.
   */
  Instance updateInstance(Instance instance);

  /**
   * Updates an instance within a project.
   *
   * @param request a {@link com.google.bigtable.admin.v2.DeleteInstanceRequest} object.
   * @return a {@link com.google.protobuf.Empty} object.
   */
  Empty deleteInstance(DeleteInstanceRequest request);

  /**
   * Gets information about a cluster.
   *
   * @param request a {@link com.google.bigtable.admin.v2.GetClusterRequest} object.
   * @return a {@link com.google.bigtable.admin.v2.Cluster} object.
   */
  Cluster getCluster(GetClusterRequest request);

  /**
   * Lists information about clusters in an instance.
   *
   * @param request a {@link com.google.bigtable.admin.v2.ListClustersRequest} object.
   * @return a {@link com.google.bigtable.admin.v2.ListClustersResponse} object.
   */
  ListClustersResponse listCluster(ListClustersRequest request);

  /**
   * Updates a cluster within an instance.
   *
   * @param cluster a {@link com.google.bigtable.admin.v2.Cluster} object.
   * @return a {@link com.google.longrunning.Operation} object.
   */
  Operation updateCluster(Cluster cluster);
  
  /**
   * Deletes a cluster from an instance.
   * 
   * @param request a {@link com.google.bigtable.admin.v2.DeleteClusterRequest} object.
   * @return a {@link com.google.protobuf.Empty} object.
   */
  Empty deleteCluster(DeleteClusterRequest request);

  /**
   * Partially updates an instance within a project.
   * 
   * @param request a {@link com.google.bigtable.admin.v2.PartialUpdateInstanceRequest} object.
   * @return a {@link com.google.longrunning.Operation} object.
   */
  Operation partialUpdateInstance(PartialUpdateInstanceRequest request);

  /**
   * This is a private alpha release of Cloud Bigtable replication. This feature
   * is not currently available to most Cloud Bigtable customers. This feature
   * might be changed in backward-incompatible ways and is not recommended for
   * production use. It is not subject to any SLA or deprecation policy.
   * Creates an app profile within an instance.
   * 
   * @param request a {@link com.google.bigtable.admin.v2.CreateAppProfileRequest} object.
   * @return a {@link com.google.bigtable.admin.v2.AppProfile} object.
   */
  AppProfile createAppProfile(CreateAppProfileRequest request);

  /**
   * This is a private alpha release of Cloud Bigtable replication. This feature
   * is not currently available to most Cloud Bigtable customers. This feature
   * might be changed in backward-incompatible ways and is not recommended for
   * production use. It is not subject to any SLA or deprecation policy.
   * Gets information about an app profile.
   *  
   * @param request a {@link com.google.bigtable.admin.v2.GetAppProfileRequest} object.
   * @return a {@link com.google.bigtable.admin.v2.AppProfile} object.
   */
  AppProfile getAppProfile(GetAppProfileRequest request);

  /**
   * This is a private alpha release of Cloud Bigtable replication. This feature
   * is not currently available to most Cloud Bigtable customers. This feature
   * might be changed in backward-incompatible ways and is not recommended for
   * production use. It is not subject to any SLA or deprecation policy.
   * Lists information about app profiles in an instance.
   * 
   * @param request a {@link com.google.bigtable.admin.v2.ListAppProfilesRequest} object. 
   * @return a {@link com.google.bigtable.admin.v2.ListAppProfilesResponse} object.
   */
  ListAppProfilesResponse listAppProfiles(ListAppProfilesRequest request);

  /**
   * This is a private alpha release of Cloud Bigtable replication. This feature
   * is not currently available to most Cloud Bigtable customers. This feature
   * might be changed in backward-incompatible ways and is not recommended for
   * production use. It is not subject to any SLA or deprecation policy.
   * Updates an app profile within an instance.
   * 
   * @param request a {@link com.google.longrunning.Operation} object.
   * @return a {@link com.google.bigtable.admin.v2.UpdateAppProfileRequest} object.
   */
  Operation updateAppProfile(UpdateAppProfileRequest request);

  /**
   * This is a private alpha release of Cloud Bigtable replication. This feature
   * is not currently available to most Cloud Bigtable customers. This feature
   * might be changed in backward-incompatible ways and is not recommended for
   * production use. It is not subject to any SLA or deprecation policy.
   * Deletes an app profile from an instance.
   * 
   * @param request a {@link com.google.bigtable.admin.v2.DeleteAppProfileRequest} object. 
   * @return a {@link com.google.protobuf.Empty} object.
   */
  Empty deleteAppProfile(DeleteAppProfileRequest request);

  /**
   * This is a private alpha release of Cloud Bigtable instance level
   * permissions. This feature is not currently available to most Cloud Bigtable
   * customers. This feature might be changed in backward-incompatible ways and
   * is not recommended for production use. It is not subject to any SLA or
   * deprecation policy.
   * Gets the access control policy for an instance resource. Returns an empty
   * policy if an instance exists but does not have a policy set.
   * 
   * @param request a {@link com.google.iam.v1.GetIamPolicyRequest} object.
   * @return a {@link com.google.iam.v1.Policy} object.
   */
  Policy getIamPolicy(GetIamPolicyRequest request);

  /**
   * This is a private alpha release of Cloud Bigtable instance level
   * permissions. This feature is not currently available to most Cloud Bigtable
   * customers. This feature might be changed in backward-incompatible ways and
   * is not recommended for production use. It is not subject to any SLA or
   * deprecation policy.
   * Sets the access control policy on an instance resource. Replaces any
   * existing policy.
   * 
   * @param request a {@link com.google.iam.v1.SetIamPolicyRequest} object.
   * @return a {@link com.google.iam.v1.Policy} object.
   */
  Policy setIamPolicy(SetIamPolicyRequest request);

  /**
   * This is a private alpha release of Cloud Bigtable instance level
   * permissions. This feature is not currently available to most Cloud Bigtable
   * customers. This feature might be changed in backward-incompatible ways and
   * is not recommended for production use. It is not subject to any SLA or
   * deprecation policy.
   * Returns permissions that the caller has on the specified instance resource.
   * 
   * @param request a {@link com.google.iam.v1.TestIamPermissionsRequest} object.
   * @return a {@link com.google.iam.v1.TestIamPermissionsResponse} object.
   */
  TestIamPermissionsResponse testIamPermissions(TestIamPermissionsRequest request);
}
