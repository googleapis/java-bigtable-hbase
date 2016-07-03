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
import com.google.bigtable.admin.v2.CreateInstanceRequest;
import com.google.bigtable.admin.v2.DeleteInstanceRequest;
import com.google.bigtable.admin.v2.GetClusterRequest;
import com.google.bigtable.admin.v2.Instance;
import com.google.bigtable.admin.v2.ListClustersRequest;
import com.google.bigtable.admin.v2.ListClustersResponse;
import com.google.bigtable.admin.v2.ListInstancesRequest;
import com.google.bigtable.admin.v2.ListInstancesResponse;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import com.google.protobuf.Empty;

/**
 *
 * @author sduskis
 *
 */
public interface BigtableInstanceClient {

  /**
  * <P>Creates an instance and begins preparing it to begin serving. The returned
  * {@link Operation} can be used to track the progress of turning up the new instance.
  * Immediately upon completion of this request:</p><ul>
  *  <li> The instance will be readable via the API, with all requested attributes
  *    but no allocated resources.
  * </ul>
  * <p>Upon completion of the operation:</p><ul>
  *  <li> Billing for all successfully-allocated resources will begin (some types
  *    may have lower than the requested levels).
  *  <li> New tables can be created in the instance.
  *  <li> The instance's allocated resource levels will be readable via the API.
  * </ul>
  */
  Operation createInstance(CreateInstanceRequest request);

  /**
   * <p>
   * Gets the latest state of a long-running operation. Clients may use this method to poll the
   * operation result at intervals as recommended by the API service.
   * </p>
   * <p>
   * {@link #createInstance(CreateInstanceRequest)} will return a {@link Operation}. Use this method
   * and pass in the {@link Operation}'s name in the request to see if the Operation is done via
   * {@link Operation#getDone()}. The instance will not be available until that happens.
   */
  Operation getOperation(GetOperationRequest request);

  /**
   * Lists all instances in the given project.
   */
  ListInstancesResponse listInstances(ListInstancesRequest request);

  /**
   * Updates an instance within a project.
   */
  Instance updateInstance(Instance instance);

  /**
   * Updates an instance within a project.
   */
  Empty deleteInstance(DeleteInstanceRequest request);

  /**
   * Gets information about a cluster.
   */
  Cluster getCluster(GetClusterRequest request);

  /**
   * Lists information about clusters in an instance.
   */
  ListClustersResponse listCluster(ListClustersRequest request);

  /**
   * Updates a cluster within an instance.
   */
  Operation updateCluster(Cluster cluster);
}
