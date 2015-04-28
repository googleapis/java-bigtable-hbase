/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

/**
 * A client for the Cloud Bigtable Cluster Admin API.
 */
public interface BigtableClusterAdminClient extends AutoCloseable {

  /**
   * Lists the names of all zones for a given project.
   */
  ListZonesResponse listZones(ListZonesRequest request);

  /**
   * Lists all clusters in the given project, along with any zones for which cluster information 
   * could not be retrieved.
   */
  ListClustersResponse listClusters(ListClustersRequest request);

  /**
   *  Gets information about a particular cluster.
   */
  Cluster getCluster(GetClusterRequest request);

  /**
   * <p> Gets the latest state of a long-running operation.  Clients may use this
   * method to poll the operation result at intervals as recommended by the API
   * service.</p>
   * 
   * <p> {@link #createCluster(CreateClusterRequest)} will return a {@link Cluster} with a set
   *  {@link Cluster#getCurrentOperation()}.  Use this method and pass in the {@link Operation}'s 
   *  name in the request to see if the Operation is done via {@link Operation#getDone()}.  
   *  The cluster will not be created until that happens.
   *   
   */
  // TODO(sduskis): Move this into a separate interface.  Operation management is 
  //   useful for more than just clusters.
  Operation getOperation(GetOperationRequest request);
  
  /**
  * <P>Creates a cluster and begins preparing it to begin serving. The returned
  * cluster embeds as its "current_operation" a long-running operation which
  * can be used to track the progress of turning up the new cluster.
  * Immediately upon completion of this request:</p><ul>
  *  <li> The cluster will be readable via the API, with all requested attributes
  *    but no allocated resources.
  * </ul>
  * <p>Until completion of the embedded operation:</p><ul>
  *  <li> Cancelling the operation will render the cluster immediately unreadable
  *      via the API.
  *  <li> All other attempts to modify or delete the cluster will be rejected.
  * </ul>
  * <p>Upon completion of the embedded operation:</p><ul>
  *  <li> Billing for all successfully-allocated resources will begin (some types
  *    may have lower than the requested levels).
  *  <li> New tables can be created in the cluster.
  *  <li> The cluster's allocated resource levels will be readable via the API.
  * </ul>
  * The embedded operation's "metadata" field type is
  * [CreateClusterMetadata][google.bigtable.admin.cluster.v1.CreateClusterMetadata] 
  * The embedded operation's "response" field type is
  * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
   */
  Cluster createCluster(CreateClusterRequest request);

  /**
  * Updates a cluster, and begins allocating or releasing resources as
  * requested. The returned cluster embeds as its "current_operation" a
  * long-running operation which can be used to track the progress of updating
  * the cluster.
  * Immediately upon completion of this request:
  *  * For resource types where a decrease in the cluster's allocation has been
  *    requested, billing will be based on the newly-requested level.
  * Until completion of the embedded operation:
  *  * Cancelling the operation will set its metadata's "cancelled_at_time",
  *    and begin restoring resources to their pre-request values. The operation
  *    is guaranteed to succeed at undoing all resource changes, after which
  *    point it will terminate with a CANCELLED status.
  *  * All other attempts to modify or delete the cluster will be rejected.
  *  * Reading the cluster via the API will continue to give the pre-request
  *    resource levels.
  * Upon completion of the embedded operation:
  *  * Billing will begin for all successfully-allocated resources (some types
  *    may have lower than the requested levels).
  *  * All newly-reserved resources will be available for serving the cluster's
  *    tables.
  *  * The cluster's new resource levels will be readable via the API.
  * [UpdateClusterMetadata][google.bigtable.admin.cluster.v1.UpdateClusterMetadata] (-- NOLINT --)
  * The embedded operation's "response" field type is
  * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
   */
  Cluster updateCluster(Cluster cluster);

  /**
  * Marks a cluster and all of its tables for permanent deletion in 7 days.
  * Immediately upon completion of the request:
  *  * Billing will cease for all of the cluster's reserved resources.
  *  * The cluster's "delete_time" field will be set 7 days in the future.
  * Soon afterward:
  *  * All tables within the cluster will become unavailable.
  * Prior to the cluster's "delete_time":
  *  * The cluster can be recovered with a call to UndeleteCluster.
  *  * All other attempts to modify or delete the cluster will be rejected.
  * At the cluster's "delete_time":
  *  * The cluster and *all of its tables* will immediately and irrevocably
  *    disappear from the API, and their data will be permanently deleted.
   */
  void deleteCluster(DeleteClusterRequest request);
}
