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
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.io.CredentialInterceptorCache;
import com.google.cloud.bigtable.grpc.io.HeaderInterceptor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

/** Sizes a Cluster to a specific node count. */
public class BigtableClusterUtilities {
  public static boolean resizeCluster(
      BigtableInstanceName instanceName, String clusterId, int newSize)
      throws IOException, GeneralSecurityException {
    final BigtableOptions options =
        new BigtableOptions.Builder()
            .setProjectId(instanceName.getProjectId())
            .setInstanceId(instanceName.getInstanceId())
            .build();
    HeaderInterceptor interceptor =
        CredentialInterceptorCache.getInstance()
            .getCredentialsInterceptor(options.getCredentialOptions(), options.getRetryOptions());
    ManagedChannel channelPool =
        new ChannelPool(
            ImmutableList.<HeaderInterceptor>of(interceptor),
            new ChannelPool.ChannelFactory() {

              @Override
              public ManagedChannel create() throws IOException {
                return BigtableSession.createNettyChannel(
                    BigtableOptions.BIGTABLE_INSTANCE_ADMIN_HOST_DEFAULT, options);
              }
            });

    try {
      BigtableInstanceClient client = new BigtableInstanceGrpcClient(channelPool);
      ListClustersRequest listRequest =
          ListClustersRequest.newBuilder().setParent(instanceName.getInstanceName()).build();
      ListClustersResponse list = client.listCluster(listRequest);
      Cluster cluster = getCluster(list, clusterId);
      System.out.println(cluster ); 
    } finally {
      channelPool.shutdownNow();
    }
    //    BigtableInstanceClient client =
    return false;
  }


  private static Cluster getCluster(ListClustersResponse list, String clusterId) {
    for (Cluster cluster : list.getClustersList()) {
      if (cluster.getName().endsWith("/clusters/" + clusterId)){
        return cluster;
      }
    }
    return null;
  }


  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(
        args.length == 4,
        "Usage: BigtableClusterUtilities [projectId] [instanceId] [clusterId] [size]");
    BigtableClusterUtilities.resizeCluster(
        new BigtableInstanceName(args[0], args[1]), args[2], Integer.valueOf(args[3]));
  }
}
