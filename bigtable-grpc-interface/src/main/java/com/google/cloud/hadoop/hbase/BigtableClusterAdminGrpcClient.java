package com.google.cloud.hadoop.hbase;

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

import java.util.concurrent.ExecutorService;

/**
 * A gRPC client for accessing the Bigtable Cluster Admin API.
 */
public class BigtableClusterAdminGrpcClient implements BigtableClusterAdminClient{

  /**
   * Factory method to create an gRPC based client.
   * @return A client ready to access Bigtable cluster services.
   */
  public static BigtableClusterAdminGrpcClient createClient(
      TransportOptions transportOptions,
      ChannelOptions channelOptions,
      ExecutorService executorService) {

    CloseableChannel channel = BigtableChannels.createChannel(
        transportOptions,
        channelOptions,
        executorService);

    return new BigtableClusterAdminGrpcClient(channel);
  }

  private final BigtableClusterServiceGrpc.BigtableClusterServiceBlockingStub blockingStub;
  private final OperationsBlockingStub operationsStub;
  private final CloseableChannel channel;


  public BigtableClusterAdminGrpcClient(CloseableChannel channel) {
    blockingStub = BigtableClusterServiceGrpc.newBlockingStub(channel);
    operationsStub = OperationsGrpc.newBlockingStub(channel);
    this.channel = channel;
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
    channel.close();
  }

}
