package com.google.bigtable.admin.cluster.v1;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;

@javax.annotation.Generated("by gRPC proto compiler")
public class BigtableClusterServiceGrpc {

  // Static method descriptors that strictly reflect the proto.
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.ListZonesRequest,
      com.google.bigtable.admin.cluster.v1.ListZonesResponse> METHOD_LIST_ZONES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.admin.cluster.v1.BigtableClusterService", "ListZones",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListZonesRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListZonesResponse.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.GetClusterRequest,
      com.google.bigtable.admin.cluster.v1.Cluster> METHOD_GET_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.admin.cluster.v1.BigtableClusterService", "GetCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.GetClusterRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.ListClustersRequest,
      com.google.bigtable.admin.cluster.v1.ListClustersResponse> METHOD_LIST_CLUSTERS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.admin.cluster.v1.BigtableClusterService", "ListClusters",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListClustersRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListClustersResponse.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.CreateClusterRequest,
      com.google.bigtable.admin.cluster.v1.Cluster> METHOD_CREATE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.admin.cluster.v1.BigtableClusterService", "CreateCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.CreateClusterRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.Cluster,
      com.google.bigtable.admin.cluster.v1.Cluster> METHOD_UPDATE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.admin.cluster.v1.BigtableClusterService", "UpdateCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.DeleteClusterRequest,
      com.google.protobuf.Empty> METHOD_DELETE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.admin.cluster.v1.BigtableClusterService", "DeleteCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest,
      com.google.longrunning.Operation> METHOD_UNDELETE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.admin.cluster.v1.BigtableClusterService", "UndeleteCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.parser()));

  public static BigtableClusterServiceStub newStub(io.grpc.Channel channel) {
    return new BigtableClusterServiceStub(channel);
  }

  public static BigtableClusterServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableClusterServiceBlockingStub(channel);
  }

  public static BigtableClusterServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableClusterServiceFutureStub(channel);
  }

  public static interface BigtableClusterService {

    public void listZones(com.google.bigtable.admin.cluster.v1.ListZonesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListZonesResponse> responseObserver);

    public void getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver);

    public void listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListClustersResponse> responseObserver);

    public void createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver);

    public void updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver);

    public void deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    public void undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver);
  }

  public static interface BigtableClusterServiceBlockingClient {

    public com.google.bigtable.admin.cluster.v1.ListZonesResponse listZones(com.google.bigtable.admin.cluster.v1.ListZonesRequest request);

    public com.google.bigtable.admin.cluster.v1.Cluster getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request);

    public com.google.bigtable.admin.cluster.v1.ListClustersResponse listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request);

    public com.google.bigtable.admin.cluster.v1.Cluster createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request);

    public com.google.bigtable.admin.cluster.v1.Cluster updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request);

    public com.google.protobuf.Empty deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request);

    public com.google.longrunning.Operation undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request);
  }

  public static interface BigtableClusterServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.ListZonesResponse> listZones(
        com.google.bigtable.admin.cluster.v1.ListZonesRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> getCluster(
        com.google.bigtable.admin.cluster.v1.GetClusterRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.ListClustersResponse> listClusters(
        com.google.bigtable.admin.cluster.v1.ListClustersRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> createCluster(
        com.google.bigtable.admin.cluster.v1.CreateClusterRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> updateCluster(
        com.google.bigtable.admin.cluster.v1.Cluster request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteCluster(
        com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> undeleteCluster(
        com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request);
  }

  public static class BigtableClusterServiceStub extends io.grpc.stub.AbstractStub<BigtableClusterServiceStub>
      implements BigtableClusterService {
    private BigtableClusterServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableClusterServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableClusterServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableClusterServiceStub(channel, callOptions);
    }

    @java.lang.Override
    public void listZones(com.google.bigtable.admin.cluster.v1.ListZonesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListZonesResponse> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_LIST_ZONES, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_GET_CLUSTER, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListClustersResponse> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_LIST_CLUSTERS, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_CREATE_CLUSTER, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_UPDATE_CLUSTER, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_DELETE_CLUSTER, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_UNDELETE_CLUSTER, callOptions), request, responseObserver);
    }
  }

  public static class BigtableClusterServiceBlockingStub extends io.grpc.stub.AbstractStub<BigtableClusterServiceBlockingStub>
      implements BigtableClusterServiceBlockingClient {
    private BigtableClusterServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableClusterServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableClusterServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableClusterServiceBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.ListZonesResponse listZones(com.google.bigtable.admin.cluster.v1.ListZonesRequest request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_LIST_ZONES, callOptions), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.Cluster getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_GET_CLUSTER, callOptions), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.ListClustersResponse listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_LIST_CLUSTERS, callOptions), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.Cluster createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_CREATE_CLUSTER, callOptions), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.Cluster updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_UPDATE_CLUSTER, callOptions), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_DELETE_CLUSTER, callOptions), request);
    }

    @java.lang.Override
    public com.google.longrunning.Operation undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_UNDELETE_CLUSTER, callOptions), request);
    }
  }

  public static class BigtableClusterServiceFutureStub extends io.grpc.stub.AbstractStub<BigtableClusterServiceFutureStub>
      implements BigtableClusterServiceFutureClient {
    private BigtableClusterServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableClusterServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableClusterServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableClusterServiceFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.ListZonesResponse> listZones(
        com.google.bigtable.admin.cluster.v1.ListZonesRequest request) {
      return futureUnaryCall(
          channel.newCall(METHOD_LIST_ZONES, callOptions), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> getCluster(
        com.google.bigtable.admin.cluster.v1.GetClusterRequest request) {
      return futureUnaryCall(
          channel.newCall(METHOD_GET_CLUSTER, callOptions), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.ListClustersResponse> listClusters(
        com.google.bigtable.admin.cluster.v1.ListClustersRequest request) {
      return futureUnaryCall(
          channel.newCall(METHOD_LIST_CLUSTERS, callOptions), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> createCluster(
        com.google.bigtable.admin.cluster.v1.CreateClusterRequest request) {
      return futureUnaryCall(
          channel.newCall(METHOD_CREATE_CLUSTER, callOptions), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> updateCluster(
        com.google.bigtable.admin.cluster.v1.Cluster request) {
      return futureUnaryCall(
          channel.newCall(METHOD_UPDATE_CLUSTER, callOptions), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteCluster(
        com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request) {
      return futureUnaryCall(
          channel.newCall(METHOD_DELETE_CLUSTER, callOptions), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> undeleteCluster(
        com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request) {
      return futureUnaryCall(
          channel.newCall(METHOD_UNDELETE_CLUSTER, callOptions), request);
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final BigtableClusterService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder("google.bigtable.admin.cluster.v1.BigtableClusterService")
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_LIST_ZONES,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
                com.google.bigtable.admin.cluster.v1.ListZonesRequest,
                com.google.bigtable.admin.cluster.v1.ListZonesResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.ListZonesRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListZonesResponse> responseObserver) {
                serviceImpl.listZones(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_GET_CLUSTER,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
                com.google.bigtable.admin.cluster.v1.GetClusterRequest,
                com.google.bigtable.admin.cluster.v1.Cluster>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.GetClusterRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
                serviceImpl.getCluster(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_LIST_CLUSTERS,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
                com.google.bigtable.admin.cluster.v1.ListClustersRequest,
                com.google.bigtable.admin.cluster.v1.ListClustersResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.ListClustersRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListClustersResponse> responseObserver) {
                serviceImpl.listClusters(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_CREATE_CLUSTER,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
                com.google.bigtable.admin.cluster.v1.CreateClusterRequest,
                com.google.bigtable.admin.cluster.v1.Cluster>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.CreateClusterRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
                serviceImpl.createCluster(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_UPDATE_CLUSTER,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
                com.google.bigtable.admin.cluster.v1.Cluster,
                com.google.bigtable.admin.cluster.v1.Cluster>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.Cluster request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
                serviceImpl.updateCluster(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_DELETE_CLUSTER,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
                com.google.bigtable.admin.cluster.v1.DeleteClusterRequest,
                com.google.protobuf.Empty>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request,
                  io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
                serviceImpl.deleteCluster(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_UNDELETE_CLUSTER,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
                com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest,
                com.google.longrunning.Operation>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request,
                  io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
                serviceImpl.undeleteCluster(request, responseObserver);
              }
            }))).build();
  }
}
