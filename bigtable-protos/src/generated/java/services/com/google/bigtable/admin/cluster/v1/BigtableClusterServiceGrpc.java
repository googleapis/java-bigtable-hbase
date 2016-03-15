package com.google.bigtable.admin.cluster.v1;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;

@javax.annotation.Generated("by gRPC proto compiler")
public class BigtableClusterServiceGrpc {

  private BigtableClusterServiceGrpc() {}

  public static final String SERVICE_NAME = "google.bigtable.admin.cluster.v1.BigtableClusterService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.ListZonesRequest,
      com.google.bigtable.admin.cluster.v1.ListZonesResponse> METHOD_LIST_ZONES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.cluster.v1.BigtableClusterService", "ListZones"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListZonesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListZonesResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.GetClusterRequest,
      com.google.bigtable.admin.cluster.v1.Cluster> METHOD_GET_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.cluster.v1.BigtableClusterService", "GetCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.GetClusterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.ListClustersRequest,
      com.google.bigtable.admin.cluster.v1.ListClustersResponse> METHOD_LIST_CLUSTERS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.cluster.v1.BigtableClusterService", "ListClusters"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListClustersRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListClustersResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.CreateClusterRequest,
      com.google.bigtable.admin.cluster.v1.Cluster> METHOD_CREATE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.cluster.v1.BigtableClusterService", "CreateCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.CreateClusterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.Cluster,
      com.google.bigtable.admin.cluster.v1.Cluster> METHOD_UPDATE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.cluster.v1.BigtableClusterService", "UpdateCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.DeleteClusterRequest,
      com.google.protobuf.Empty> METHOD_DELETE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.cluster.v1.BigtableClusterService", "DeleteCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest,
      com.google.longrunning.Operation> METHOD_UNDELETE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.cluster.v1.BigtableClusterService", "UndeleteCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.getDefaultInstance()));

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
          getChannel().newCall(METHOD_LIST_ZONES, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_CLUSTER, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListClustersResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_CLUSTERS, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_CLUSTER, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CLUSTER, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_CLUSTER, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UNDELETE_CLUSTER, getCallOptions()), request, responseObserver);
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
          getChannel(), METHOD_LIST_ZONES, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.Cluster getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_CLUSTER, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.ListClustersResponse listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_CLUSTERS, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.Cluster createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_CLUSTER, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.Cluster updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_CLUSTER, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_CLUSTER, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.longrunning.Operation undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UNDELETE_CLUSTER, getCallOptions(), request);
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
          getChannel().newCall(METHOD_LIST_ZONES, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> getCluster(
        com.google.bigtable.admin.cluster.v1.GetClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_CLUSTER, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.ListClustersResponse> listClusters(
        com.google.bigtable.admin.cluster.v1.ListClustersRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_CLUSTERS, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> createCluster(
        com.google.bigtable.admin.cluster.v1.CreateClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_CLUSTER, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> updateCluster(
        com.google.bigtable.admin.cluster.v1.Cluster request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CLUSTER, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteCluster(
        com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_CLUSTER, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> undeleteCluster(
        com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UNDELETE_CLUSTER, getCallOptions()), request);
    }
  }

  private static final int METHODID_LIST_ZONES = 0;
  private static final int METHODID_GET_CLUSTER = 1;
  private static final int METHODID_LIST_CLUSTERS = 2;
  private static final int METHODID_CREATE_CLUSTER = 3;
  private static final int METHODID_UPDATE_CLUSTER = 4;
  private static final int METHODID_DELETE_CLUSTER = 5;
  private static final int METHODID_UNDELETE_CLUSTER = 6;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BigtableClusterService serviceImpl;
    private final int methodId;

    public MethodHandlers(BigtableClusterService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_LIST_ZONES:
          serviceImpl.listZones((com.google.bigtable.admin.cluster.v1.ListZonesRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListZonesResponse>) responseObserver);
          break;
        case METHODID_GET_CLUSTER:
          serviceImpl.getCluster((com.google.bigtable.admin.cluster.v1.GetClusterRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster>) responseObserver);
          break;
        case METHODID_LIST_CLUSTERS:
          serviceImpl.listClusters((com.google.bigtable.admin.cluster.v1.ListClustersRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListClustersResponse>) responseObserver);
          break;
        case METHODID_CREATE_CLUSTER:
          serviceImpl.createCluster((com.google.bigtable.admin.cluster.v1.CreateClusterRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster>) responseObserver);
          break;
        case METHODID_UPDATE_CLUSTER:
          serviceImpl.updateCluster((com.google.bigtable.admin.cluster.v1.Cluster) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster>) responseObserver);
          break;
        case METHODID_DELETE_CLUSTER:
          serviceImpl.deleteCluster((com.google.bigtable.admin.cluster.v1.DeleteClusterRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_UNDELETE_CLUSTER:
          serviceImpl.undeleteCluster((com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest) request,
              (io.grpc.stub.StreamObserver<com.google.longrunning.Operation>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final BigtableClusterService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_LIST_ZONES,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.cluster.v1.ListZonesRequest,
              com.google.bigtable.admin.cluster.v1.ListZonesResponse>(
                serviceImpl, METHODID_LIST_ZONES)))
        .addMethod(
          METHOD_GET_CLUSTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.cluster.v1.GetClusterRequest,
              com.google.bigtable.admin.cluster.v1.Cluster>(
                serviceImpl, METHODID_GET_CLUSTER)))
        .addMethod(
          METHOD_LIST_CLUSTERS,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.cluster.v1.ListClustersRequest,
              com.google.bigtable.admin.cluster.v1.ListClustersResponse>(
                serviceImpl, METHODID_LIST_CLUSTERS)))
        .addMethod(
          METHOD_CREATE_CLUSTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.cluster.v1.CreateClusterRequest,
              com.google.bigtable.admin.cluster.v1.Cluster>(
                serviceImpl, METHODID_CREATE_CLUSTER)))
        .addMethod(
          METHOD_UPDATE_CLUSTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.cluster.v1.Cluster,
              com.google.bigtable.admin.cluster.v1.Cluster>(
                serviceImpl, METHODID_UPDATE_CLUSTER)))
        .addMethod(
          METHOD_DELETE_CLUSTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.cluster.v1.DeleteClusterRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_DELETE_CLUSTER)))
        .addMethod(
          METHOD_UNDELETE_CLUSTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest,
              com.google.longrunning.Operation>(
                serviceImpl, METHODID_UNDELETE_CLUSTER)))
        .build();
  }
}
