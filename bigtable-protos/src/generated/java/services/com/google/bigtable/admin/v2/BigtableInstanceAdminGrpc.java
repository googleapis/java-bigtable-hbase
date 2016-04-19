package com.google.bigtable.admin.v2;

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
public class BigtableInstanceAdminGrpc {

  private BigtableInstanceAdminGrpc() {}

  public static final String SERVICE_NAME = "google.bigtable.admin.v2.BigtableInstanceAdmin";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.CreateInstanceRequest,
      com.google.longrunning.Operation> METHOD_CREATE_INSTANCE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "CreateInstance"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.CreateInstanceRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.GetInstanceRequest,
      com.google.bigtable.admin.v2.Instance> METHOD_GET_INSTANCE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "GetInstance"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.GetInstanceRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.Instance.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.ListInstancesRequest,
      com.google.bigtable.admin.v2.ListInstancesResponse> METHOD_LIST_INSTANCES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "ListInstances"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.ListInstancesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.ListInstancesResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.Instance,
      com.google.bigtable.admin.v2.Instance> METHOD_UPDATE_INSTANCE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "UpdateInstance"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.Instance.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.Instance.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.DeleteInstanceRequest,
      com.google.protobuf.Empty> METHOD_DELETE_INSTANCE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "DeleteInstance"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.DeleteInstanceRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.CreateClusterRequest,
      com.google.longrunning.Operation> METHOD_CREATE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "CreateCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.CreateClusterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.GetClusterRequest,
      com.google.bigtable.admin.v2.Cluster> METHOD_GET_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "GetCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.GetClusterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.Cluster.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.ListClustersRequest,
      com.google.bigtable.admin.v2.ListClustersResponse> METHOD_LIST_CLUSTERS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "ListClusters"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.ListClustersRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.ListClustersResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.Cluster,
      com.google.longrunning.Operation> METHOD_UPDATE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "UpdateCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.Cluster.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.DeleteClusterRequest,
      com.google.protobuf.Empty> METHOD_DELETE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "DeleteCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.DeleteClusterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));

  public static BigtableInstanceAdminStub newStub(io.grpc.Channel channel) {
    return new BigtableInstanceAdminStub(channel);
  }

  public static BigtableInstanceAdminBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableInstanceAdminBlockingStub(channel);
  }

  public static BigtableInstanceAdminFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableInstanceAdminFutureStub(channel);
  }

  public static interface BigtableInstanceAdmin {

    public void createInstance(com.google.bigtable.admin.v2.CreateInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver);

    public void getInstance(com.google.bigtable.admin.v2.GetInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance> responseObserver);

    public void listInstances(com.google.bigtable.admin.v2.ListInstancesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListInstancesResponse> responseObserver);

    public void updateInstance(com.google.bigtable.admin.v2.Instance request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance> responseObserver);

    public void deleteInstance(com.google.bigtable.admin.v2.DeleteInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    public void createCluster(com.google.bigtable.admin.v2.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver);

    public void getCluster(com.google.bigtable.admin.v2.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Cluster> responseObserver);

    public void listClusters(com.google.bigtable.admin.v2.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListClustersResponse> responseObserver);

    public void updateCluster(com.google.bigtable.admin.v2.Cluster request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver);

    public void deleteCluster(com.google.bigtable.admin.v2.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);
  }

  public static interface BigtableInstanceAdminBlockingClient {

    public com.google.longrunning.Operation createInstance(com.google.bigtable.admin.v2.CreateInstanceRequest request);

    public com.google.bigtable.admin.v2.Instance getInstance(com.google.bigtable.admin.v2.GetInstanceRequest request);

    public com.google.bigtable.admin.v2.ListInstancesResponse listInstances(com.google.bigtable.admin.v2.ListInstancesRequest request);

    public com.google.bigtable.admin.v2.Instance updateInstance(com.google.bigtable.admin.v2.Instance request);

    public com.google.protobuf.Empty deleteInstance(com.google.bigtable.admin.v2.DeleteInstanceRequest request);

    public com.google.longrunning.Operation createCluster(com.google.bigtable.admin.v2.CreateClusterRequest request);

    public com.google.bigtable.admin.v2.Cluster getCluster(com.google.bigtable.admin.v2.GetClusterRequest request);

    public com.google.bigtable.admin.v2.ListClustersResponse listClusters(com.google.bigtable.admin.v2.ListClustersRequest request);

    public com.google.longrunning.Operation updateCluster(com.google.bigtable.admin.v2.Cluster request);

    public com.google.protobuf.Empty deleteCluster(com.google.bigtable.admin.v2.DeleteClusterRequest request);
  }

  public static interface BigtableInstanceAdminFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> createInstance(
        com.google.bigtable.admin.v2.CreateInstanceRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Instance> getInstance(
        com.google.bigtable.admin.v2.GetInstanceRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.ListInstancesResponse> listInstances(
        com.google.bigtable.admin.v2.ListInstancesRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Instance> updateInstance(
        com.google.bigtable.admin.v2.Instance request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteInstance(
        com.google.bigtable.admin.v2.DeleteInstanceRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> createCluster(
        com.google.bigtable.admin.v2.CreateClusterRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Cluster> getCluster(
        com.google.bigtable.admin.v2.GetClusterRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.ListClustersResponse> listClusters(
        com.google.bigtable.admin.v2.ListClustersRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> updateCluster(
        com.google.bigtable.admin.v2.Cluster request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteCluster(
        com.google.bigtable.admin.v2.DeleteClusterRequest request);
  }

  public static class BigtableInstanceAdminStub extends io.grpc.stub.AbstractStub<BigtableInstanceAdminStub>
      implements BigtableInstanceAdmin {
    private BigtableInstanceAdminStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableInstanceAdminStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableInstanceAdminStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableInstanceAdminStub(channel, callOptions);
    }

    @java.lang.Override
    public void createInstance(com.google.bigtable.admin.v2.CreateInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_INSTANCE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void getInstance(com.google.bigtable.admin.v2.GetInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_INSTANCE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void listInstances(com.google.bigtable.admin.v2.ListInstancesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListInstancesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_INSTANCES, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void updateInstance(com.google.bigtable.admin.v2.Instance request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_INSTANCE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void deleteInstance(com.google.bigtable.admin.v2.DeleteInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_INSTANCE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void createCluster(com.google.bigtable.admin.v2.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_CLUSTER, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void getCluster(com.google.bigtable.admin.v2.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Cluster> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_CLUSTER, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void listClusters(com.google.bigtable.admin.v2.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListClustersResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_CLUSTERS, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void updateCluster(com.google.bigtable.admin.v2.Cluster request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CLUSTER, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void deleteCluster(com.google.bigtable.admin.v2.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_CLUSTER, getCallOptions()), request, responseObserver);
    }
  }

  public static class BigtableInstanceAdminBlockingStub extends io.grpc.stub.AbstractStub<BigtableInstanceAdminBlockingStub>
      implements BigtableInstanceAdminBlockingClient {
    private BigtableInstanceAdminBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableInstanceAdminBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableInstanceAdminBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableInstanceAdminBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.longrunning.Operation createInstance(com.google.bigtable.admin.v2.CreateInstanceRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_INSTANCE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.v2.Instance getInstance(com.google.bigtable.admin.v2.GetInstanceRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_INSTANCE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.v2.ListInstancesResponse listInstances(com.google.bigtable.admin.v2.ListInstancesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_INSTANCES, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.v2.Instance updateInstance(com.google.bigtable.admin.v2.Instance request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_INSTANCE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteInstance(com.google.bigtable.admin.v2.DeleteInstanceRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_INSTANCE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.longrunning.Operation createCluster(com.google.bigtable.admin.v2.CreateClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_CLUSTER, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.v2.Cluster getCluster(com.google.bigtable.admin.v2.GetClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_CLUSTER, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.v2.ListClustersResponse listClusters(com.google.bigtable.admin.v2.ListClustersRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_CLUSTERS, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.longrunning.Operation updateCluster(com.google.bigtable.admin.v2.Cluster request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_CLUSTER, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteCluster(com.google.bigtable.admin.v2.DeleteClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_CLUSTER, getCallOptions(), request);
    }
  }

  public static class BigtableInstanceAdminFutureStub extends io.grpc.stub.AbstractStub<BigtableInstanceAdminFutureStub>
      implements BigtableInstanceAdminFutureClient {
    private BigtableInstanceAdminFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableInstanceAdminFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableInstanceAdminFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableInstanceAdminFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> createInstance(
        com.google.bigtable.admin.v2.CreateInstanceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_INSTANCE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Instance> getInstance(
        com.google.bigtable.admin.v2.GetInstanceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_INSTANCE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.ListInstancesResponse> listInstances(
        com.google.bigtable.admin.v2.ListInstancesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_INSTANCES, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Instance> updateInstance(
        com.google.bigtable.admin.v2.Instance request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_INSTANCE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteInstance(
        com.google.bigtable.admin.v2.DeleteInstanceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_INSTANCE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> createCluster(
        com.google.bigtable.admin.v2.CreateClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_CLUSTER, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Cluster> getCluster(
        com.google.bigtable.admin.v2.GetClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_CLUSTER, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.ListClustersResponse> listClusters(
        com.google.bigtable.admin.v2.ListClustersRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_CLUSTERS, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> updateCluster(
        com.google.bigtable.admin.v2.Cluster request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CLUSTER, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteCluster(
        com.google.bigtable.admin.v2.DeleteClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_CLUSTER, getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_INSTANCE = 0;
  private static final int METHODID_GET_INSTANCE = 1;
  private static final int METHODID_LIST_INSTANCES = 2;
  private static final int METHODID_UPDATE_INSTANCE = 3;
  private static final int METHODID_DELETE_INSTANCE = 4;
  private static final int METHODID_CREATE_CLUSTER = 5;
  private static final int METHODID_GET_CLUSTER = 6;
  private static final int METHODID_LIST_CLUSTERS = 7;
  private static final int METHODID_UPDATE_CLUSTER = 8;
  private static final int METHODID_DELETE_CLUSTER = 9;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BigtableInstanceAdmin serviceImpl;
    private final int methodId;

    public MethodHandlers(BigtableInstanceAdmin serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_INSTANCE:
          serviceImpl.createInstance((com.google.bigtable.admin.v2.CreateInstanceRequest) request,
              (io.grpc.stub.StreamObserver<com.google.longrunning.Operation>) responseObserver);
          break;
        case METHODID_GET_INSTANCE:
          serviceImpl.getInstance((com.google.bigtable.admin.v2.GetInstanceRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance>) responseObserver);
          break;
        case METHODID_LIST_INSTANCES:
          serviceImpl.listInstances((com.google.bigtable.admin.v2.ListInstancesRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListInstancesResponse>) responseObserver);
          break;
        case METHODID_UPDATE_INSTANCE:
          serviceImpl.updateInstance((com.google.bigtable.admin.v2.Instance) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance>) responseObserver);
          break;
        case METHODID_DELETE_INSTANCE:
          serviceImpl.deleteInstance((com.google.bigtable.admin.v2.DeleteInstanceRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_CREATE_CLUSTER:
          serviceImpl.createCluster((com.google.bigtable.admin.v2.CreateClusterRequest) request,
              (io.grpc.stub.StreamObserver<com.google.longrunning.Operation>) responseObserver);
          break;
        case METHODID_GET_CLUSTER:
          serviceImpl.getCluster((com.google.bigtable.admin.v2.GetClusterRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Cluster>) responseObserver);
          break;
        case METHODID_LIST_CLUSTERS:
          serviceImpl.listClusters((com.google.bigtable.admin.v2.ListClustersRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListClustersResponse>) responseObserver);
          break;
        case METHODID_UPDATE_CLUSTER:
          serviceImpl.updateCluster((com.google.bigtable.admin.v2.Cluster) request,
              (io.grpc.stub.StreamObserver<com.google.longrunning.Operation>) responseObserver);
          break;
        case METHODID_DELETE_CLUSTER:
          serviceImpl.deleteCluster((com.google.bigtable.admin.v2.DeleteClusterRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
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
      final BigtableInstanceAdmin serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_CREATE_INSTANCE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.CreateInstanceRequest,
              com.google.longrunning.Operation>(
                serviceImpl, METHODID_CREATE_INSTANCE)))
        .addMethod(
          METHOD_GET_INSTANCE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.GetInstanceRequest,
              com.google.bigtable.admin.v2.Instance>(
                serviceImpl, METHODID_GET_INSTANCE)))
        .addMethod(
          METHOD_LIST_INSTANCES,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.ListInstancesRequest,
              com.google.bigtable.admin.v2.ListInstancesResponse>(
                serviceImpl, METHODID_LIST_INSTANCES)))
        .addMethod(
          METHOD_UPDATE_INSTANCE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.Instance,
              com.google.bigtable.admin.v2.Instance>(
                serviceImpl, METHODID_UPDATE_INSTANCE)))
        .addMethod(
          METHOD_DELETE_INSTANCE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.DeleteInstanceRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_DELETE_INSTANCE)))
        .addMethod(
          METHOD_CREATE_CLUSTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.CreateClusterRequest,
              com.google.longrunning.Operation>(
                serviceImpl, METHODID_CREATE_CLUSTER)))
        .addMethod(
          METHOD_GET_CLUSTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.GetClusterRequest,
              com.google.bigtable.admin.v2.Cluster>(
                serviceImpl, METHODID_GET_CLUSTER)))
        .addMethod(
          METHOD_LIST_CLUSTERS,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.ListClustersRequest,
              com.google.bigtable.admin.v2.ListClustersResponse>(
                serviceImpl, METHODID_LIST_CLUSTERS)))
        .addMethod(
          METHOD_UPDATE_CLUSTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.Cluster,
              com.google.longrunning.Operation>(
                serviceImpl, METHODID_UPDATE_CLUSTER)))
        .addMethod(
          METHOD_DELETE_CLUSTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.DeleteClusterRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_DELETE_CLUSTER)))
        .build();
  }
}
