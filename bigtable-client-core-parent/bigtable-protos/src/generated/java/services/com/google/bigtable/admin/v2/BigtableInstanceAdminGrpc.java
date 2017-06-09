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
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 * <pre>
 * Service for creating, configuring, and deleting Cloud Bigtable Instances and
 * Clusters. Provides access to the Instance and Cluster schemas only, not the
 * tables' metadata or data stored in those tables.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.4.0)",
    comments = "Source: google/bigtable/admin/v2/bigtable_instance_admin.proto")
public final class BigtableInstanceAdminGrpc {

  private BigtableInstanceAdminGrpc() {}

  public static final String SERVICE_NAME = "google.bigtable.admin.v2.BigtableInstanceAdmin";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.CreateInstanceRequest,
      com.google.longrunning.Operation> METHOD_CREATE_INSTANCE =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.CreateInstanceRequest, com.google.longrunning.Operation>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "CreateInstance"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.CreateInstanceRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.longrunning.Operation.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.GetInstanceRequest,
      com.google.bigtable.admin.v2.Instance> METHOD_GET_INSTANCE =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.GetInstanceRequest, com.google.bigtable.admin.v2.Instance>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "GetInstance"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.GetInstanceRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.Instance.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.ListInstancesRequest,
      com.google.bigtable.admin.v2.ListInstancesResponse> METHOD_LIST_INSTANCES =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.ListInstancesRequest, com.google.bigtable.admin.v2.ListInstancesResponse>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "ListInstances"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.ListInstancesRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.ListInstancesResponse.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.Instance,
      com.google.bigtable.admin.v2.Instance> METHOD_UPDATE_INSTANCE =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.Instance, com.google.bigtable.admin.v2.Instance>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "UpdateInstance"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.Instance.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.Instance.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.DeleteInstanceRequest,
      com.google.protobuf.Empty> METHOD_DELETE_INSTANCE =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.DeleteInstanceRequest, com.google.protobuf.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "DeleteInstance"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.DeleteInstanceRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.protobuf.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.CreateClusterRequest,
      com.google.longrunning.Operation> METHOD_CREATE_CLUSTER =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.CreateClusterRequest, com.google.longrunning.Operation>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "CreateCluster"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.CreateClusterRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.longrunning.Operation.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.GetClusterRequest,
      com.google.bigtable.admin.v2.Cluster> METHOD_GET_CLUSTER =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.GetClusterRequest, com.google.bigtable.admin.v2.Cluster>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "GetCluster"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.GetClusterRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.Cluster.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.ListClustersRequest,
      com.google.bigtable.admin.v2.ListClustersResponse> METHOD_LIST_CLUSTERS =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.ListClustersRequest, com.google.bigtable.admin.v2.ListClustersResponse>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "ListClusters"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.ListClustersRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.ListClustersResponse.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.Cluster,
      com.google.longrunning.Operation> METHOD_UPDATE_CLUSTER =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.Cluster, com.google.longrunning.Operation>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "UpdateCluster"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.Cluster.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.longrunning.Operation.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.DeleteClusterRequest,
      com.google.protobuf.Empty> METHOD_DELETE_CLUSTER =
      io.grpc.MethodDescriptor.<com.google.bigtable.admin.v2.DeleteClusterRequest, com.google.protobuf.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "google.bigtable.admin.v2.BigtableInstanceAdmin", "DeleteCluster"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.bigtable.admin.v2.DeleteClusterRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.protobuf.Empty.getDefaultInstance()))
          .build();

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BigtableInstanceAdminStub newStub(io.grpc.Channel channel) {
    return new BigtableInstanceAdminStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BigtableInstanceAdminBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableInstanceAdminBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BigtableInstanceAdminFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableInstanceAdminFutureStub(channel);
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable Instances and
   * Clusters. Provides access to the Instance and Cluster schemas only, not the
   * tables' metadata or data stored in those tables.
   * </pre>
   */
  public static abstract class BigtableInstanceAdminImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Create an instance within a project.
     * </pre>
     */
    public void createInstance(com.google.bigtable.admin.v2.CreateInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_INSTANCE, responseObserver);
    }

    /**
     * <pre>
     * Gets information about an instance.
     * </pre>
     */
    public void getInstance(com.google.bigtable.admin.v2.GetInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_INSTANCE, responseObserver);
    }

    /**
     * <pre>
     * Lists information about instances in a project.
     * </pre>
     */
    public void listInstances(com.google.bigtable.admin.v2.ListInstancesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListInstancesResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_INSTANCES, responseObserver);
    }

    /**
     * <pre>
     * Updates an instance within a project.
     * </pre>
     */
    public void updateInstance(com.google.bigtable.admin.v2.Instance request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UPDATE_INSTANCE, responseObserver);
    }

    /**
     * <pre>
     * Delete an instance from a project.
     * </pre>
     */
    public void deleteInstance(com.google.bigtable.admin.v2.DeleteInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_INSTANCE, responseObserver);
    }

    /**
     * <pre>
     * Creates a cluster within an instance.
     * </pre>
     */
    public void createCluster(com.google.bigtable.admin.v2.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_CLUSTER, responseObserver);
    }

    /**
     * <pre>
     * Gets information about a cluster.
     * </pre>
     */
    public void getCluster(com.google.bigtable.admin.v2.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Cluster> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_CLUSTER, responseObserver);
    }

    /**
     * <pre>
     * Lists information about clusters in an instance.
     * </pre>
     */
    public void listClusters(com.google.bigtable.admin.v2.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListClustersResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_CLUSTERS, responseObserver);
    }

    /**
     * <pre>
     * Updates a cluster within an instance.
     * </pre>
     */
    public void updateCluster(com.google.bigtable.admin.v2.Cluster request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UPDATE_CLUSTER, responseObserver);
    }

    /**
     * <pre>
     * Deletes a cluster from an instance.
     * </pre>
     */
    public void deleteCluster(com.google.bigtable.admin.v2.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_CLUSTER, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_CREATE_INSTANCE,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.CreateInstanceRequest,
                com.google.longrunning.Operation>(
                  this, METHODID_CREATE_INSTANCE)))
          .addMethod(
            METHOD_GET_INSTANCE,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.GetInstanceRequest,
                com.google.bigtable.admin.v2.Instance>(
                  this, METHODID_GET_INSTANCE)))
          .addMethod(
            METHOD_LIST_INSTANCES,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.ListInstancesRequest,
                com.google.bigtable.admin.v2.ListInstancesResponse>(
                  this, METHODID_LIST_INSTANCES)))
          .addMethod(
            METHOD_UPDATE_INSTANCE,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.Instance,
                com.google.bigtable.admin.v2.Instance>(
                  this, METHODID_UPDATE_INSTANCE)))
          .addMethod(
            METHOD_DELETE_INSTANCE,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.DeleteInstanceRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_DELETE_INSTANCE)))
          .addMethod(
            METHOD_CREATE_CLUSTER,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.CreateClusterRequest,
                com.google.longrunning.Operation>(
                  this, METHODID_CREATE_CLUSTER)))
          .addMethod(
            METHOD_GET_CLUSTER,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.GetClusterRequest,
                com.google.bigtable.admin.v2.Cluster>(
                  this, METHODID_GET_CLUSTER)))
          .addMethod(
            METHOD_LIST_CLUSTERS,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.ListClustersRequest,
                com.google.bigtable.admin.v2.ListClustersResponse>(
                  this, METHODID_LIST_CLUSTERS)))
          .addMethod(
            METHOD_UPDATE_CLUSTER,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.Cluster,
                com.google.longrunning.Operation>(
                  this, METHODID_UPDATE_CLUSTER)))
          .addMethod(
            METHOD_DELETE_CLUSTER,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.v2.DeleteClusterRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_DELETE_CLUSTER)))
          .build();
    }
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable Instances and
   * Clusters. Provides access to the Instance and Cluster schemas only, not the
   * tables' metadata or data stored in those tables.
   * </pre>
   */
  public static final class BigtableInstanceAdminStub extends io.grpc.stub.AbstractStub<BigtableInstanceAdminStub> {
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

    /**
     * <pre>
     * Create an instance within a project.
     * </pre>
     */
    public void createInstance(com.google.bigtable.admin.v2.CreateInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_INSTANCE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Gets information about an instance.
     * </pre>
     */
    public void getInstance(com.google.bigtable.admin.v2.GetInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_INSTANCE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Lists information about instances in a project.
     * </pre>
     */
    public void listInstances(com.google.bigtable.admin.v2.ListInstancesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListInstancesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_INSTANCES, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Updates an instance within a project.
     * </pre>
     */
    public void updateInstance(com.google.bigtable.admin.v2.Instance request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Instance> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_INSTANCE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete an instance from a project.
     * </pre>
     */
    public void deleteInstance(com.google.bigtable.admin.v2.DeleteInstanceRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_INSTANCE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Creates a cluster within an instance.
     * </pre>
     */
    public void createCluster(com.google.bigtable.admin.v2.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_CLUSTER, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Gets information about a cluster.
     * </pre>
     */
    public void getCluster(com.google.bigtable.admin.v2.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Cluster> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_CLUSTER, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Lists information about clusters in an instance.
     * </pre>
     */
    public void listClusters(com.google.bigtable.admin.v2.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListClustersResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_CLUSTERS, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Updates a cluster within an instance.
     * </pre>
     */
    public void updateCluster(com.google.bigtable.admin.v2.Cluster request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CLUSTER, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Deletes a cluster from an instance.
     * </pre>
     */
    public void deleteCluster(com.google.bigtable.admin.v2.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_CLUSTER, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable Instances and
   * Clusters. Provides access to the Instance and Cluster schemas only, not the
   * tables' metadata or data stored in those tables.
   * </pre>
   */
  public static final class BigtableInstanceAdminBlockingStub extends io.grpc.stub.AbstractStub<BigtableInstanceAdminBlockingStub> {
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

    /**
     * <pre>
     * Create an instance within a project.
     * </pre>
     */
    public com.google.longrunning.Operation createInstance(com.google.bigtable.admin.v2.CreateInstanceRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_INSTANCE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Gets information about an instance.
     * </pre>
     */
    public com.google.bigtable.admin.v2.Instance getInstance(com.google.bigtable.admin.v2.GetInstanceRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_INSTANCE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Lists information about instances in a project.
     * </pre>
     */
    public com.google.bigtable.admin.v2.ListInstancesResponse listInstances(com.google.bigtable.admin.v2.ListInstancesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_INSTANCES, getCallOptions(), request);
    }

    /**
     * <pre>
     * Updates an instance within a project.
     * </pre>
     */
    public com.google.bigtable.admin.v2.Instance updateInstance(com.google.bigtable.admin.v2.Instance request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_INSTANCE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete an instance from a project.
     * </pre>
     */
    public com.google.protobuf.Empty deleteInstance(com.google.bigtable.admin.v2.DeleteInstanceRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_INSTANCE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Creates a cluster within an instance.
     * </pre>
     */
    public com.google.longrunning.Operation createCluster(com.google.bigtable.admin.v2.CreateClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_CLUSTER, getCallOptions(), request);
    }

    /**
     * <pre>
     * Gets information about a cluster.
     * </pre>
     */
    public com.google.bigtable.admin.v2.Cluster getCluster(com.google.bigtable.admin.v2.GetClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_CLUSTER, getCallOptions(), request);
    }

    /**
     * <pre>
     * Lists information about clusters in an instance.
     * </pre>
     */
    public com.google.bigtable.admin.v2.ListClustersResponse listClusters(com.google.bigtable.admin.v2.ListClustersRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_CLUSTERS, getCallOptions(), request);
    }

    /**
     * <pre>
     * Updates a cluster within an instance.
     * </pre>
     */
    public com.google.longrunning.Operation updateCluster(com.google.bigtable.admin.v2.Cluster request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_CLUSTER, getCallOptions(), request);
    }

    /**
     * <pre>
     * Deletes a cluster from an instance.
     * </pre>
     */
    public com.google.protobuf.Empty deleteCluster(com.google.bigtable.admin.v2.DeleteClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_CLUSTER, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable Instances and
   * Clusters. Provides access to the Instance and Cluster schemas only, not the
   * tables' metadata or data stored in those tables.
   * </pre>
   */
  public static final class BigtableInstanceAdminFutureStub extends io.grpc.stub.AbstractStub<BigtableInstanceAdminFutureStub> {
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

    /**
     * <pre>
     * Create an instance within a project.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> createInstance(
        com.google.bigtable.admin.v2.CreateInstanceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_INSTANCE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Gets information about an instance.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Instance> getInstance(
        com.google.bigtable.admin.v2.GetInstanceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_INSTANCE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Lists information about instances in a project.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.ListInstancesResponse> listInstances(
        com.google.bigtable.admin.v2.ListInstancesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_INSTANCES, getCallOptions()), request);
    }

    /**
     * <pre>
     * Updates an instance within a project.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Instance> updateInstance(
        com.google.bigtable.admin.v2.Instance request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_INSTANCE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete an instance from a project.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteInstance(
        com.google.bigtable.admin.v2.DeleteInstanceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_INSTANCE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Creates a cluster within an instance.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> createCluster(
        com.google.bigtable.admin.v2.CreateClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_CLUSTER, getCallOptions()), request);
    }

    /**
     * <pre>
     * Gets information about a cluster.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Cluster> getCluster(
        com.google.bigtable.admin.v2.GetClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_CLUSTER, getCallOptions()), request);
    }

    /**
     * <pre>
     * Lists information about clusters in an instance.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.ListClustersResponse> listClusters(
        com.google.bigtable.admin.v2.ListClustersRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_CLUSTERS, getCallOptions()), request);
    }

    /**
     * <pre>
     * Updates a cluster within an instance.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> updateCluster(
        com.google.bigtable.admin.v2.Cluster request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CLUSTER, getCallOptions()), request);
    }

    /**
     * <pre>
     * Deletes a cluster from an instance.
     * </pre>
     */
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

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BigtableInstanceAdminImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BigtableInstanceAdminImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
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

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class BigtableInstanceAdminDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.google.bigtable.admin.v2.BigtableInstanceAdminProto.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (BigtableInstanceAdminGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BigtableInstanceAdminDescriptorSupplier())
              .addMethod(METHOD_CREATE_INSTANCE)
              .addMethod(METHOD_GET_INSTANCE)
              .addMethod(METHOD_LIST_INSTANCES)
              .addMethod(METHOD_UPDATE_INSTANCE)
              .addMethod(METHOD_DELETE_INSTANCE)
              .addMethod(METHOD_CREATE_CLUSTER)
              .addMethod(METHOD_GET_CLUSTER)
              .addMethod(METHOD_LIST_CLUSTERS)
              .addMethod(METHOD_UPDATE_CLUSTER)
              .addMethod(METHOD_DELETE_CLUSTER)
              .build();
        }
      }
    }
    return result;
  }
}
