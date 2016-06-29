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
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 * <pre>
 * Service for managing zonal Cloud Bigtable resources.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 0.14.1)",
    comments = "Source: google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto")
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

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BigtableClusterServiceStub newStub(io.grpc.Channel channel) {
    return new BigtableClusterServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BigtableClusterServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableClusterServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static BigtableClusterServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableClusterServiceFutureStub(channel);
  }

  /**
   * <pre>
   * Service for managing zonal Cloud Bigtable resources.
   * </pre>
   */
  public static interface BigtableClusterService {

    /**
     * <pre>
     * Lists the supported zones for the given project.
     * </pre>
     */
    public void listZones(com.google.bigtable.admin.cluster.v1.ListZonesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListZonesResponse> responseObserver);

    /**
     * <pre>
     * Gets information about a particular cluster.
     * </pre>
     */
    public void getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver);

    /**
     * <pre>
     * Lists all clusters in the given project, along with any zones for which
     * cluster information could not be retrieved.
     * </pre>
     */
    public void listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListClustersResponse> responseObserver);

    /**
     * <pre>
     * Creates a cluster and begins preparing it to begin serving. The returned
     * cluster embeds as its "current_operation" a long-running operation which
     * can be used to track the progress of turning up the new cluster.
     * Immediately upon completion of this request:
     *  * The cluster will be readable via the API, with all requested attributes
     *    but no allocated resources.
     * Until completion of the embedded operation:
     *  * Cancelling the operation will render the cluster immediately unreadable
     *    via the API.
     *  * All other attempts to modify or delete the cluster will be rejected.
     * Upon completion of the embedded operation:
     *  * Billing for all successfully-allocated resources will begin (some types
     *    may have lower than the requested levels).
     *  * New tables can be created in the cluster.
     *  * The cluster's allocated resource levels will be readable via the API.
     * The embedded operation's "metadata" field type is
     * [CreateClusterMetadata][google.bigtable.admin.cluster.v1.CreateClusterMetadata] The embedded operation's "response" field type is
     * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
     * </pre>
     */
    public void createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver);

    /**
     * <pre>
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
     * [UpdateClusterMetadata][google.bigtable.admin.cluster.v1.UpdateClusterMetadata] The embedded operation's "response" field type is
     * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
     * </pre>
     */
    public void updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver);

    /**
     * <pre>
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
     * </pre>
     */
    public void deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    /**
     * <pre>
     * Cancels the scheduled deletion of an cluster and begins preparing it to
     * resume serving. The returned operation will also be embedded as the
     * cluster's "current_operation".
     * Immediately upon completion of this request:
     *  * The cluster's "delete_time" field will be unset, protecting it from
     *    automatic deletion.
     * Until completion of the returned operation:
     *  * The operation cannot be cancelled.
     * Upon completion of the returned operation:
     *  * Billing for the cluster's resources will resume.
     *  * All tables within the cluster will be available.
     * [UndeleteClusterMetadata][google.bigtable.admin.cluster.v1.UndeleteClusterMetadata] The embedded operation's "response" field type is
     * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
     * </pre>
     */
    public void undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver);
  }

  @io.grpc.ExperimentalApi
  public static abstract class AbstractBigtableClusterService implements BigtableClusterService, io.grpc.BindableService {

    @java.lang.Override
    public void listZones(com.google.bigtable.admin.cluster.v1.ListZonesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListZonesResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_ZONES, responseObserver);
    }

    @java.lang.Override
    public void getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_CLUSTER, responseObserver);
    }

    @java.lang.Override
    public void listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListClustersResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_CLUSTERS, responseObserver);
    }

    @java.lang.Override
    public void createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_CLUSTER, responseObserver);
    }

    @java.lang.Override
    public void updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UPDATE_CLUSTER, responseObserver);
    }

    @java.lang.Override
    public void deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_CLUSTER, responseObserver);
    }

    @java.lang.Override
    public void undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UNDELETE_CLUSTER, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return BigtableClusterServiceGrpc.bindService(this);
    }
  }

  /**
   * <pre>
   * Service for managing zonal Cloud Bigtable resources.
   * </pre>
   */
  public static interface BigtableClusterServiceBlockingClient {

    /**
     * <pre>
     * Lists the supported zones for the given project.
     * </pre>
     */
    public com.google.bigtable.admin.cluster.v1.ListZonesResponse listZones(com.google.bigtable.admin.cluster.v1.ListZonesRequest request);

    /**
     * <pre>
     * Gets information about a particular cluster.
     * </pre>
     */
    public com.google.bigtable.admin.cluster.v1.Cluster getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request);

    /**
     * <pre>
     * Lists all clusters in the given project, along with any zones for which
     * cluster information could not be retrieved.
     * </pre>
     */
    public com.google.bigtable.admin.cluster.v1.ListClustersResponse listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request);

    /**
     * <pre>
     * Creates a cluster and begins preparing it to begin serving. The returned
     * cluster embeds as its "current_operation" a long-running operation which
     * can be used to track the progress of turning up the new cluster.
     * Immediately upon completion of this request:
     *  * The cluster will be readable via the API, with all requested attributes
     *    but no allocated resources.
     * Until completion of the embedded operation:
     *  * Cancelling the operation will render the cluster immediately unreadable
     *    via the API.
     *  * All other attempts to modify or delete the cluster will be rejected.
     * Upon completion of the embedded operation:
     *  * Billing for all successfully-allocated resources will begin (some types
     *    may have lower than the requested levels).
     *  * New tables can be created in the cluster.
     *  * The cluster's allocated resource levels will be readable via the API.
     * The embedded operation's "metadata" field type is
     * [CreateClusterMetadata][google.bigtable.admin.cluster.v1.CreateClusterMetadata] The embedded operation's "response" field type is
     * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
     * </pre>
     */
    public com.google.bigtable.admin.cluster.v1.Cluster createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request);

    /**
     * <pre>
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
     * [UpdateClusterMetadata][google.bigtable.admin.cluster.v1.UpdateClusterMetadata] The embedded operation's "response" field type is
     * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
     * </pre>
     */
    public com.google.bigtable.admin.cluster.v1.Cluster updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request);

    /**
     * <pre>
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
     * </pre>
     */
    public com.google.protobuf.Empty deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request);

    /**
     * <pre>
     * Cancels the scheduled deletion of an cluster and begins preparing it to
     * resume serving. The returned operation will also be embedded as the
     * cluster's "current_operation".
     * Immediately upon completion of this request:
     *  * The cluster's "delete_time" field will be unset, protecting it from
     *    automatic deletion.
     * Until completion of the returned operation:
     *  * The operation cannot be cancelled.
     * Upon completion of the returned operation:
     *  * Billing for the cluster's resources will resume.
     *  * All tables within the cluster will be available.
     * [UndeleteClusterMetadata][google.bigtable.admin.cluster.v1.UndeleteClusterMetadata] The embedded operation's "response" field type is
     * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
     * </pre>
     */
    public com.google.longrunning.Operation undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request);
  }

  /**
   * <pre>
   * Service for managing zonal Cloud Bigtable resources.
   * </pre>
   */
  public static interface BigtableClusterServiceFutureClient {

    /**
     * <pre>
     * Lists the supported zones for the given project.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.ListZonesResponse> listZones(
        com.google.bigtable.admin.cluster.v1.ListZonesRequest request);

    /**
     * <pre>
     * Gets information about a particular cluster.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> getCluster(
        com.google.bigtable.admin.cluster.v1.GetClusterRequest request);

    /**
     * <pre>
     * Lists all clusters in the given project, along with any zones for which
     * cluster information could not be retrieved.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.ListClustersResponse> listClusters(
        com.google.bigtable.admin.cluster.v1.ListClustersRequest request);

    /**
     * <pre>
     * Creates a cluster and begins preparing it to begin serving. The returned
     * cluster embeds as its "current_operation" a long-running operation which
     * can be used to track the progress of turning up the new cluster.
     * Immediately upon completion of this request:
     *  * The cluster will be readable via the API, with all requested attributes
     *    but no allocated resources.
     * Until completion of the embedded operation:
     *  * Cancelling the operation will render the cluster immediately unreadable
     *    via the API.
     *  * All other attempts to modify or delete the cluster will be rejected.
     * Upon completion of the embedded operation:
     *  * Billing for all successfully-allocated resources will begin (some types
     *    may have lower than the requested levels).
     *  * New tables can be created in the cluster.
     *  * The cluster's allocated resource levels will be readable via the API.
     * The embedded operation's "metadata" field type is
     * [CreateClusterMetadata][google.bigtable.admin.cluster.v1.CreateClusterMetadata] The embedded operation's "response" field type is
     * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> createCluster(
        com.google.bigtable.admin.cluster.v1.CreateClusterRequest request);

    /**
     * <pre>
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
     * [UpdateClusterMetadata][google.bigtable.admin.cluster.v1.UpdateClusterMetadata] The embedded operation's "response" field type is
     * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> updateCluster(
        com.google.bigtable.admin.cluster.v1.Cluster request);

    /**
     * <pre>
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
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteCluster(
        com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request);

    /**
     * <pre>
     * Cancels the scheduled deletion of an cluster and begins preparing it to
     * resume serving. The returned operation will also be embedded as the
     * cluster's "current_operation".
     * Immediately upon completion of this request:
     *  * The cluster's "delete_time" field will be unset, protecting it from
     *    automatic deletion.
     * Until completion of the returned operation:
     *  * The operation cannot be cancelled.
     * Upon completion of the returned operation:
     *  * Billing for the cluster's resources will resume.
     *  * All tables within the cluster will be available.
     * [UndeleteClusterMetadata][google.bigtable.admin.cluster.v1.UndeleteClusterMetadata] The embedded operation's "response" field type is
     * [Cluster][google.bigtable.admin.cluster.v1.Cluster], if successful.
     * </pre>
     */
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

    @java.lang.Override
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
