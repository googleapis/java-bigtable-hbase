package com.google.bigtable.admin.cluster.v1;

import static io.grpc.stub.Calls.createMethodDescriptor;
import static io.grpc.stub.Calls.asyncUnaryCall;
import static io.grpc.stub.Calls.asyncServerStreamingCall;
import static io.grpc.stub.Calls.asyncClientStreamingCall;
import static io.grpc.stub.Calls.duplexStreamingCall;
import static io.grpc.stub.Calls.blockingUnaryCall;
import static io.grpc.stub.Calls.blockingServerStreamingCall;
import static io.grpc.stub.Calls.unaryFutureCall;
import static io.grpc.stub.ServerCalls.createMethodDefinition;
import static io.grpc.stub.ServerCalls.asyncUnaryRequestCall;
import static io.grpc.stub.ServerCalls.asyncStreamingRequestCall;

@javax.annotation.Generated("by gRPC proto compiler")
public class BigtableClusterServiceGrpc {

  private static final io.grpc.stub.Method<com.google.bigtable.admin.cluster.v1.ListZonesRequest,
      com.google.bigtable.admin.cluster.v1.ListZonesResponse> METHOD_LIST_ZONES =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "ListZones",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListZonesRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListZonesResponse.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.cluster.v1.GetClusterRequest,
      com.google.bigtable.admin.cluster.v1.Cluster> METHOD_GET_CLUSTER =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "GetCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.GetClusterRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.cluster.v1.ListClustersRequest,
      com.google.bigtable.admin.cluster.v1.ListClustersResponse> METHOD_LIST_CLUSTERS =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "ListClusters",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListClustersRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.ListClustersResponse.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.cluster.v1.CreateClusterRequest,
      com.google.bigtable.admin.cluster.v1.Cluster> METHOD_CREATE_CLUSTER =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "CreateCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.CreateClusterRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.cluster.v1.Cluster,
      com.google.bigtable.admin.cluster.v1.Cluster> METHOD_UPDATE_CLUSTER =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "UpdateCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.Cluster.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.cluster.v1.DeleteClusterRequest,
      com.google.protobuf.Empty> METHOD_DELETE_CLUSTER =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "DeleteCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest,
      com.google.longrunning.Operation> METHOD_UNDELETE_CLUSTER =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "UndeleteCluster",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.PARSER));

  public static BigtableClusterServiceStub newStub(io.grpc.Channel channel) {
    return new BigtableClusterServiceStub(channel, CONFIG);
  }

  public static BigtableClusterServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableClusterServiceBlockingStub(channel, CONFIG);
  }

  public static BigtableClusterServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableClusterServiceFutureStub(channel, CONFIG);
  }

  public static final BigtableClusterServiceServiceDescriptor CONFIG =
      new BigtableClusterServiceServiceDescriptor();

  @javax.annotation.concurrent.Immutable
  public static class BigtableClusterServiceServiceDescriptor extends
      io.grpc.stub.AbstractServiceDescriptor<BigtableClusterServiceServiceDescriptor> {
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.ListZonesRequest,
        com.google.bigtable.admin.cluster.v1.ListZonesResponse> listZones;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.GetClusterRequest,
        com.google.bigtable.admin.cluster.v1.Cluster> getCluster;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.ListClustersRequest,
        com.google.bigtable.admin.cluster.v1.ListClustersResponse> listClusters;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.CreateClusterRequest,
        com.google.bigtable.admin.cluster.v1.Cluster> createCluster;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.Cluster,
        com.google.bigtable.admin.cluster.v1.Cluster> updateCluster;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.DeleteClusterRequest,
        com.google.protobuf.Empty> deleteCluster;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest,
        com.google.longrunning.Operation> undeleteCluster;

    private BigtableClusterServiceServiceDescriptor() {
      listZones = createMethodDescriptor(
          "google.bigtable.admin.cluster.v1.BigtableClusterService", METHOD_LIST_ZONES);
      getCluster = createMethodDescriptor(
          "google.bigtable.admin.cluster.v1.BigtableClusterService", METHOD_GET_CLUSTER);
      listClusters = createMethodDescriptor(
          "google.bigtable.admin.cluster.v1.BigtableClusterService", METHOD_LIST_CLUSTERS);
      createCluster = createMethodDescriptor(
          "google.bigtable.admin.cluster.v1.BigtableClusterService", METHOD_CREATE_CLUSTER);
      updateCluster = createMethodDescriptor(
          "google.bigtable.admin.cluster.v1.BigtableClusterService", METHOD_UPDATE_CLUSTER);
      deleteCluster = createMethodDescriptor(
          "google.bigtable.admin.cluster.v1.BigtableClusterService", METHOD_DELETE_CLUSTER);
      undeleteCluster = createMethodDescriptor(
          "google.bigtable.admin.cluster.v1.BigtableClusterService", METHOD_UNDELETE_CLUSTER);
    }

    @SuppressWarnings("unchecked")
    private BigtableClusterServiceServiceDescriptor(
        java.util.Map<java.lang.String, io.grpc.MethodDescriptor<?, ?>> methodMap) {
      listZones = (io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.ListZonesRequest,
          com.google.bigtable.admin.cluster.v1.ListZonesResponse>) methodMap.get(
          CONFIG.listZones.getName());
      getCluster = (io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.GetClusterRequest,
          com.google.bigtable.admin.cluster.v1.Cluster>) methodMap.get(
          CONFIG.getCluster.getName());
      listClusters = (io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.ListClustersRequest,
          com.google.bigtable.admin.cluster.v1.ListClustersResponse>) methodMap.get(
          CONFIG.listClusters.getName());
      createCluster = (io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.CreateClusterRequest,
          com.google.bigtable.admin.cluster.v1.Cluster>) methodMap.get(
          CONFIG.createCluster.getName());
      updateCluster = (io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.Cluster,
          com.google.bigtable.admin.cluster.v1.Cluster>) methodMap.get(
          CONFIG.updateCluster.getName());
      deleteCluster = (io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.DeleteClusterRequest,
          com.google.protobuf.Empty>) methodMap.get(
          CONFIG.deleteCluster.getName());
      undeleteCluster = (io.grpc.MethodDescriptor<com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest,
          com.google.longrunning.Operation>) methodMap.get(
          CONFIG.undeleteCluster.getName());
    }

    @java.lang.Override
    protected BigtableClusterServiceServiceDescriptor build(
        java.util.Map<java.lang.String, io.grpc.MethodDescriptor<?, ?>> methodMap) {
      return new BigtableClusterServiceServiceDescriptor(methodMap);
    }

    @java.lang.Override
    public com.google.common.collect.ImmutableList<io.grpc.MethodDescriptor<?, ?>> methods() {
      return com.google.common.collect.ImmutableList.<io.grpc.MethodDescriptor<?, ?>>of(
          listZones,
          getCluster,
          listClusters,
          createCluster,
          updateCluster,
          deleteCluster,
          undeleteCluster);
    }
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

  public static class BigtableClusterServiceStub extends
      io.grpc.stub.AbstractStub<BigtableClusterServiceStub, BigtableClusterServiceServiceDescriptor>
      implements BigtableClusterService {
    private BigtableClusterServiceStub(io.grpc.Channel channel,
        BigtableClusterServiceServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected BigtableClusterServiceStub build(io.grpc.Channel channel,
        BigtableClusterServiceServiceDescriptor config) {
      return new BigtableClusterServiceStub(channel, config);
    }

    @java.lang.Override
    public void listZones(com.google.bigtable.admin.cluster.v1.ListZonesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListZonesResponse> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.listZones), request, responseObserver);
    }

    @java.lang.Override
    public void getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.getCluster), request, responseObserver);
    }

    @java.lang.Override
    public void listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListClustersResponse> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.listClusters), request, responseObserver);
    }

    @java.lang.Override
    public void createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.createCluster), request, responseObserver);
    }

    @java.lang.Override
    public void updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.updateCluster), request, responseObserver);
    }

    @java.lang.Override
    public void deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.deleteCluster), request, responseObserver);
    }

    @java.lang.Override
    public void undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.undeleteCluster), request, responseObserver);
    }
  }

  public static class BigtableClusterServiceBlockingStub extends
      io.grpc.stub.AbstractStub<BigtableClusterServiceBlockingStub, BigtableClusterServiceServiceDescriptor>
      implements BigtableClusterServiceBlockingClient {
    private BigtableClusterServiceBlockingStub(io.grpc.Channel channel,
        BigtableClusterServiceServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected BigtableClusterServiceBlockingStub build(io.grpc.Channel channel,
        BigtableClusterServiceServiceDescriptor config) {
      return new BigtableClusterServiceBlockingStub(channel, config);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.ListZonesResponse listZones(com.google.bigtable.admin.cluster.v1.ListZonesRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.listZones), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.Cluster getCluster(com.google.bigtable.admin.cluster.v1.GetClusterRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.getCluster), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.ListClustersResponse listClusters(com.google.bigtable.admin.cluster.v1.ListClustersRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.listClusters), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.Cluster createCluster(com.google.bigtable.admin.cluster.v1.CreateClusterRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.createCluster), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.cluster.v1.Cluster updateCluster(com.google.bigtable.admin.cluster.v1.Cluster request) {
      return blockingUnaryCall(
          channel.newCall(config.updateCluster), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteCluster(com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.deleteCluster), request);
    }

    @java.lang.Override
    public com.google.longrunning.Operation undeleteCluster(com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.undeleteCluster), request);
    }
  }

  public static class BigtableClusterServiceFutureStub extends
      io.grpc.stub.AbstractStub<BigtableClusterServiceFutureStub, BigtableClusterServiceServiceDescriptor>
      implements BigtableClusterServiceFutureClient {
    private BigtableClusterServiceFutureStub(io.grpc.Channel channel,
        BigtableClusterServiceServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected BigtableClusterServiceFutureStub build(io.grpc.Channel channel,
        BigtableClusterServiceServiceDescriptor config) {
      return new BigtableClusterServiceFutureStub(channel, config);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.ListZonesResponse> listZones(
        com.google.bigtable.admin.cluster.v1.ListZonesRequest request) {
      return unaryFutureCall(
          channel.newCall(config.listZones), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> getCluster(
        com.google.bigtable.admin.cluster.v1.GetClusterRequest request) {
      return unaryFutureCall(
          channel.newCall(config.getCluster), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.ListClustersResponse> listClusters(
        com.google.bigtable.admin.cluster.v1.ListClustersRequest request) {
      return unaryFutureCall(
          channel.newCall(config.listClusters), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> createCluster(
        com.google.bigtable.admin.cluster.v1.CreateClusterRequest request) {
      return unaryFutureCall(
          channel.newCall(config.createCluster), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.cluster.v1.Cluster> updateCluster(
        com.google.bigtable.admin.cluster.v1.Cluster request) {
      return unaryFutureCall(
          channel.newCall(config.updateCluster), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteCluster(
        com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request) {
      return unaryFutureCall(
          channel.newCall(config.deleteCluster), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> undeleteCluster(
        com.google.bigtable.admin.cluster.v1.UndeleteClusterRequest request) {
      return unaryFutureCall(
          channel.newCall(config.undeleteCluster), request);
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final BigtableClusterService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder("google.bigtable.admin.cluster.v1.BigtableClusterService")
      .addMethod(createMethodDefinition(
          METHOD_LIST_ZONES,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.cluster.v1.ListZonesRequest,
                com.google.bigtable.admin.cluster.v1.ListZonesResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.ListZonesRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListZonesResponse> responseObserver) {
                serviceImpl.listZones(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_GET_CLUSTER,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.cluster.v1.GetClusterRequest,
                com.google.bigtable.admin.cluster.v1.Cluster>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.GetClusterRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
                serviceImpl.getCluster(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_LIST_CLUSTERS,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.cluster.v1.ListClustersRequest,
                com.google.bigtable.admin.cluster.v1.ListClustersResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.ListClustersRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.ListClustersResponse> responseObserver) {
                serviceImpl.listClusters(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_CREATE_CLUSTER,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.cluster.v1.CreateClusterRequest,
                com.google.bigtable.admin.cluster.v1.Cluster>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.CreateClusterRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
                serviceImpl.createCluster(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_UPDATE_CLUSTER,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.cluster.v1.Cluster,
                com.google.bigtable.admin.cluster.v1.Cluster>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.Cluster request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.cluster.v1.Cluster> responseObserver) {
                serviceImpl.updateCluster(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_DELETE_CLUSTER,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.cluster.v1.DeleteClusterRequest,
                com.google.protobuf.Empty>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.cluster.v1.DeleteClusterRequest request,
                  io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
                serviceImpl.deleteCluster(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_UNDELETE_CLUSTER,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
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
