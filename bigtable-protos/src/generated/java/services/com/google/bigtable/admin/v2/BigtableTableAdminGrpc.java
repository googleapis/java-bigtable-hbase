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
public class BigtableTableAdminGrpc {

  private BigtableTableAdminGrpc() {}

  public static final String SERVICE_NAME = "google.bigtable.admin.v2.BigtableTableAdmin";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.CreateTableRequest,
      com.google.bigtable.admin.v2.Table> METHOD_CREATE_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableTableAdmin", "CreateTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.CreateTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.Table.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.ListTablesRequest,
      com.google.bigtable.admin.v2.ListTablesResponse> METHOD_LIST_TABLES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableTableAdmin", "ListTables"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.ListTablesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.ListTablesResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.GetTableRequest,
      com.google.bigtable.admin.v2.Table> METHOD_GET_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableTableAdmin", "GetTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.GetTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.Table.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.DeleteTableRequest,
      com.google.protobuf.Empty> METHOD_DELETE_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableTableAdmin", "DeleteTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.DeleteTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest,
      com.google.bigtable.admin.v2.Table> METHOD_MODIFY_COLUMN_FAMILIES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableTableAdmin", "ModifyColumnFamilies"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.Table.getDefaultInstance()));

  public static BigtableTableAdminStub newStub(io.grpc.Channel channel) {
    return new BigtableTableAdminStub(channel);
  }

  public static BigtableTableAdminBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableTableAdminBlockingStub(channel);
  }

  public static BigtableTableAdminFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableTableAdminFutureStub(channel);
  }

  public static interface BigtableTableAdmin {

    public void createTable(com.google.bigtable.admin.v2.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver);

    public void listTables(com.google.bigtable.admin.v2.ListTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListTablesResponse> responseObserver);

    public void getTable(com.google.bigtable.admin.v2.GetTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver);

    public void deleteTable(com.google.bigtable.admin.v2.DeleteTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    public void modifyColumnFamilies(com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver);
  }

  public static interface BigtableTableAdminBlockingClient {

    public com.google.bigtable.admin.v2.Table createTable(com.google.bigtable.admin.v2.CreateTableRequest request);

    public com.google.bigtable.admin.v2.ListTablesResponse listTables(com.google.bigtable.admin.v2.ListTablesRequest request);

    public com.google.bigtable.admin.v2.Table getTable(com.google.bigtable.admin.v2.GetTableRequest request);

    public com.google.protobuf.Empty deleteTable(com.google.bigtable.admin.v2.DeleteTableRequest request);

    public com.google.bigtable.admin.v2.Table modifyColumnFamilies(com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request);
  }

  public static interface BigtableTableAdminFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Table> createTable(
        com.google.bigtable.admin.v2.CreateTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.ListTablesResponse> listTables(
        com.google.bigtable.admin.v2.ListTablesRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Table> getTable(
        com.google.bigtable.admin.v2.GetTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteTable(
        com.google.bigtable.admin.v2.DeleteTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Table> modifyColumnFamilies(
        com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request);
  }

  public static class BigtableTableAdminStub extends io.grpc.stub.AbstractStub<BigtableTableAdminStub>
      implements BigtableTableAdmin {
    private BigtableTableAdminStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableTableAdminStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableTableAdminStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableTableAdminStub(channel, callOptions);
    }

    @java.lang.Override
    public void createTable(com.google.bigtable.admin.v2.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void listTables(com.google.bigtable.admin.v2.ListTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListTablesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLES, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void getTable(com.google.bigtable.admin.v2.GetTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void deleteTable(com.google.bigtable.admin.v2.DeleteTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void modifyColumnFamilies(com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MODIFY_COLUMN_FAMILIES, getCallOptions()), request, responseObserver);
    }
  }

  public static class BigtableTableAdminBlockingStub extends io.grpc.stub.AbstractStub<BigtableTableAdminBlockingStub>
      implements BigtableTableAdminBlockingClient {
    private BigtableTableAdminBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableTableAdminBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableTableAdminBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableTableAdminBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.bigtable.admin.v2.Table createTable(com.google.bigtable.admin.v2.CreateTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.v2.ListTablesResponse listTables(com.google.bigtable.admin.v2.ListTablesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_TABLES, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.v2.Table getTable(com.google.bigtable.admin.v2.GetTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteTable(com.google.bigtable.admin.v2.DeleteTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.v2.Table modifyColumnFamilies(com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MODIFY_COLUMN_FAMILIES, getCallOptions(), request);
    }
  }

  public static class BigtableTableAdminFutureStub extends io.grpc.stub.AbstractStub<BigtableTableAdminFutureStub>
      implements BigtableTableAdminFutureClient {
    private BigtableTableAdminFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableTableAdminFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableTableAdminFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableTableAdminFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Table> createTable(
        com.google.bigtable.admin.v2.CreateTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.ListTablesResponse> listTables(
        com.google.bigtable.admin.v2.ListTablesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLES, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Table> getTable(
        com.google.bigtable.admin.v2.GetTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteTable(
        com.google.bigtable.admin.v2.DeleteTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Table> modifyColumnFamilies(
        com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_MODIFY_COLUMN_FAMILIES, getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_TABLE = 0;
  private static final int METHODID_LIST_TABLES = 1;
  private static final int METHODID_GET_TABLE = 2;
  private static final int METHODID_DELETE_TABLE = 3;
  private static final int METHODID_MODIFY_COLUMN_FAMILIES = 4;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BigtableTableAdmin serviceImpl;
    private final int methodId;

    public MethodHandlers(BigtableTableAdmin serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_TABLE:
          serviceImpl.createTable((com.google.bigtable.admin.v2.CreateTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table>) responseObserver);
          break;
        case METHODID_LIST_TABLES:
          serviceImpl.listTables((com.google.bigtable.admin.v2.ListTablesRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListTablesResponse>) responseObserver);
          break;
        case METHODID_GET_TABLE:
          serviceImpl.getTable((com.google.bigtable.admin.v2.GetTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table>) responseObserver);
          break;
        case METHODID_DELETE_TABLE:
          serviceImpl.deleteTable((com.google.bigtable.admin.v2.DeleteTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_MODIFY_COLUMN_FAMILIES:
          serviceImpl.modifyColumnFamilies((com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table>) responseObserver);
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
      final BigtableTableAdmin serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_CREATE_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.CreateTableRequest,
              com.google.bigtable.admin.v2.Table>(
                serviceImpl, METHODID_CREATE_TABLE)))
        .addMethod(
          METHOD_LIST_TABLES,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.ListTablesRequest,
              com.google.bigtable.admin.v2.ListTablesResponse>(
                serviceImpl, METHODID_LIST_TABLES)))
        .addMethod(
          METHOD_GET_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.GetTableRequest,
              com.google.bigtable.admin.v2.Table>(
                serviceImpl, METHODID_GET_TABLE)))
        .addMethod(
          METHOD_DELETE_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.DeleteTableRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_DELETE_TABLE)))
        .addMethod(
          METHOD_MODIFY_COLUMN_FAMILIES,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest,
              com.google.bigtable.admin.v2.Table>(
                serviceImpl, METHODID_MODIFY_COLUMN_FAMILIES)))
        .build();
  }
}
