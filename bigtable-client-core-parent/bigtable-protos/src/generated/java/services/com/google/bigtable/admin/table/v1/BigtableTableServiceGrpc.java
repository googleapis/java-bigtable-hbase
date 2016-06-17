package com.google.bigtable.admin.table.v1;

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
public class BigtableTableServiceGrpc {

  private BigtableTableServiceGrpc() {}

  public static final String SERVICE_NAME = "google.bigtable.admin.table.v1.BigtableTableService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.CreateTableRequest,
      com.google.bigtable.admin.table.v1.Table> METHOD_CREATE_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "CreateTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.CreateTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.Table.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.ListTablesRequest,
      com.google.bigtable.admin.table.v1.ListTablesResponse> METHOD_LIST_TABLES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "ListTables"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ListTablesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ListTablesResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.GetTableRequest,
      com.google.bigtable.admin.table.v1.Table> METHOD_GET_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "GetTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.GetTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.Table.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.DeleteTableRequest,
      com.google.protobuf.Empty> METHOD_DELETE_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "DeleteTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.DeleteTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.RenameTableRequest,
      com.google.protobuf.Empty> METHOD_RENAME_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "RenameTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.RenameTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest,
      com.google.bigtable.admin.table.v1.ColumnFamily> METHOD_CREATE_COLUMN_FAMILY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "CreateColumnFamily"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ColumnFamily.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.ColumnFamily,
      com.google.bigtable.admin.table.v1.ColumnFamily> METHOD_UPDATE_COLUMN_FAMILY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "UpdateColumnFamily"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ColumnFamily.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ColumnFamily.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest,
      com.google.protobuf.Empty> METHOD_DELETE_COLUMN_FAMILY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "DeleteColumnFamily"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest,
      com.google.protobuf.Empty> METHOD_BULK_DELETE_ROWS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "BulkDeleteRows"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));

  public static BigtableTableServiceStub newStub(io.grpc.Channel channel) {
    return new BigtableTableServiceStub(channel);
  }

  public static BigtableTableServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableTableServiceBlockingStub(channel);
  }

  public static BigtableTableServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableTableServiceFutureStub(channel);
  }

  public static interface BigtableTableService {

    public void createTable(com.google.bigtable.admin.table.v1.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver);

    public void listTables(com.google.bigtable.admin.table.v1.ListTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ListTablesResponse> responseObserver);

    public void getTable(com.google.bigtable.admin.table.v1.GetTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver);

    public void deleteTable(com.google.bigtable.admin.table.v1.DeleteTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    public void renameTable(com.google.bigtable.admin.table.v1.RenameTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    public void createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver);

    public void updateColumnFamily(com.google.bigtable.admin.table.v1.ColumnFamily request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver);

    public void deleteColumnFamily(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    public void bulkDeleteRows(com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);
  }

  public static interface BigtableTableServiceBlockingClient {

    public com.google.bigtable.admin.table.v1.Table createTable(com.google.bigtable.admin.table.v1.CreateTableRequest request);

    public com.google.bigtable.admin.table.v1.ListTablesResponse listTables(com.google.bigtable.admin.table.v1.ListTablesRequest request);

    public com.google.bigtable.admin.table.v1.Table getTable(com.google.bigtable.admin.table.v1.GetTableRequest request);

    public com.google.protobuf.Empty deleteTable(com.google.bigtable.admin.table.v1.DeleteTableRequest request);

    public com.google.protobuf.Empty renameTable(com.google.bigtable.admin.table.v1.RenameTableRequest request);

    public com.google.bigtable.admin.table.v1.ColumnFamily createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request);

    public com.google.bigtable.admin.table.v1.ColumnFamily updateColumnFamily(com.google.bigtable.admin.table.v1.ColumnFamily request);

    public com.google.protobuf.Empty deleteColumnFamily(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request);

    public com.google.protobuf.Empty bulkDeleteRows(com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest request);
  }

  public static interface BigtableTableServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.Table> createTable(
        com.google.bigtable.admin.table.v1.CreateTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ListTablesResponse> listTables(
        com.google.bigtable.admin.table.v1.ListTablesRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.Table> getTable(
        com.google.bigtable.admin.table.v1.GetTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteTable(
        com.google.bigtable.admin.table.v1.DeleteTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> renameTable(
        com.google.bigtable.admin.table.v1.RenameTableRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ColumnFamily> createColumnFamily(
        com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ColumnFamily> updateColumnFamily(
        com.google.bigtable.admin.table.v1.ColumnFamily request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteColumnFamily(
        com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> bulkDeleteRows(
        com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest request);
  }

  public static class BigtableTableServiceStub extends io.grpc.stub.AbstractStub<BigtableTableServiceStub>
      implements BigtableTableService {
    private BigtableTableServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableTableServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableTableServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableTableServiceStub(channel, callOptions);
    }

    @java.lang.Override
    public void createTable(com.google.bigtable.admin.table.v1.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void listTables(com.google.bigtable.admin.table.v1.ListTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ListTablesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLES, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void getTable(com.google.bigtable.admin.table.v1.GetTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void deleteTable(com.google.bigtable.admin.table.v1.DeleteTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void renameTable(com.google.bigtable.admin.table.v1.RenameTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RENAME_TABLE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_COLUMN_FAMILY, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void updateColumnFamily(com.google.bigtable.admin.table.v1.ColumnFamily request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_COLUMN_FAMILY, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void deleteColumnFamily(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_COLUMN_FAMILY, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void bulkDeleteRows(com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_BULK_DELETE_ROWS, getCallOptions()), request, responseObserver);
    }
  }

  public static class BigtableTableServiceBlockingStub extends io.grpc.stub.AbstractStub<BigtableTableServiceBlockingStub>
      implements BigtableTableServiceBlockingClient {
    private BigtableTableServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableTableServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableTableServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableTableServiceBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.Table createTable(com.google.bigtable.admin.table.v1.CreateTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.ListTablesResponse listTables(com.google.bigtable.admin.table.v1.ListTablesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_TABLES, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.Table getTable(com.google.bigtable.admin.table.v1.GetTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteTable(com.google.bigtable.admin.table.v1.DeleteTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty renameTable(com.google.bigtable.admin.table.v1.RenameTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RENAME_TABLE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.ColumnFamily createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_COLUMN_FAMILY, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.ColumnFamily updateColumnFamily(com.google.bigtable.admin.table.v1.ColumnFamily request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_COLUMN_FAMILY, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteColumnFamily(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_COLUMN_FAMILY, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty bulkDeleteRows(com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_BULK_DELETE_ROWS, getCallOptions(), request);
    }
  }

  public static class BigtableTableServiceFutureStub extends io.grpc.stub.AbstractStub<BigtableTableServiceFutureStub>
      implements BigtableTableServiceFutureClient {
    private BigtableTableServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableTableServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableTableServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableTableServiceFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.Table> createTable(
        com.google.bigtable.admin.table.v1.CreateTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ListTablesResponse> listTables(
        com.google.bigtable.admin.table.v1.ListTablesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLES, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.Table> getTable(
        com.google.bigtable.admin.table.v1.GetTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteTable(
        com.google.bigtable.admin.table.v1.DeleteTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> renameTable(
        com.google.bigtable.admin.table.v1.RenameTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RENAME_TABLE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ColumnFamily> createColumnFamily(
        com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_COLUMN_FAMILY, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ColumnFamily> updateColumnFamily(
        com.google.bigtable.admin.table.v1.ColumnFamily request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_COLUMN_FAMILY, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteColumnFamily(
        com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_COLUMN_FAMILY, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> bulkDeleteRows(
        com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_BULK_DELETE_ROWS, getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_TABLE = 0;
  private static final int METHODID_LIST_TABLES = 1;
  private static final int METHODID_GET_TABLE = 2;
  private static final int METHODID_DELETE_TABLE = 3;
  private static final int METHODID_RENAME_TABLE = 4;
  private static final int METHODID_CREATE_COLUMN_FAMILY = 5;
  private static final int METHODID_UPDATE_COLUMN_FAMILY = 6;
  private static final int METHODID_DELETE_COLUMN_FAMILY = 7;
  private static final int METHODID_BULK_DELETE_ROWS = 8;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BigtableTableService serviceImpl;
    private final int methodId;

    public MethodHandlers(BigtableTableService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_TABLE:
          serviceImpl.createTable((com.google.bigtable.admin.table.v1.CreateTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table>) responseObserver);
          break;
        case METHODID_LIST_TABLES:
          serviceImpl.listTables((com.google.bigtable.admin.table.v1.ListTablesRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ListTablesResponse>) responseObserver);
          break;
        case METHODID_GET_TABLE:
          serviceImpl.getTable((com.google.bigtable.admin.table.v1.GetTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table>) responseObserver);
          break;
        case METHODID_DELETE_TABLE:
          serviceImpl.deleteTable((com.google.bigtable.admin.table.v1.DeleteTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_RENAME_TABLE:
          serviceImpl.renameTable((com.google.bigtable.admin.table.v1.RenameTableRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_CREATE_COLUMN_FAMILY:
          serviceImpl.createColumnFamily((com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily>) responseObserver);
          break;
        case METHODID_UPDATE_COLUMN_FAMILY:
          serviceImpl.updateColumnFamily((com.google.bigtable.admin.table.v1.ColumnFamily) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily>) responseObserver);
          break;
        case METHODID_DELETE_COLUMN_FAMILY:
          serviceImpl.deleteColumnFamily((com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_BULK_DELETE_ROWS:
          serviceImpl.bulkDeleteRows((com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest) request,
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
      final BigtableTableService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_CREATE_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.table.v1.CreateTableRequest,
              com.google.bigtable.admin.table.v1.Table>(
                serviceImpl, METHODID_CREATE_TABLE)))
        .addMethod(
          METHOD_LIST_TABLES,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.table.v1.ListTablesRequest,
              com.google.bigtable.admin.table.v1.ListTablesResponse>(
                serviceImpl, METHODID_LIST_TABLES)))
        .addMethod(
          METHOD_GET_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.table.v1.GetTableRequest,
              com.google.bigtable.admin.table.v1.Table>(
                serviceImpl, METHODID_GET_TABLE)))
        .addMethod(
          METHOD_DELETE_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.table.v1.DeleteTableRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_DELETE_TABLE)))
        .addMethod(
          METHOD_RENAME_TABLE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.table.v1.RenameTableRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_RENAME_TABLE)))
        .addMethod(
          METHOD_CREATE_COLUMN_FAMILY,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest,
              com.google.bigtable.admin.table.v1.ColumnFamily>(
                serviceImpl, METHODID_CREATE_COLUMN_FAMILY)))
        .addMethod(
          METHOD_UPDATE_COLUMN_FAMILY,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.table.v1.ColumnFamily,
              com.google.bigtable.admin.table.v1.ColumnFamily>(
                serviceImpl, METHODID_UPDATE_COLUMN_FAMILY)))
        .addMethod(
          METHOD_DELETE_COLUMN_FAMILY,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_DELETE_COLUMN_FAMILY)))
        .addMethod(
          METHOD_BULK_DELETE_ROWS,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_BULK_DELETE_ROWS)))
        .build();
  }
}
