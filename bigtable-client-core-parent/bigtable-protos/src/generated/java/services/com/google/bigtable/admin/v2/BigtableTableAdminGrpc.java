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
 * Service for creating, configuring, and deleting Cloud Bigtable tables.
 * Provides access to the table schemas only, not the data stored within
 * the tables.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 0.14.1)",
    comments = "Source: google/bigtable/admin/v2/bigtable_table_admin.proto")
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
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.v2.DropRowRangeRequest,
      com.google.protobuf.Empty> METHOD_DROP_ROW_RANGE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.v2.BigtableTableAdmin", "DropRowRange"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.v2.DropRowRangeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BigtableTableAdminStub newStub(io.grpc.Channel channel) {
    return new BigtableTableAdminStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BigtableTableAdminBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableTableAdminBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static BigtableTableAdminFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableTableAdminFutureStub(channel);
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable tables.
   * Provides access to the table schemas only, not the data stored within
   * the tables.
   * </pre>
   */
  public static interface BigtableTableAdmin {

    /**
     * <pre>
     * Creates a new table in the specified instance.
     * The table can be created with a full set of initial column families,
     * specified in the request.
     * </pre>
     */
    public void createTable(com.google.bigtable.admin.v2.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver);

    /**
     * <pre>
     * Lists all tables served from a specified instance.
     * </pre>
     */
    public void listTables(com.google.bigtable.admin.v2.ListTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListTablesResponse> responseObserver);

    /**
     * <pre>
     * Gets metadata information about the specified table.
     * </pre>
     */
    public void getTable(com.google.bigtable.admin.v2.GetTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver);

    /**
     * <pre>
     * Permanently deletes a specified table and all of its data.
     * </pre>
     */
    public void deleteTable(com.google.bigtable.admin.v2.DeleteTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    /**
     * <pre>
     * Atomically performs a series of column family modifications
     * on the specified table.
     * </pre>
     */
    public void modifyColumnFamilies(com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver);

    /**
     * <pre>
     * Permanently drop/delete a row range from a specified table. The request can
     * specify whether to delete all rows in a table, or only those that match a
     * particular prefix.
     * </pre>
     */
    public void dropRowRange(com.google.bigtable.admin.v2.DropRowRangeRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);
  }

  @io.grpc.ExperimentalApi
  public static abstract class AbstractBigtableTableAdmin implements BigtableTableAdmin, io.grpc.BindableService {

    @java.lang.Override
    public void createTable(com.google.bigtable.admin.v2.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_TABLE, responseObserver);
    }

    @java.lang.Override
    public void listTables(com.google.bigtable.admin.v2.ListTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.ListTablesResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_TABLES, responseObserver);
    }

    @java.lang.Override
    public void getTable(com.google.bigtable.admin.v2.GetTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_TABLE, responseObserver);
    }

    @java.lang.Override
    public void deleteTable(com.google.bigtable.admin.v2.DeleteTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_TABLE, responseObserver);
    }

    @java.lang.Override
    public void modifyColumnFamilies(com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_MODIFY_COLUMN_FAMILIES, responseObserver);
    }

    @java.lang.Override
    public void dropRowRange(com.google.bigtable.admin.v2.DropRowRangeRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DROP_ROW_RANGE, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return BigtableTableAdminGrpc.bindService(this);
    }
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable tables.
   * Provides access to the table schemas only, not the data stored within
   * the tables.
   * </pre>
   */
  public static interface BigtableTableAdminBlockingClient {

    /**
     * <pre>
     * Creates a new table in the specified instance.
     * The table can be created with a full set of initial column families,
     * specified in the request.
     * </pre>
     */
    public com.google.bigtable.admin.v2.Table createTable(com.google.bigtable.admin.v2.CreateTableRequest request);

    /**
     * <pre>
     * Lists all tables served from a specified instance.
     * </pre>
     */
    public com.google.bigtable.admin.v2.ListTablesResponse listTables(com.google.bigtable.admin.v2.ListTablesRequest request);

    /**
     * <pre>
     * Gets metadata information about the specified table.
     * </pre>
     */
    public com.google.bigtable.admin.v2.Table getTable(com.google.bigtable.admin.v2.GetTableRequest request);

    /**
     * <pre>
     * Permanently deletes a specified table and all of its data.
     * </pre>
     */
    public com.google.protobuf.Empty deleteTable(com.google.bigtable.admin.v2.DeleteTableRequest request);

    /**
     * <pre>
     * Atomically performs a series of column family modifications
     * on the specified table.
     * </pre>
     */
    public com.google.bigtable.admin.v2.Table modifyColumnFamilies(com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request);

    /**
     * <pre>
     * Permanently drop/delete a row range from a specified table. The request can
     * specify whether to delete all rows in a table, or only those that match a
     * particular prefix.
     * </pre>
     */
    public com.google.protobuf.Empty dropRowRange(com.google.bigtable.admin.v2.DropRowRangeRequest request);
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable tables.
   * Provides access to the table schemas only, not the data stored within
   * the tables.
   * </pre>
   */
  public static interface BigtableTableAdminFutureClient {

    /**
     * <pre>
     * Creates a new table in the specified instance.
     * The table can be created with a full set of initial column families,
     * specified in the request.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Table> createTable(
        com.google.bigtable.admin.v2.CreateTableRequest request);

    /**
     * <pre>
     * Lists all tables served from a specified instance.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.ListTablesResponse> listTables(
        com.google.bigtable.admin.v2.ListTablesRequest request);

    /**
     * <pre>
     * Gets metadata information about the specified table.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Table> getTable(
        com.google.bigtable.admin.v2.GetTableRequest request);

    /**
     * <pre>
     * Permanently deletes a specified table and all of its data.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteTable(
        com.google.bigtable.admin.v2.DeleteTableRequest request);

    /**
     * <pre>
     * Atomically performs a series of column family modifications
     * on the specified table.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.v2.Table> modifyColumnFamilies(
        com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request);

    /**
     * <pre>
     * Permanently drop/delete a row range from a specified table. The request can
     * specify whether to delete all rows in a table, or only those that match a
     * particular prefix.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> dropRowRange(
        com.google.bigtable.admin.v2.DropRowRangeRequest request);
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

    @java.lang.Override
    public void dropRowRange(com.google.bigtable.admin.v2.DropRowRangeRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DROP_ROW_RANGE, getCallOptions()), request, responseObserver);
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

    @java.lang.Override
    public com.google.protobuf.Empty dropRowRange(com.google.bigtable.admin.v2.DropRowRangeRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DROP_ROW_RANGE, getCallOptions(), request);
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

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> dropRowRange(
        com.google.bigtable.admin.v2.DropRowRangeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DROP_ROW_RANGE, getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_TABLE = 0;
  private static final int METHODID_LIST_TABLES = 1;
  private static final int METHODID_GET_TABLE = 2;
  private static final int METHODID_DELETE_TABLE = 3;
  private static final int METHODID_MODIFY_COLUMN_FAMILIES = 4;
  private static final int METHODID_DROP_ROW_RANGE = 5;

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

    @java.lang.Override
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
        case METHODID_DROP_ROW_RANGE:
          serviceImpl.dropRowRange((com.google.bigtable.admin.v2.DropRowRangeRequest) request,
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
        .addMethod(
          METHOD_DROP_ROW_RANGE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.admin.v2.DropRowRangeRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_DROP_ROW_RANGE)))
        .build();
  }
}
