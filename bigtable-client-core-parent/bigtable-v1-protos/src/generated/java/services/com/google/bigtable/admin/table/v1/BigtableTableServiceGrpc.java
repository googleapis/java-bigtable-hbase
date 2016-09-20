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
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 * <pre>
 * Service for creating, configuring, and deleting Cloud Bigtable tables.
 * Provides access to the table schemas only, not the data stored within the tables.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.0)",
    comments = "Source: google/bigtable/admin/table/v1/bigtable_table_service.proto")
public class BigtableTableServiceGrpc {

  private BigtableTableServiceGrpc() {}

  public static final String SERVICE_NAME = "google.bigtable.admin.table.v1.BigtableTableService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.CreateTableRequest,
      com.google.bigtable.admin.table.v1.Table> METHOD_CREATE_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "CreateTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.CreateTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.Table.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.ListTablesRequest,
      com.google.bigtable.admin.table.v1.ListTablesResponse> METHOD_LIST_TABLES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "ListTables"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ListTablesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ListTablesResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.GetTableRequest,
      com.google.bigtable.admin.table.v1.Table> METHOD_GET_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "GetTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.GetTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.Table.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.DeleteTableRequest,
      com.google.protobuf.Empty> METHOD_DELETE_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "DeleteTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.DeleteTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.RenameTableRequest,
      com.google.protobuf.Empty> METHOD_RENAME_TABLE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "RenameTable"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.RenameTableRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest,
      com.google.bigtable.admin.table.v1.ColumnFamily> METHOD_CREATE_COLUMN_FAMILY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "CreateColumnFamily"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ColumnFamily.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.ColumnFamily,
      com.google.bigtable.admin.table.v1.ColumnFamily> METHOD_UPDATE_COLUMN_FAMILY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "UpdateColumnFamily"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ColumnFamily.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ColumnFamily.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest,
      com.google.protobuf.Empty> METHOD_DELETE_COLUMN_FAMILY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "DeleteColumnFamily"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest,
      com.google.protobuf.Empty> METHOD_BULK_DELETE_ROWS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.admin.table.v1.BigtableTableService", "BulkDeleteRows"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BigtableTableServiceStub newStub(io.grpc.Channel channel) {
    return new BigtableTableServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BigtableTableServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableTableServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static BigtableTableServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableTableServiceFutureStub(channel);
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable tables.
   * Provides access to the table schemas only, not the data stored within the tables.
   * </pre>
   */
  public static abstract class BigtableTableServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Creates a new table, to be served from a specified cluster.
     * The table can be created with a full set of initial column families,
     * specified in the request.
     * </pre>
     */
    public void createTable(com.google.bigtable.admin.table.v1.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_TABLE, responseObserver);
    }

    /**
     * <pre>
     * Lists the names of all tables served from a specified cluster.
     * </pre>
     */
    public void listTables(com.google.bigtable.admin.table.v1.ListTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ListTablesResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_TABLES, responseObserver);
    }

    /**
     * <pre>
     * Gets the schema of the specified table, including its column families.
     * </pre>
     */
    public void getTable(com.google.bigtable.admin.table.v1.GetTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_TABLE, responseObserver);
    }

    /**
     * <pre>
     * Permanently deletes a specified table and all of its data.
     * </pre>
     */
    public void deleteTable(com.google.bigtable.admin.table.v1.DeleteTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_TABLE, responseObserver);
    }

    /**
     * <pre>
     * Changes the name of a specified table.
     * Cannot be used to move tables between clusters, zones, or projects.
     * </pre>
     */
    public void renameTable(com.google.bigtable.admin.table.v1.RenameTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RENAME_TABLE, responseObserver);
    }

    /**
     * <pre>
     * Creates a new column family within a specified table.
     * </pre>
     */
    public void createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_COLUMN_FAMILY, responseObserver);
    }

    /**
     * <pre>
     * Changes the configuration of a specified column family.
     * </pre>
     */
    public void updateColumnFamily(com.google.bigtable.admin.table.v1.ColumnFamily request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UPDATE_COLUMN_FAMILY, responseObserver);
    }

    /**
     * <pre>
     * Permanently deletes a specified column family and all of its data.
     * </pre>
     */
    public void deleteColumnFamily(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_COLUMN_FAMILY, responseObserver);
    }

    /**
     * <pre>
     * Delete all rows in a table corresponding to a particular prefix
     * </pre>
     */
    public void bulkDeleteRows(com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_BULK_DELETE_ROWS, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_CREATE_TABLE,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.table.v1.CreateTableRequest,
                com.google.bigtable.admin.table.v1.Table>(
                  this, METHODID_CREATE_TABLE)))
          .addMethod(
            METHOD_LIST_TABLES,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.table.v1.ListTablesRequest,
                com.google.bigtable.admin.table.v1.ListTablesResponse>(
                  this, METHODID_LIST_TABLES)))
          .addMethod(
            METHOD_GET_TABLE,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.table.v1.GetTableRequest,
                com.google.bigtable.admin.table.v1.Table>(
                  this, METHODID_GET_TABLE)))
          .addMethod(
            METHOD_DELETE_TABLE,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.table.v1.DeleteTableRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_DELETE_TABLE)))
          .addMethod(
            METHOD_RENAME_TABLE,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.table.v1.RenameTableRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_RENAME_TABLE)))
          .addMethod(
            METHOD_CREATE_COLUMN_FAMILY,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest,
                com.google.bigtable.admin.table.v1.ColumnFamily>(
                  this, METHODID_CREATE_COLUMN_FAMILY)))
          .addMethod(
            METHOD_UPDATE_COLUMN_FAMILY,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.table.v1.ColumnFamily,
                com.google.bigtable.admin.table.v1.ColumnFamily>(
                  this, METHODID_UPDATE_COLUMN_FAMILY)))
          .addMethod(
            METHOD_DELETE_COLUMN_FAMILY,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_DELETE_COLUMN_FAMILY)))
          .addMethod(
            METHOD_BULK_DELETE_ROWS,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_BULK_DELETE_ROWS)))
          .build();
    }
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable tables.
   * Provides access to the table schemas only, not the data stored within the tables.
   * </pre>
   */
  public static final class BigtableTableServiceStub extends io.grpc.stub.AbstractStub<BigtableTableServiceStub> {
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

    /**
     * <pre>
     * Creates a new table, to be served from a specified cluster.
     * The table can be created with a full set of initial column families,
     * specified in the request.
     * </pre>
     */
    public void createTable(com.google.bigtable.admin.table.v1.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Lists the names of all tables served from a specified cluster.
     * </pre>
     */
    public void listTables(com.google.bigtable.admin.table.v1.ListTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ListTablesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLES, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Gets the schema of the specified table, including its column families.
     * </pre>
     */
    public void getTable(com.google.bigtable.admin.table.v1.GetTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_TABLE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Permanently deletes a specified table and all of its data.
     * </pre>
     */
    public void deleteTable(com.google.bigtable.admin.table.v1.DeleteTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_TABLE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Changes the name of a specified table.
     * Cannot be used to move tables between clusters, zones, or projects.
     * </pre>
     */
    public void renameTable(com.google.bigtable.admin.table.v1.RenameTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RENAME_TABLE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Creates a new column family within a specified table.
     * </pre>
     */
    public void createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_COLUMN_FAMILY, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Changes the configuration of a specified column family.
     * </pre>
     */
    public void updateColumnFamily(com.google.bigtable.admin.table.v1.ColumnFamily request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_COLUMN_FAMILY, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Permanently deletes a specified column family and all of its data.
     * </pre>
     */
    public void deleteColumnFamily(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_COLUMN_FAMILY, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete all rows in a table corresponding to a particular prefix
     * </pre>
     */
    public void bulkDeleteRows(com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_BULK_DELETE_ROWS, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable tables.
   * Provides access to the table schemas only, not the data stored within the tables.
   * </pre>
   */
  public static final class BigtableTableServiceBlockingStub extends io.grpc.stub.AbstractStub<BigtableTableServiceBlockingStub> {
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

    /**
     * <pre>
     * Creates a new table, to be served from a specified cluster.
     * The table can be created with a full set of initial column families,
     * specified in the request.
     * </pre>
     */
    public com.google.bigtable.admin.table.v1.Table createTable(com.google.bigtable.admin.table.v1.CreateTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_TABLE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Lists the names of all tables served from a specified cluster.
     * </pre>
     */
    public com.google.bigtable.admin.table.v1.ListTablesResponse listTables(com.google.bigtable.admin.table.v1.ListTablesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_TABLES, getCallOptions(), request);
    }

    /**
     * <pre>
     * Gets the schema of the specified table, including its column families.
     * </pre>
     */
    public com.google.bigtable.admin.table.v1.Table getTable(com.google.bigtable.admin.table.v1.GetTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_TABLE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Permanently deletes a specified table and all of its data.
     * </pre>
     */
    public com.google.protobuf.Empty deleteTable(com.google.bigtable.admin.table.v1.DeleteTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_TABLE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Changes the name of a specified table.
     * Cannot be used to move tables between clusters, zones, or projects.
     * </pre>
     */
    public com.google.protobuf.Empty renameTable(com.google.bigtable.admin.table.v1.RenameTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RENAME_TABLE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Creates a new column family within a specified table.
     * </pre>
     */
    public com.google.bigtable.admin.table.v1.ColumnFamily createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_COLUMN_FAMILY, getCallOptions(), request);
    }

    /**
     * <pre>
     * Changes the configuration of a specified column family.
     * </pre>
     */
    public com.google.bigtable.admin.table.v1.ColumnFamily updateColumnFamily(com.google.bigtable.admin.table.v1.ColumnFamily request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_COLUMN_FAMILY, getCallOptions(), request);
    }

    /**
     * <pre>
     * Permanently deletes a specified column family and all of its data.
     * </pre>
     */
    public com.google.protobuf.Empty deleteColumnFamily(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_COLUMN_FAMILY, getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete all rows in a table corresponding to a particular prefix
     * </pre>
     */
    public com.google.protobuf.Empty bulkDeleteRows(com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_BULK_DELETE_ROWS, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Service for creating, configuring, and deleting Cloud Bigtable tables.
   * Provides access to the table schemas only, not the data stored within the tables.
   * </pre>
   */
  public static final class BigtableTableServiceFutureStub extends io.grpc.stub.AbstractStub<BigtableTableServiceFutureStub> {
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

    /**
     * <pre>
     * Creates a new table, to be served from a specified cluster.
     * The table can be created with a full set of initial column families,
     * specified in the request.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.Table> createTable(
        com.google.bigtable.admin.table.v1.CreateTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_TABLE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Lists the names of all tables served from a specified cluster.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ListTablesResponse> listTables(
        com.google.bigtable.admin.table.v1.ListTablesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLES, getCallOptions()), request);
    }

    /**
     * <pre>
     * Gets the schema of the specified table, including its column families.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.Table> getTable(
        com.google.bigtable.admin.table.v1.GetTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_TABLE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Permanently deletes a specified table and all of its data.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteTable(
        com.google.bigtable.admin.table.v1.DeleteTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_TABLE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Changes the name of a specified table.
     * Cannot be used to move tables between clusters, zones, or projects.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> renameTable(
        com.google.bigtable.admin.table.v1.RenameTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RENAME_TABLE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Creates a new column family within a specified table.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ColumnFamily> createColumnFamily(
        com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_COLUMN_FAMILY, getCallOptions()), request);
    }

    /**
     * <pre>
     * Changes the configuration of a specified column family.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ColumnFamily> updateColumnFamily(
        com.google.bigtable.admin.table.v1.ColumnFamily request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_COLUMN_FAMILY, getCallOptions()), request);
    }

    /**
     * <pre>
     * Permanently deletes a specified column family and all of its data.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteColumnFamily(
        com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_COLUMN_FAMILY, getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete all rows in a table corresponding to a particular prefix
     * </pre>
     */
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
    private final BigtableTableServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(BigtableTableServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
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

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_CREATE_TABLE,
        METHOD_LIST_TABLES,
        METHOD_GET_TABLE,
        METHOD_DELETE_TABLE,
        METHOD_RENAME_TABLE,
        METHOD_CREATE_COLUMN_FAMILY,
        METHOD_UPDATE_COLUMN_FAMILY,
        METHOD_DELETE_COLUMN_FAMILY,
        METHOD_BULK_DELETE_ROWS);
  }

}
