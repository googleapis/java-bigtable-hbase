package com.google.bigtable.admin.table.v1;

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
public class BigtableTableServiceGrpc {

  private static final io.grpc.stub.Method<com.google.bigtable.admin.table.v1.CreateTableRequest,
      com.google.bigtable.admin.table.v1.Table> METHOD_CREATE_TABLE =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "CreateTable",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.CreateTableRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.Table.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.table.v1.ListTablesRequest,
      com.google.bigtable.admin.table.v1.ListTablesResponse> METHOD_LIST_TABLES =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "ListTables",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ListTablesRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ListTablesResponse.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.table.v1.GetTableRequest,
      com.google.bigtable.admin.table.v1.Table> METHOD_GET_TABLE =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "GetTable",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.GetTableRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.Table.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.table.v1.DeleteTableRequest,
      com.google.protobuf.Empty> METHOD_DELETE_TABLE =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "DeleteTable",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.DeleteTableRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.table.v1.RenameTableRequest,
      com.google.protobuf.Empty> METHOD_RENAME_TABLE =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "RenameTable",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.RenameTableRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest,
      com.google.bigtable.admin.table.v1.ColumnFamily> METHOD_CREATE_COLUMN_FAMILY =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "CreateColumnFamily",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ColumnFamily.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.table.v1.ColumnFamily,
      com.google.bigtable.admin.table.v1.ColumnFamily> METHOD_UPDATE_COLUMN_FAMILY =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "UpdateColumnFamily",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ColumnFamily.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.ColumnFamily.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest,
      com.google.protobuf.Empty> METHOD_DELETE_COLUMN_FAMILY =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "DeleteColumnFamily",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.PARSER));

  public static BigtableTableServiceStub newStub(io.grpc.Channel channel) {
    return new BigtableTableServiceStub(channel, CONFIG);
  }

  public static BigtableTableServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableTableServiceBlockingStub(channel, CONFIG);
  }

  public static BigtableTableServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableTableServiceFutureStub(channel, CONFIG);
  }

  public static final BigtableTableServiceServiceDescriptor CONFIG =
      new BigtableTableServiceServiceDescriptor();

  @javax.annotation.concurrent.Immutable
  public static class BigtableTableServiceServiceDescriptor extends
      io.grpc.stub.AbstractServiceDescriptor<BigtableTableServiceServiceDescriptor> {
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.CreateTableRequest,
        com.google.bigtable.admin.table.v1.Table> createTable;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.ListTablesRequest,
        com.google.bigtable.admin.table.v1.ListTablesResponse> listTables;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.GetTableRequest,
        com.google.bigtable.admin.table.v1.Table> getTable;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.DeleteTableRequest,
        com.google.protobuf.Empty> deleteTable;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.RenameTableRequest,
        com.google.protobuf.Empty> renameTable;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest,
        com.google.bigtable.admin.table.v1.ColumnFamily> createColumnFamily;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.ColumnFamily,
        com.google.bigtable.admin.table.v1.ColumnFamily> updateColumnFamily;
    public final io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest,
        com.google.protobuf.Empty> deleteColumnFamily;

    private BigtableTableServiceServiceDescriptor() {
      createTable = createMethodDescriptor(
          "google.bigtable.admin.table.v1.BigtableTableService", METHOD_CREATE_TABLE);
      listTables = createMethodDescriptor(
          "google.bigtable.admin.table.v1.BigtableTableService", METHOD_LIST_TABLES);
      getTable = createMethodDescriptor(
          "google.bigtable.admin.table.v1.BigtableTableService", METHOD_GET_TABLE);
      deleteTable = createMethodDescriptor(
          "google.bigtable.admin.table.v1.BigtableTableService", METHOD_DELETE_TABLE);
      renameTable = createMethodDescriptor(
          "google.bigtable.admin.table.v1.BigtableTableService", METHOD_RENAME_TABLE);
      createColumnFamily = createMethodDescriptor(
          "google.bigtable.admin.table.v1.BigtableTableService", METHOD_CREATE_COLUMN_FAMILY);
      updateColumnFamily = createMethodDescriptor(
          "google.bigtable.admin.table.v1.BigtableTableService", METHOD_UPDATE_COLUMN_FAMILY);
      deleteColumnFamily = createMethodDescriptor(
          "google.bigtable.admin.table.v1.BigtableTableService", METHOD_DELETE_COLUMN_FAMILY);
    }

    @SuppressWarnings("unchecked")
    private BigtableTableServiceServiceDescriptor(
        java.util.Map<java.lang.String, io.grpc.MethodDescriptor<?, ?>> methodMap) {
      createTable = (io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.CreateTableRequest,
          com.google.bigtable.admin.table.v1.Table>) methodMap.get(
          CONFIG.createTable.getName());
      listTables = (io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.ListTablesRequest,
          com.google.bigtable.admin.table.v1.ListTablesResponse>) methodMap.get(
          CONFIG.listTables.getName());
      getTable = (io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.GetTableRequest,
          com.google.bigtable.admin.table.v1.Table>) methodMap.get(
          CONFIG.getTable.getName());
      deleteTable = (io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.DeleteTableRequest,
          com.google.protobuf.Empty>) methodMap.get(
          CONFIG.deleteTable.getName());
      renameTable = (io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.RenameTableRequest,
          com.google.protobuf.Empty>) methodMap.get(
          CONFIG.renameTable.getName());
      createColumnFamily = (io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest,
          com.google.bigtable.admin.table.v1.ColumnFamily>) methodMap.get(
          CONFIG.createColumnFamily.getName());
      updateColumnFamily = (io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.ColumnFamily,
          com.google.bigtable.admin.table.v1.ColumnFamily>) methodMap.get(
          CONFIG.updateColumnFamily.getName());
      deleteColumnFamily = (io.grpc.MethodDescriptor<com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest,
          com.google.protobuf.Empty>) methodMap.get(
          CONFIG.deleteColumnFamily.getName());
    }

    @java.lang.Override
    protected BigtableTableServiceServiceDescriptor build(
        java.util.Map<java.lang.String, io.grpc.MethodDescriptor<?, ?>> methodMap) {
      return new BigtableTableServiceServiceDescriptor(methodMap);
    }

    @java.lang.Override
    public com.google.common.collect.ImmutableList<io.grpc.MethodDescriptor<?, ?>> methods() {
      return com.google.common.collect.ImmutableList.<io.grpc.MethodDescriptor<?, ?>>of(
          createTable,
          listTables,
          getTable,
          deleteTable,
          renameTable,
          createColumnFamily,
          updateColumnFamily,
          deleteColumnFamily);
    }
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
  }

  public static class BigtableTableServiceStub extends
      io.grpc.stub.AbstractStub<BigtableTableServiceStub, BigtableTableServiceServiceDescriptor>
      implements BigtableTableService {
    private BigtableTableServiceStub(io.grpc.Channel channel,
        BigtableTableServiceServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected BigtableTableServiceStub build(io.grpc.Channel channel,
        BigtableTableServiceServiceDescriptor config) {
      return new BigtableTableServiceStub(channel, config);
    }

    @java.lang.Override
    public void createTable(com.google.bigtable.admin.table.v1.CreateTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.createTable), request, responseObserver);
    }

    @java.lang.Override
    public void listTables(com.google.bigtable.admin.table.v1.ListTablesRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ListTablesResponse> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.listTables), request, responseObserver);
    }

    @java.lang.Override
    public void getTable(com.google.bigtable.admin.table.v1.GetTableRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.getTable), request, responseObserver);
    }

    @java.lang.Override
    public void deleteTable(com.google.bigtable.admin.table.v1.DeleteTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.deleteTable), request, responseObserver);
    }

    @java.lang.Override
    public void renameTable(com.google.bigtable.admin.table.v1.RenameTableRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.renameTable), request, responseObserver);
    }

    @java.lang.Override
    public void createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.createColumnFamily), request, responseObserver);
    }

    @java.lang.Override
    public void updateColumnFamily(com.google.bigtable.admin.table.v1.ColumnFamily request,
        io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.updateColumnFamily), request, responseObserver);
    }

    @java.lang.Override
    public void deleteColumnFamily(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.deleteColumnFamily), request, responseObserver);
    }
  }

  public static class BigtableTableServiceBlockingStub extends
      io.grpc.stub.AbstractStub<BigtableTableServiceBlockingStub, BigtableTableServiceServiceDescriptor>
      implements BigtableTableServiceBlockingClient {
    private BigtableTableServiceBlockingStub(io.grpc.Channel channel,
        BigtableTableServiceServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected BigtableTableServiceBlockingStub build(io.grpc.Channel channel,
        BigtableTableServiceServiceDescriptor config) {
      return new BigtableTableServiceBlockingStub(channel, config);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.Table createTable(com.google.bigtable.admin.table.v1.CreateTableRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.createTable), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.ListTablesResponse listTables(com.google.bigtable.admin.table.v1.ListTablesRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.listTables), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.Table getTable(com.google.bigtable.admin.table.v1.GetTableRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.getTable), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteTable(com.google.bigtable.admin.table.v1.DeleteTableRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.deleteTable), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty renameTable(com.google.bigtable.admin.table.v1.RenameTableRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.renameTable), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.ColumnFamily createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.createColumnFamily), request);
    }

    @java.lang.Override
    public com.google.bigtable.admin.table.v1.ColumnFamily updateColumnFamily(com.google.bigtable.admin.table.v1.ColumnFamily request) {
      return blockingUnaryCall(
          channel.newCall(config.updateColumnFamily), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteColumnFamily(com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.deleteColumnFamily), request);
    }
  }

  public static class BigtableTableServiceFutureStub extends
      io.grpc.stub.AbstractStub<BigtableTableServiceFutureStub, BigtableTableServiceServiceDescriptor>
      implements BigtableTableServiceFutureClient {
    private BigtableTableServiceFutureStub(io.grpc.Channel channel,
        BigtableTableServiceServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected BigtableTableServiceFutureStub build(io.grpc.Channel channel,
        BigtableTableServiceServiceDescriptor config) {
      return new BigtableTableServiceFutureStub(channel, config);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.Table> createTable(
        com.google.bigtable.admin.table.v1.CreateTableRequest request) {
      return unaryFutureCall(
          channel.newCall(config.createTable), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ListTablesResponse> listTables(
        com.google.bigtable.admin.table.v1.ListTablesRequest request) {
      return unaryFutureCall(
          channel.newCall(config.listTables), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.Table> getTable(
        com.google.bigtable.admin.table.v1.GetTableRequest request) {
      return unaryFutureCall(
          channel.newCall(config.getTable), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteTable(
        com.google.bigtable.admin.table.v1.DeleteTableRequest request) {
      return unaryFutureCall(
          channel.newCall(config.deleteTable), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> renameTable(
        com.google.bigtable.admin.table.v1.RenameTableRequest request) {
      return unaryFutureCall(
          channel.newCall(config.renameTable), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ColumnFamily> createColumnFamily(
        com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request) {
      return unaryFutureCall(
          channel.newCall(config.createColumnFamily), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.admin.table.v1.ColumnFamily> updateColumnFamily(
        com.google.bigtable.admin.table.v1.ColumnFamily request) {
      return unaryFutureCall(
          channel.newCall(config.updateColumnFamily), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteColumnFamily(
        com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request) {
      return unaryFutureCall(
          channel.newCall(config.deleteColumnFamily), request);
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final BigtableTableService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder("google.bigtable.admin.table.v1.BigtableTableService")
      .addMethod(createMethodDefinition(
          METHOD_CREATE_TABLE,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.table.v1.CreateTableRequest,
                com.google.bigtable.admin.table.v1.Table>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.table.v1.CreateTableRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
                serviceImpl.createTable(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_LIST_TABLES,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.table.v1.ListTablesRequest,
                com.google.bigtable.admin.table.v1.ListTablesResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.table.v1.ListTablesRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ListTablesResponse> responseObserver) {
                serviceImpl.listTables(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_GET_TABLE,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.table.v1.GetTableRequest,
                com.google.bigtable.admin.table.v1.Table>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.table.v1.GetTableRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.Table> responseObserver) {
                serviceImpl.getTable(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_DELETE_TABLE,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.table.v1.DeleteTableRequest,
                com.google.protobuf.Empty>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.table.v1.DeleteTableRequest request,
                  io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
                serviceImpl.deleteTable(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_RENAME_TABLE,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.table.v1.RenameTableRequest,
                com.google.protobuf.Empty>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.table.v1.RenameTableRequest request,
                  io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
                serviceImpl.renameTable(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_CREATE_COLUMN_FAMILY,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest,
                com.google.bigtable.admin.table.v1.ColumnFamily>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
                serviceImpl.createColumnFamily(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_UPDATE_COLUMN_FAMILY,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.table.v1.ColumnFamily,
                com.google.bigtable.admin.table.v1.ColumnFamily>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.table.v1.ColumnFamily request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.admin.table.v1.ColumnFamily> responseObserver) {
                serviceImpl.updateColumnFamily(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_DELETE_COLUMN_FAMILY,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest,
                com.google.protobuf.Empty>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest request,
                  io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
                serviceImpl.deleteColumnFamily(request, responseObserver);
              }
            }))).build();
  }
}
