package com.google.bigtable.v1;

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
public class BigtableServiceGrpc {

  private static final io.grpc.stub.Method<com.google.bigtable.v1.ReadRowsRequest,
      com.google.bigtable.v1.ReadRowsResponse> METHOD_READ_ROWS =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.SERVER_STREAMING, "ReadRows",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.ReadRowsRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.ReadRowsResponse.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.v1.SampleRowKeysRequest,
      com.google.bigtable.v1.SampleRowKeysResponse> METHOD_SAMPLE_ROW_KEYS =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.SERVER_STREAMING, "SampleRowKeys",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.SampleRowKeysRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.SampleRowKeysResponse.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.v1.MutateRowRequest,
      com.google.protobuf.Empty> METHOD_MUTATE_ROW =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "MutateRow",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.MutateRowRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.v1.CheckAndMutateRowRequest,
      com.google.bigtable.v1.CheckAndMutateRowResponse> METHOD_CHECK_AND_MUTATE_ROW =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "CheckAndMutateRow",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.CheckAndMutateRowRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.CheckAndMutateRowResponse.PARSER));
  private static final io.grpc.stub.Method<com.google.bigtable.v1.ReadModifyWriteRowRequest,
      com.google.bigtable.v1.Row> METHOD_READ_MODIFY_WRITE_ROW =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "ReadModifyWriteRow",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.ReadModifyWriteRowRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.Row.PARSER));

  public static BigtableServiceStub newStub(io.grpc.Channel channel) {
    return new BigtableServiceStub(channel, CONFIG);
  }

  public static BigtableServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableServiceBlockingStub(channel, CONFIG);
  }

  public static BigtableServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableServiceFutureStub(channel, CONFIG);
  }

  public static final BigtableServiceServiceDescriptor CONFIG =
      new BigtableServiceServiceDescriptor();

  @javax.annotation.concurrent.Immutable
  public static class BigtableServiceServiceDescriptor extends
      io.grpc.stub.AbstractServiceDescriptor<BigtableServiceServiceDescriptor> {
    public final io.grpc.MethodDescriptor<com.google.bigtable.v1.ReadRowsRequest,
        com.google.bigtable.v1.ReadRowsResponse> readRows;
    public final io.grpc.MethodDescriptor<com.google.bigtable.v1.SampleRowKeysRequest,
        com.google.bigtable.v1.SampleRowKeysResponse> sampleRowKeys;
    public final io.grpc.MethodDescriptor<com.google.bigtable.v1.MutateRowRequest,
        com.google.protobuf.Empty> mutateRow;
    public final io.grpc.MethodDescriptor<com.google.bigtable.v1.CheckAndMutateRowRequest,
        com.google.bigtable.v1.CheckAndMutateRowResponse> checkAndMutateRow;
    public final io.grpc.MethodDescriptor<com.google.bigtable.v1.ReadModifyWriteRowRequest,
        com.google.bigtable.v1.Row> readModifyWriteRow;

    private BigtableServiceServiceDescriptor() {
      readRows = createMethodDescriptor(
          "google.bigtable.v1.BigtableService", METHOD_READ_ROWS);
      sampleRowKeys = createMethodDescriptor(
          "google.bigtable.v1.BigtableService", METHOD_SAMPLE_ROW_KEYS);
      mutateRow = createMethodDescriptor(
          "google.bigtable.v1.BigtableService", METHOD_MUTATE_ROW);
      checkAndMutateRow = createMethodDescriptor(
          "google.bigtable.v1.BigtableService", METHOD_CHECK_AND_MUTATE_ROW);
      readModifyWriteRow = createMethodDescriptor(
          "google.bigtable.v1.BigtableService", METHOD_READ_MODIFY_WRITE_ROW);
    }

    @SuppressWarnings("unchecked")
    private BigtableServiceServiceDescriptor(
        java.util.Map<java.lang.String, io.grpc.MethodDescriptor<?, ?>> methodMap) {
      readRows = (io.grpc.MethodDescriptor<com.google.bigtable.v1.ReadRowsRequest,
          com.google.bigtable.v1.ReadRowsResponse>) methodMap.get(
          CONFIG.readRows.getName());
      sampleRowKeys = (io.grpc.MethodDescriptor<com.google.bigtable.v1.SampleRowKeysRequest,
          com.google.bigtable.v1.SampleRowKeysResponse>) methodMap.get(
          CONFIG.sampleRowKeys.getName());
      mutateRow = (io.grpc.MethodDescriptor<com.google.bigtable.v1.MutateRowRequest,
          com.google.protobuf.Empty>) methodMap.get(
          CONFIG.mutateRow.getName());
      checkAndMutateRow = (io.grpc.MethodDescriptor<com.google.bigtable.v1.CheckAndMutateRowRequest,
          com.google.bigtable.v1.CheckAndMutateRowResponse>) methodMap.get(
          CONFIG.checkAndMutateRow.getName());
      readModifyWriteRow = (io.grpc.MethodDescriptor<com.google.bigtable.v1.ReadModifyWriteRowRequest,
          com.google.bigtable.v1.Row>) methodMap.get(
          CONFIG.readModifyWriteRow.getName());
    }

    @java.lang.Override
    protected BigtableServiceServiceDescriptor build(
        java.util.Map<java.lang.String, io.grpc.MethodDescriptor<?, ?>> methodMap) {
      return new BigtableServiceServiceDescriptor(methodMap);
    }

    @java.lang.Override
    public com.google.common.collect.ImmutableList<io.grpc.MethodDescriptor<?, ?>> methods() {
      return com.google.common.collect.ImmutableList.<io.grpc.MethodDescriptor<?, ?>>of(
          readRows,
          sampleRowKeys,
          mutateRow,
          checkAndMutateRow,
          readModifyWriteRow);
    }
  }

  public static interface BigtableService {

    public void readRows(com.google.bigtable.v1.ReadRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.ReadRowsResponse> responseObserver);

    public void sampleRowKeys(com.google.bigtable.v1.SampleRowKeysRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.SampleRowKeysResponse> responseObserver);

    public void mutateRow(com.google.bigtable.v1.MutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    public void checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.CheckAndMutateRowResponse> responseObserver);

    public void readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.Row> responseObserver);
  }

  public static interface BigtableServiceBlockingClient {

    public java.util.Iterator<com.google.bigtable.v1.ReadRowsResponse> readRows(
        com.google.bigtable.v1.ReadRowsRequest request);

    public java.util.Iterator<com.google.bigtable.v1.SampleRowKeysResponse> sampleRowKeys(
        com.google.bigtable.v1.SampleRowKeysRequest request);

    public com.google.protobuf.Empty mutateRow(com.google.bigtable.v1.MutateRowRequest request);

    public com.google.bigtable.v1.CheckAndMutateRowResponse checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request);

    public com.google.bigtable.v1.Row readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request);
  }

  public static interface BigtableServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> mutateRow(
        com.google.bigtable.v1.MutateRowRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.CheckAndMutateRowResponse> checkAndMutateRow(
        com.google.bigtable.v1.CheckAndMutateRowRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.Row> readModifyWriteRow(
        com.google.bigtable.v1.ReadModifyWriteRowRequest request);
  }

  public static class BigtableServiceStub extends
      io.grpc.stub.AbstractStub<BigtableServiceStub, BigtableServiceServiceDescriptor>
      implements BigtableService {
    private BigtableServiceStub(io.grpc.Channel channel,
        BigtableServiceServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected BigtableServiceStub build(io.grpc.Channel channel,
        BigtableServiceServiceDescriptor config) {
      return new BigtableServiceStub(channel, config);
    }

    @java.lang.Override
    public void readRows(com.google.bigtable.v1.ReadRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.ReadRowsResponse> responseObserver) {
      asyncServerStreamingCall(
          channel.newCall(config.readRows), request, responseObserver);
    }

    @java.lang.Override
    public void sampleRowKeys(com.google.bigtable.v1.SampleRowKeysRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.SampleRowKeysResponse> responseObserver) {
      asyncServerStreamingCall(
          channel.newCall(config.sampleRowKeys), request, responseObserver);
    }

    @java.lang.Override
    public void mutateRow(com.google.bigtable.v1.MutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.mutateRow), request, responseObserver);
    }

    @java.lang.Override
    public void checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.CheckAndMutateRowResponse> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.checkAndMutateRow), request, responseObserver);
    }

    @java.lang.Override
    public void readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.Row> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.readModifyWriteRow), request, responseObserver);
    }
  }

  public static class BigtableServiceBlockingStub extends
      io.grpc.stub.AbstractStub<BigtableServiceBlockingStub, BigtableServiceServiceDescriptor>
      implements BigtableServiceBlockingClient {
    private BigtableServiceBlockingStub(io.grpc.Channel channel,
        BigtableServiceServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected BigtableServiceBlockingStub build(io.grpc.Channel channel,
        BigtableServiceServiceDescriptor config) {
      return new BigtableServiceBlockingStub(channel, config);
    }

    @java.lang.Override
    public java.util.Iterator<com.google.bigtable.v1.ReadRowsResponse> readRows(
        com.google.bigtable.v1.ReadRowsRequest request) {
      return blockingServerStreamingCall(
          channel.newCall(config.readRows), request);
    }

    @java.lang.Override
    public java.util.Iterator<com.google.bigtable.v1.SampleRowKeysResponse> sampleRowKeys(
        com.google.bigtable.v1.SampleRowKeysRequest request) {
      return blockingServerStreamingCall(
          channel.newCall(config.sampleRowKeys), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty mutateRow(com.google.bigtable.v1.MutateRowRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.mutateRow), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.CheckAndMutateRowResponse checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.checkAndMutateRow), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.Row readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.readModifyWriteRow), request);
    }
  }

  public static class BigtableServiceFutureStub extends
      io.grpc.stub.AbstractStub<BigtableServiceFutureStub, BigtableServiceServiceDescriptor>
      implements BigtableServiceFutureClient {
    private BigtableServiceFutureStub(io.grpc.Channel channel,
        BigtableServiceServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected BigtableServiceFutureStub build(io.grpc.Channel channel,
        BigtableServiceServiceDescriptor config) {
      return new BigtableServiceFutureStub(channel, config);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> mutateRow(
        com.google.bigtable.v1.MutateRowRequest request) {
      return unaryFutureCall(
          channel.newCall(config.mutateRow), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.CheckAndMutateRowResponse> checkAndMutateRow(
        com.google.bigtable.v1.CheckAndMutateRowRequest request) {
      return unaryFutureCall(
          channel.newCall(config.checkAndMutateRow), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.Row> readModifyWriteRow(
        com.google.bigtable.v1.ReadModifyWriteRowRequest request) {
      return unaryFutureCall(
          channel.newCall(config.readModifyWriteRow), request);
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final BigtableService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder("google.bigtable.v1.BigtableService")
      .addMethod(createMethodDefinition(
          METHOD_READ_ROWS,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.v1.ReadRowsRequest,
                com.google.bigtable.v1.ReadRowsResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.v1.ReadRowsRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.v1.ReadRowsResponse> responseObserver) {
                serviceImpl.readRows(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_SAMPLE_ROW_KEYS,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.v1.SampleRowKeysRequest,
                com.google.bigtable.v1.SampleRowKeysResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.v1.SampleRowKeysRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.v1.SampleRowKeysResponse> responseObserver) {
                serviceImpl.sampleRowKeys(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_MUTATE_ROW,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.v1.MutateRowRequest,
                com.google.protobuf.Empty>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.v1.MutateRowRequest request,
                  io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
                serviceImpl.mutateRow(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_CHECK_AND_MUTATE_ROW,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.v1.CheckAndMutateRowRequest,
                com.google.bigtable.v1.CheckAndMutateRowResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.v1.CheckAndMutateRowRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.v1.CheckAndMutateRowResponse> responseObserver) {
                serviceImpl.checkAndMutateRow(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_READ_MODIFY_WRITE_ROW,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.bigtable.v1.ReadModifyWriteRowRequest,
                com.google.bigtable.v1.Row>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.v1.ReadModifyWriteRowRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.v1.Row> responseObserver) {
                serviceImpl.readModifyWriteRow(request, responseObserver);
              }
            }))).build();
  }
}
