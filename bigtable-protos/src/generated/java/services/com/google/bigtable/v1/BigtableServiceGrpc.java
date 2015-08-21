package com.google.bigtable.v1;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;

@javax.annotation.Generated("by gRPC proto compiler")
public class BigtableServiceGrpc {

  // Static method descriptors that strictly reflect the proto.
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.ReadRowsRequest,
      com.google.bigtable.v1.ReadRowsResponse> METHOD_READ_ROWS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          "google.bigtable.v1.BigtableService", "ReadRows",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.ReadRowsRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.ReadRowsResponse.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.SampleRowKeysRequest,
      com.google.bigtable.v1.SampleRowKeysResponse> METHOD_SAMPLE_ROW_KEYS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          "google.bigtable.v1.BigtableService", "SampleRowKeys",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.SampleRowKeysRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.SampleRowKeysResponse.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.MutateRowRequest,
      com.google.protobuf.Empty> METHOD_MUTATE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.v1.BigtableService", "MutateRow",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.MutateRowRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.CheckAndMutateRowRequest,
      com.google.bigtable.v1.CheckAndMutateRowResponse> METHOD_CHECK_AND_MUTATE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.v1.BigtableService", "CheckAndMutateRow",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.CheckAndMutateRowRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.CheckAndMutateRowResponse.parser()));
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.ReadModifyWriteRowRequest,
      com.google.bigtable.v1.Row> METHOD_READ_MODIFY_WRITE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          "google.bigtable.v1.BigtableService", "ReadModifyWriteRow",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.ReadModifyWriteRowRequest.parser()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.Row.parser()));

  public static BigtableServiceStub newStub(io.grpc.Channel channel) {
    return new BigtableServiceStub(channel);
  }

  public static BigtableServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableServiceBlockingStub(channel);
  }

  public static BigtableServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableServiceFutureStub(channel);
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

  public static class BigtableServiceStub extends io.grpc.stub.AbstractStub<BigtableServiceStub>
      implements BigtableService {
    private BigtableServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableServiceStub(channel, callOptions);
    }

    @java.lang.Override
    public void readRows(com.google.bigtable.v1.ReadRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.ReadRowsResponse> responseObserver) {
      asyncServerStreamingCall(
          channel.newCall(METHOD_READ_ROWS, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void sampleRowKeys(com.google.bigtable.v1.SampleRowKeysRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.SampleRowKeysResponse> responseObserver) {
      asyncServerStreamingCall(
          channel.newCall(METHOD_SAMPLE_ROW_KEYS, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void mutateRow(com.google.bigtable.v1.MutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_MUTATE_ROW, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.CheckAndMutateRowResponse> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_CHECK_AND_MUTATE_ROW, callOptions), request, responseObserver);
    }

    @java.lang.Override
    public void readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.Row> responseObserver) {
      asyncUnaryCall(
          channel.newCall(METHOD_READ_MODIFY_WRITE_ROW, callOptions), request, responseObserver);
    }
  }

  public static class BigtableServiceBlockingStub extends io.grpc.stub.AbstractStub<BigtableServiceBlockingStub>
      implements BigtableServiceBlockingClient {
    private BigtableServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableServiceBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public java.util.Iterator<com.google.bigtable.v1.ReadRowsResponse> readRows(
        com.google.bigtable.v1.ReadRowsRequest request) {
      return blockingServerStreamingCall(
          channel.newCall(METHOD_READ_ROWS, callOptions), request);
    }

    @java.lang.Override
    public java.util.Iterator<com.google.bigtable.v1.SampleRowKeysResponse> sampleRowKeys(
        com.google.bigtable.v1.SampleRowKeysRequest request) {
      return blockingServerStreamingCall(
          channel.newCall(METHOD_SAMPLE_ROW_KEYS, callOptions), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty mutateRow(com.google.bigtable.v1.MutateRowRequest request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_MUTATE_ROW, callOptions), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.CheckAndMutateRowResponse checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_CHECK_AND_MUTATE_ROW, callOptions), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.Row readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request) {
      return blockingUnaryCall(
          channel.newCall(METHOD_READ_MODIFY_WRITE_ROW, callOptions), request);
    }
  }

  public static class BigtableServiceFutureStub extends io.grpc.stub.AbstractStub<BigtableServiceFutureStub>
      implements BigtableServiceFutureClient {
    private BigtableServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableServiceFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> mutateRow(
        com.google.bigtable.v1.MutateRowRequest request) {
      return futureUnaryCall(
          channel.newCall(METHOD_MUTATE_ROW, callOptions), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.CheckAndMutateRowResponse> checkAndMutateRow(
        com.google.bigtable.v1.CheckAndMutateRowRequest request) {
      return futureUnaryCall(
          channel.newCall(METHOD_CHECK_AND_MUTATE_ROW, callOptions), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.Row> readModifyWriteRow(
        com.google.bigtable.v1.ReadModifyWriteRowRequest request) {
      return futureUnaryCall(
          channel.newCall(METHOD_READ_MODIFY_WRITE_ROW, callOptions), request);
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final BigtableService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder("google.bigtable.v1.BigtableService")
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_READ_ROWS,
          asyncServerStreamingCall(
            new io.grpc.stub.ServerCalls.ServerStreamingMethod<
                com.google.bigtable.v1.ReadRowsRequest,
                com.google.bigtable.v1.ReadRowsResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.v1.ReadRowsRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.v1.ReadRowsResponse> responseObserver) {
                serviceImpl.readRows(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_SAMPLE_ROW_KEYS,
          asyncServerStreamingCall(
            new io.grpc.stub.ServerCalls.ServerStreamingMethod<
                com.google.bigtable.v1.SampleRowKeysRequest,
                com.google.bigtable.v1.SampleRowKeysResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.v1.SampleRowKeysRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.v1.SampleRowKeysResponse> responseObserver) {
                serviceImpl.sampleRowKeys(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_MUTATE_ROW,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
                com.google.bigtable.v1.MutateRowRequest,
                com.google.protobuf.Empty>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.v1.MutateRowRequest request,
                  io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
                serviceImpl.mutateRow(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_CHECK_AND_MUTATE_ROW,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
                com.google.bigtable.v1.CheckAndMutateRowRequest,
                com.google.bigtable.v1.CheckAndMutateRowResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.bigtable.v1.CheckAndMutateRowRequest request,
                  io.grpc.stub.StreamObserver<com.google.bigtable.v1.CheckAndMutateRowResponse> responseObserver) {
                serviceImpl.checkAndMutateRow(request, responseObserver);
              }
            })))
      .addMethod(io.grpc.ServerMethodDefinition.create(
          METHOD_READ_MODIFY_WRITE_ROW,
          asyncUnaryCall(
            new io.grpc.stub.ServerCalls.UnaryMethod<
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
