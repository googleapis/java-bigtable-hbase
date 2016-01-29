package com.google.bigtable.v1;

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
public class BigtableServiceGrpc {

  private BigtableServiceGrpc() {}

  public static final String SERVICE_NAME = "google.bigtable.v1.BigtableService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.ReadRowsRequest,
      com.google.bigtable.v1.ReadRowsResponse> METHOD_READ_ROWS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "google.bigtable.v1.BigtableService", "ReadRows"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.ReadRowsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.ReadRowsResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.SampleRowKeysRequest,
      com.google.bigtable.v1.SampleRowKeysResponse> METHOD_SAMPLE_ROW_KEYS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "google.bigtable.v1.BigtableService", "SampleRowKeys"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.SampleRowKeysRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.SampleRowKeysResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.MutateRowRequest,
      com.google.protobuf.Empty> METHOD_MUTATE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v1.BigtableService", "MutateRow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.MutateRowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.MutateRowsRequest,
      com.google.bigtable.v1.MutateRowsResponse> METHOD_MUTATE_ROWS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v1.BigtableService", "MutateRows"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.MutateRowsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.MutateRowsResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.CheckAndMutateRowRequest,
      com.google.bigtable.v1.CheckAndMutateRowResponse> METHOD_CHECK_AND_MUTATE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v1.BigtableService", "CheckAndMutateRow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.CheckAndMutateRowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.CheckAndMutateRowResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v1.ReadModifyWriteRowRequest,
      com.google.bigtable.v1.Row> METHOD_READ_MODIFY_WRITE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v1.BigtableService", "ReadModifyWriteRow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.ReadModifyWriteRowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v1.Row.getDefaultInstance()));

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

    public void mutateRows(com.google.bigtable.v1.MutateRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.MutateRowsResponse> responseObserver);

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

    public com.google.bigtable.v1.MutateRowsResponse mutateRows(com.google.bigtable.v1.MutateRowsRequest request);

    public com.google.bigtable.v1.CheckAndMutateRowResponse checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request);

    public com.google.bigtable.v1.Row readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request);
  }

  public static interface BigtableServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> mutateRow(
        com.google.bigtable.v1.MutateRowRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.MutateRowsResponse> mutateRows(
        com.google.bigtable.v1.MutateRowsRequest request);

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
          getChannel().newCall(METHOD_READ_ROWS, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void sampleRowKeys(com.google.bigtable.v1.SampleRowKeysRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.SampleRowKeysResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SAMPLE_ROW_KEYS, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void mutateRow(com.google.bigtable.v1.MutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MUTATE_ROW, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void mutateRows(com.google.bigtable.v1.MutateRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.MutateRowsResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MUTATE_ROWS, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.CheckAndMutateRowResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_MUTATE_ROW, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v1.Row> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_READ_MODIFY_WRITE_ROW, getCallOptions()), request, responseObserver);
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
          getChannel().newCall(METHOD_READ_ROWS, getCallOptions()), request);
    }

    @java.lang.Override
    public java.util.Iterator<com.google.bigtable.v1.SampleRowKeysResponse> sampleRowKeys(
        com.google.bigtable.v1.SampleRowKeysRequest request) {
      return blockingServerStreamingCall(
          getChannel().newCall(METHOD_SAMPLE_ROW_KEYS, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty mutateRow(com.google.bigtable.v1.MutateRowRequest request) {
      return blockingUnaryCall(
          getChannel().newCall(METHOD_MUTATE_ROW, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.MutateRowsResponse mutateRows(com.google.bigtable.v1.MutateRowsRequest request) {
      return blockingUnaryCall(
          getChannel().newCall(METHOD_MUTATE_ROWS, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.CheckAndMutateRowResponse checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request) {
      return blockingUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_MUTATE_ROW, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.Row readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request) {
      return blockingUnaryCall(
          getChannel().newCall(METHOD_READ_MODIFY_WRITE_ROW, getCallOptions()), request);
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
          getChannel().newCall(METHOD_MUTATE_ROW, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.MutateRowsResponse> mutateRows(
        com.google.bigtable.v1.MutateRowsRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_MUTATE_ROWS, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.CheckAndMutateRowResponse> checkAndMutateRow(
        com.google.bigtable.v1.CheckAndMutateRowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_MUTATE_ROW, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v1.Row> readModifyWriteRow(
        com.google.bigtable.v1.ReadModifyWriteRowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_READ_MODIFY_WRITE_ROW, getCallOptions()), request);
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final BigtableService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
      .addMethod(
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
          }))
      .addMethod(
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
          }))
      .addMethod(
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
          }))
      .addMethod(
        METHOD_MUTATE_ROWS,
        asyncUnaryCall(
          new io.grpc.stub.ServerCalls.UnaryMethod<
              com.google.bigtable.v1.MutateRowsRequest,
              com.google.bigtable.v1.MutateRowsResponse>() {
            @java.lang.Override
            public void invoke(
                com.google.bigtable.v1.MutateRowsRequest request,
                io.grpc.stub.StreamObserver<com.google.bigtable.v1.MutateRowsResponse> responseObserver) {
              serviceImpl.mutateRows(request, responseObserver);
            }
          }))
      .addMethod(
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
          }))
      .addMethod(
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
          })).build();
  }
}
