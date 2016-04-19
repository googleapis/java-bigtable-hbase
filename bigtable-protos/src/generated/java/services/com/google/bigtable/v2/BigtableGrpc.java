package com.google.bigtable.v2;

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
public class BigtableGrpc {

  private BigtableGrpc() {}

  public static final String SERVICE_NAME = "google.bigtable.v2.Bigtable";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.ReadRowsRequest,
      com.google.bigtable.v2.ReadRowsResponse> METHOD_READ_ROWS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "ReadRows"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.ReadRowsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.ReadRowsResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.SampleRowKeysRequest,
      com.google.bigtable.v2.SampleRowKeysResponse> METHOD_SAMPLE_ROW_KEYS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "SampleRowKeys"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.SampleRowKeysRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.SampleRowKeysResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.MutateRowRequest,
      com.google.bigtable.v2.MutateRowResponse> METHOD_MUTATE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "MutateRow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.MutateRowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.MutateRowResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.CheckAndMutateRowRequest,
      com.google.bigtable.v2.CheckAndMutateRowResponse> METHOD_CHECK_AND_MUTATE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "CheckAndMutateRow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.CheckAndMutateRowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.CheckAndMutateRowResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.ReadModifyWriteRowRequest,
      com.google.bigtable.v2.ReadModifyWriteRowResponse> METHOD_READ_MODIFY_WRITE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "ReadModifyWriteRow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.ReadModifyWriteRowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.ReadModifyWriteRowResponse.getDefaultInstance()));

  public static BigtableStub newStub(io.grpc.Channel channel) {
    return new BigtableStub(channel);
  }

  public static BigtableBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableBlockingStub(channel);
  }

  public static BigtableFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableFutureStub(channel);
  }

  public static interface Bigtable {

    public void readRows(com.google.bigtable.v2.ReadRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadRowsResponse> responseObserver);

    public void sampleRowKeys(com.google.bigtable.v2.SampleRowKeysRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.SampleRowKeysResponse> responseObserver);

    public void mutateRow(com.google.bigtable.v2.MutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.MutateRowResponse> responseObserver);

    public void checkAndMutateRow(com.google.bigtable.v2.CheckAndMutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.CheckAndMutateRowResponse> responseObserver);

    public void readModifyWriteRow(com.google.bigtable.v2.ReadModifyWriteRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadModifyWriteRowResponse> responseObserver);
  }

  public static interface BigtableBlockingClient {

    public java.util.Iterator<com.google.bigtable.v2.ReadRowsResponse> readRows(
        com.google.bigtable.v2.ReadRowsRequest request);

    public java.util.Iterator<com.google.bigtable.v2.SampleRowKeysResponse> sampleRowKeys(
        com.google.bigtable.v2.SampleRowKeysRequest request);

    public com.google.bigtable.v2.MutateRowResponse mutateRow(com.google.bigtable.v2.MutateRowRequest request);

    public com.google.bigtable.v2.CheckAndMutateRowResponse checkAndMutateRow(com.google.bigtable.v2.CheckAndMutateRowRequest request);

    public com.google.bigtable.v2.ReadModifyWriteRowResponse readModifyWriteRow(com.google.bigtable.v2.ReadModifyWriteRowRequest request);
  }

  public static interface BigtableFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v2.MutateRowResponse> mutateRow(
        com.google.bigtable.v2.MutateRowRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v2.CheckAndMutateRowResponse> checkAndMutateRow(
        com.google.bigtable.v2.CheckAndMutateRowRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v2.ReadModifyWriteRowResponse> readModifyWriteRow(
        com.google.bigtable.v2.ReadModifyWriteRowRequest request);
  }

  public static class BigtableStub extends io.grpc.stub.AbstractStub<BigtableStub>
      implements Bigtable {
    private BigtableStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableStub(channel, callOptions);
    }

    @java.lang.Override
    public void readRows(com.google.bigtable.v2.ReadRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadRowsResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_READ_ROWS, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void sampleRowKeys(com.google.bigtable.v2.SampleRowKeysRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.SampleRowKeysResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SAMPLE_ROW_KEYS, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void mutateRow(com.google.bigtable.v2.MutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.MutateRowResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MUTATE_ROW, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void checkAndMutateRow(com.google.bigtable.v2.CheckAndMutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.CheckAndMutateRowResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_MUTATE_ROW, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void readModifyWriteRow(com.google.bigtable.v2.ReadModifyWriteRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadModifyWriteRowResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_READ_MODIFY_WRITE_ROW, getCallOptions()), request, responseObserver);
    }
  }

  public static class BigtableBlockingStub extends io.grpc.stub.AbstractStub<BigtableBlockingStub>
      implements BigtableBlockingClient {
    private BigtableBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public java.util.Iterator<com.google.bigtable.v2.ReadRowsResponse> readRows(
        com.google.bigtable.v2.ReadRowsRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_READ_ROWS, getCallOptions(), request);
    }

    @java.lang.Override
    public java.util.Iterator<com.google.bigtable.v2.SampleRowKeysResponse> sampleRowKeys(
        com.google.bigtable.v2.SampleRowKeysRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SAMPLE_ROW_KEYS, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.v2.MutateRowResponse mutateRow(com.google.bigtable.v2.MutateRowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MUTATE_ROW, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.v2.CheckAndMutateRowResponse checkAndMutateRow(com.google.bigtable.v2.CheckAndMutateRowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CHECK_AND_MUTATE_ROW, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.v2.ReadModifyWriteRowResponse readModifyWriteRow(com.google.bigtable.v2.ReadModifyWriteRowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_READ_MODIFY_WRITE_ROW, getCallOptions(), request);
    }
  }

  public static class BigtableFutureStub extends io.grpc.stub.AbstractStub<BigtableFutureStub>
      implements BigtableFutureClient {
    private BigtableFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BigtableFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BigtableFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BigtableFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v2.MutateRowResponse> mutateRow(
        com.google.bigtable.v2.MutateRowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_MUTATE_ROW, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v2.CheckAndMutateRowResponse> checkAndMutateRow(
        com.google.bigtable.v2.CheckAndMutateRowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_MUTATE_ROW, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v2.ReadModifyWriteRowResponse> readModifyWriteRow(
        com.google.bigtable.v2.ReadModifyWriteRowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_READ_MODIFY_WRITE_ROW, getCallOptions()), request);
    }
  }

  private static final int METHODID_READ_ROWS = 0;
  private static final int METHODID_SAMPLE_ROW_KEYS = 1;
  private static final int METHODID_MUTATE_ROW = 2;
  private static final int METHODID_CHECK_AND_MUTATE_ROW = 3;
  private static final int METHODID_READ_MODIFY_WRITE_ROW = 4;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final Bigtable serviceImpl;
    private final int methodId;

    public MethodHandlers(Bigtable serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_READ_ROWS:
          serviceImpl.readRows((com.google.bigtable.v2.ReadRowsRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadRowsResponse>) responseObserver);
          break;
        case METHODID_SAMPLE_ROW_KEYS:
          serviceImpl.sampleRowKeys((com.google.bigtable.v2.SampleRowKeysRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v2.SampleRowKeysResponse>) responseObserver);
          break;
        case METHODID_MUTATE_ROW:
          serviceImpl.mutateRow((com.google.bigtable.v2.MutateRowRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v2.MutateRowResponse>) responseObserver);
          break;
        case METHODID_CHECK_AND_MUTATE_ROW:
          serviceImpl.checkAndMutateRow((com.google.bigtable.v2.CheckAndMutateRowRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v2.CheckAndMutateRowResponse>) responseObserver);
          break;
        case METHODID_READ_MODIFY_WRITE_ROW:
          serviceImpl.readModifyWriteRow((com.google.bigtable.v2.ReadModifyWriteRowRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadModifyWriteRowResponse>) responseObserver);
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
      final Bigtable serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_READ_ROWS,
          asyncServerStreamingCall(
            new MethodHandlers<
              com.google.bigtable.v2.ReadRowsRequest,
              com.google.bigtable.v2.ReadRowsResponse>(
                serviceImpl, METHODID_READ_ROWS)))
        .addMethod(
          METHOD_SAMPLE_ROW_KEYS,
          asyncServerStreamingCall(
            new MethodHandlers<
              com.google.bigtable.v2.SampleRowKeysRequest,
              com.google.bigtable.v2.SampleRowKeysResponse>(
                serviceImpl, METHODID_SAMPLE_ROW_KEYS)))
        .addMethod(
          METHOD_MUTATE_ROW,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.v2.MutateRowRequest,
              com.google.bigtable.v2.MutateRowResponse>(
                serviceImpl, METHODID_MUTATE_ROW)))
        .addMethod(
          METHOD_CHECK_AND_MUTATE_ROW,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.v2.CheckAndMutateRowRequest,
              com.google.bigtable.v2.CheckAndMutateRowResponse>(
                serviceImpl, METHODID_CHECK_AND_MUTATE_ROW)))
        .addMethod(
          METHOD_READ_MODIFY_WRITE_ROW,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.v2.ReadModifyWriteRowRequest,
              com.google.bigtable.v2.ReadModifyWriteRowResponse>(
                serviceImpl, METHODID_READ_MODIFY_WRITE_ROW)))
        .build();
  }
}
