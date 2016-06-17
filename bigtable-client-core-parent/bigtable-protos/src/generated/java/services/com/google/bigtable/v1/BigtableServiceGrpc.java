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
          getChannel(), METHOD_READ_ROWS, getCallOptions(), request);
    }

    @java.lang.Override
    public java.util.Iterator<com.google.bigtable.v1.SampleRowKeysResponse> sampleRowKeys(
        com.google.bigtable.v1.SampleRowKeysRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SAMPLE_ROW_KEYS, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty mutateRow(com.google.bigtable.v1.MutateRowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MUTATE_ROW, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.MutateRowsResponse mutateRows(com.google.bigtable.v1.MutateRowsRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MUTATE_ROWS, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.CheckAndMutateRowResponse checkAndMutateRow(com.google.bigtable.v1.CheckAndMutateRowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CHECK_AND_MUTATE_ROW, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.bigtable.v1.Row readModifyWriteRow(com.google.bigtable.v1.ReadModifyWriteRowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_READ_MODIFY_WRITE_ROW, getCallOptions(), request);
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

  private static final int METHODID_READ_ROWS = 0;
  private static final int METHODID_SAMPLE_ROW_KEYS = 1;
  private static final int METHODID_MUTATE_ROW = 2;
  private static final int METHODID_MUTATE_ROWS = 3;
  private static final int METHODID_CHECK_AND_MUTATE_ROW = 4;
  private static final int METHODID_READ_MODIFY_WRITE_ROW = 5;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BigtableService serviceImpl;
    private final int methodId;

    public MethodHandlers(BigtableService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_READ_ROWS:
          serviceImpl.readRows((com.google.bigtable.v1.ReadRowsRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v1.ReadRowsResponse>) responseObserver);
          break;
        case METHODID_SAMPLE_ROW_KEYS:
          serviceImpl.sampleRowKeys((com.google.bigtable.v1.SampleRowKeysRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v1.SampleRowKeysResponse>) responseObserver);
          break;
        case METHODID_MUTATE_ROW:
          serviceImpl.mutateRow((com.google.bigtable.v1.MutateRowRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_MUTATE_ROWS:
          serviceImpl.mutateRows((com.google.bigtable.v1.MutateRowsRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v1.MutateRowsResponse>) responseObserver);
          break;
        case METHODID_CHECK_AND_MUTATE_ROW:
          serviceImpl.checkAndMutateRow((com.google.bigtable.v1.CheckAndMutateRowRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v1.CheckAndMutateRowResponse>) responseObserver);
          break;
        case METHODID_READ_MODIFY_WRITE_ROW:
          serviceImpl.readModifyWriteRow((com.google.bigtable.v1.ReadModifyWriteRowRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v1.Row>) responseObserver);
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
      final BigtableService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_READ_ROWS,
          asyncServerStreamingCall(
            new MethodHandlers<
              com.google.bigtable.v1.ReadRowsRequest,
              com.google.bigtable.v1.ReadRowsResponse>(
                serviceImpl, METHODID_READ_ROWS)))
        .addMethod(
          METHOD_SAMPLE_ROW_KEYS,
          asyncServerStreamingCall(
            new MethodHandlers<
              com.google.bigtable.v1.SampleRowKeysRequest,
              com.google.bigtable.v1.SampleRowKeysResponse>(
                serviceImpl, METHODID_SAMPLE_ROW_KEYS)))
        .addMethod(
          METHOD_MUTATE_ROW,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.v1.MutateRowRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_MUTATE_ROW)))
        .addMethod(
          METHOD_MUTATE_ROWS,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.v1.MutateRowsRequest,
              com.google.bigtable.v1.MutateRowsResponse>(
                serviceImpl, METHODID_MUTATE_ROWS)))
        .addMethod(
          METHOD_CHECK_AND_MUTATE_ROW,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.v1.CheckAndMutateRowRequest,
              com.google.bigtable.v1.CheckAndMutateRowResponse>(
                serviceImpl, METHODID_CHECK_AND_MUTATE_ROW)))
        .addMethod(
          METHOD_READ_MODIFY_WRITE_ROW,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.bigtable.v1.ReadModifyWriteRowRequest,
              com.google.bigtable.v1.Row>(
                serviceImpl, METHODID_READ_MODIFY_WRITE_ROW)))
        .build();
  }
}
