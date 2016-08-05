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
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 * <pre>
 * Service for reading from and writing to existing Bigtable tables.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.0-pre2)",
    comments = "Source: google/bigtable/v2/bigtable.proto")
public class BigtableGrpc {

  private BigtableGrpc() {}

  public static final String SERVICE_NAME = "google.bigtable.v2.Bigtable";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.ReadRowsRequest,
      com.google.bigtable.v2.ReadRowsResponse> METHOD_READ_ROWS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "ReadRows"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.ReadRowsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.ReadRowsResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.SampleRowKeysRequest,
      com.google.bigtable.v2.SampleRowKeysResponse> METHOD_SAMPLE_ROW_KEYS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "SampleRowKeys"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.SampleRowKeysRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.SampleRowKeysResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.MutateRowRequest,
      com.google.bigtable.v2.MutateRowResponse> METHOD_MUTATE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "MutateRow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.MutateRowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.MutateRowResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.MutateRowsRequest,
      com.google.bigtable.v2.MutateRowsResponse> METHOD_MUTATE_ROWS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "MutateRows"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.MutateRowsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.MutateRowsResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.CheckAndMutateRowRequest,
      com.google.bigtable.v2.CheckAndMutateRowResponse> METHOD_CHECK_AND_MUTATE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "CheckAndMutateRow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.CheckAndMutateRowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.CheckAndMutateRowResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.bigtable.v2.ReadModifyWriteRowRequest,
      com.google.bigtable.v2.ReadModifyWriteRowResponse> METHOD_READ_MODIFY_WRITE_ROW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.bigtable.v2.Bigtable", "ReadModifyWriteRow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.ReadModifyWriteRowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.bigtable.v2.ReadModifyWriteRowResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BigtableStub newStub(io.grpc.Channel channel) {
    return new BigtableStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BigtableBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BigtableBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static BigtableFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BigtableFutureStub(channel);
  }

  /**
   * <pre>
   * Service for reading from and writing to existing Bigtable tables.
   * </pre>
   */
  public static abstract class BigtableImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Streams back the contents of all requested rows, optionally
     * applying the same Reader filter to each. Depending on their size,
     * rows and cells may be broken up across multiple responses, but
     * atomicity of each row will still be preserved. See the
     * ReadRowsResponse documentation for details.
     * </pre>
     */
    public void readRows(com.google.bigtable.v2.ReadRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadRowsResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_READ_ROWS, responseObserver);
    }

    /**
     * <pre>
     * Returns a sample of row keys in the table. The returned row keys will
     * delimit contiguous sections of the table of approximately equal size,
     * which can be used to break up the data for distributed tasks like
     * mapreduces.
     * </pre>
     */
    public void sampleRowKeys(com.google.bigtable.v2.SampleRowKeysRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.SampleRowKeysResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SAMPLE_ROW_KEYS, responseObserver);
    }

    /**
     * <pre>
     * Mutates a row atomically. Cells already present in the row are left
     * unchanged unless explicitly changed by `mutation`.
     * </pre>
     */
    public void mutateRow(com.google.bigtable.v2.MutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.MutateRowResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_MUTATE_ROW, responseObserver);
    }

    /**
     * <pre>
     * Mutates multiple rows in a batch. Each individual row is mutated
     * atomically as in MutateRow, but the entire batch is not executed
     * atomically.
     * </pre>
     */
    public void mutateRows(com.google.bigtable.v2.MutateRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.MutateRowsResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_MUTATE_ROWS, responseObserver);
    }

    /**
     * <pre>
     * Mutates a row atomically based on the output of a predicate Reader filter.
     * </pre>
     */
    public void checkAndMutateRow(com.google.bigtable.v2.CheckAndMutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.CheckAndMutateRowResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CHECK_AND_MUTATE_ROW, responseObserver);
    }

    /**
     * <pre>
     * Modifies a row atomically. The method reads the latest existing timestamp
     * and value from the specified columns and writes a new entry based on
     * pre-defined read/modify/write rules. The new value for the timestamp is the
     * greater of the existing timestamp or the current server time. The method
     * returns the new contents of all modified cells.
     * </pre>
     */
    public void readModifyWriteRow(com.google.bigtable.v2.ReadModifyWriteRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadModifyWriteRowResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_READ_MODIFY_WRITE_ROW, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_READ_ROWS,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.google.bigtable.v2.ReadRowsRequest,
                com.google.bigtable.v2.ReadRowsResponse>(
                  this, METHODID_READ_ROWS)))
          .addMethod(
            METHOD_SAMPLE_ROW_KEYS,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.google.bigtable.v2.SampleRowKeysRequest,
                com.google.bigtable.v2.SampleRowKeysResponse>(
                  this, METHODID_SAMPLE_ROW_KEYS)))
          .addMethod(
            METHOD_MUTATE_ROW,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.v2.MutateRowRequest,
                com.google.bigtable.v2.MutateRowResponse>(
                  this, METHODID_MUTATE_ROW)))
          .addMethod(
            METHOD_MUTATE_ROWS,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.google.bigtable.v2.MutateRowsRequest,
                com.google.bigtable.v2.MutateRowsResponse>(
                  this, METHODID_MUTATE_ROWS)))
          .addMethod(
            METHOD_CHECK_AND_MUTATE_ROW,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.v2.CheckAndMutateRowRequest,
                com.google.bigtable.v2.CheckAndMutateRowResponse>(
                  this, METHODID_CHECK_AND_MUTATE_ROW)))
          .addMethod(
            METHOD_READ_MODIFY_WRITE_ROW,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.bigtable.v2.ReadModifyWriteRowRequest,
                com.google.bigtable.v2.ReadModifyWriteRowResponse>(
                  this, METHODID_READ_MODIFY_WRITE_ROW)))
          .build();
    }
  }

  /**
   * <pre>
   * Service for reading from and writing to existing Bigtable tables.
   * </pre>
   */
  public static final class BigtableStub extends io.grpc.stub.AbstractStub<BigtableStub> {
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

    /**
     * <pre>
     * Streams back the contents of all requested rows, optionally
     * applying the same Reader filter to each. Depending on their size,
     * rows and cells may be broken up across multiple responses, but
     * atomicity of each row will still be preserved. See the
     * ReadRowsResponse documentation for details.
     * </pre>
     */
    public void readRows(com.google.bigtable.v2.ReadRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadRowsResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_READ_ROWS, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Returns a sample of row keys in the table. The returned row keys will
     * delimit contiguous sections of the table of approximately equal size,
     * which can be used to break up the data for distributed tasks like
     * mapreduces.
     * </pre>
     */
    public void sampleRowKeys(com.google.bigtable.v2.SampleRowKeysRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.SampleRowKeysResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SAMPLE_ROW_KEYS, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Mutates a row atomically. Cells already present in the row are left
     * unchanged unless explicitly changed by `mutation`.
     * </pre>
     */
    public void mutateRow(com.google.bigtable.v2.MutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.MutateRowResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MUTATE_ROW, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Mutates multiple rows in a batch. Each individual row is mutated
     * atomically as in MutateRow, but the entire batch is not executed
     * atomically.
     * </pre>
     */
    public void mutateRows(com.google.bigtable.v2.MutateRowsRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.MutateRowsResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_MUTATE_ROWS, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Mutates a row atomically based on the output of a predicate Reader filter.
     * </pre>
     */
    public void checkAndMutateRow(com.google.bigtable.v2.CheckAndMutateRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.CheckAndMutateRowResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_MUTATE_ROW, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Modifies a row atomically. The method reads the latest existing timestamp
     * and value from the specified columns and writes a new entry based on
     * pre-defined read/modify/write rules. The new value for the timestamp is the
     * greater of the existing timestamp or the current server time. The method
     * returns the new contents of all modified cells.
     * </pre>
     */
    public void readModifyWriteRow(com.google.bigtable.v2.ReadModifyWriteRowRequest request,
        io.grpc.stub.StreamObserver<com.google.bigtable.v2.ReadModifyWriteRowResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_READ_MODIFY_WRITE_ROW, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Service for reading from and writing to existing Bigtable tables.
   * </pre>
   */
  public static final class BigtableBlockingStub extends io.grpc.stub.AbstractStub<BigtableBlockingStub> {
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

    /**
     * <pre>
     * Streams back the contents of all requested rows, optionally
     * applying the same Reader filter to each. Depending on their size,
     * rows and cells may be broken up across multiple responses, but
     * atomicity of each row will still be preserved. See the
     * ReadRowsResponse documentation for details.
     * </pre>
     */
    public java.util.Iterator<com.google.bigtable.v2.ReadRowsResponse> readRows(
        com.google.bigtable.v2.ReadRowsRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_READ_ROWS, getCallOptions(), request);
    }

    /**
     * <pre>
     * Returns a sample of row keys in the table. The returned row keys will
     * delimit contiguous sections of the table of approximately equal size,
     * which can be used to break up the data for distributed tasks like
     * mapreduces.
     * </pre>
     */
    public java.util.Iterator<com.google.bigtable.v2.SampleRowKeysResponse> sampleRowKeys(
        com.google.bigtable.v2.SampleRowKeysRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SAMPLE_ROW_KEYS, getCallOptions(), request);
    }

    /**
     * <pre>
     * Mutates a row atomically. Cells already present in the row are left
     * unchanged unless explicitly changed by `mutation`.
     * </pre>
     */
    public com.google.bigtable.v2.MutateRowResponse mutateRow(com.google.bigtable.v2.MutateRowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MUTATE_ROW, getCallOptions(), request);
    }

    /**
     * <pre>
     * Mutates multiple rows in a batch. Each individual row is mutated
     * atomically as in MutateRow, but the entire batch is not executed
     * atomically.
     * </pre>
     */
    public java.util.Iterator<com.google.bigtable.v2.MutateRowsResponse> mutateRows(
        com.google.bigtable.v2.MutateRowsRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_MUTATE_ROWS, getCallOptions(), request);
    }

    /**
     * <pre>
     * Mutates a row atomically based on the output of a predicate Reader filter.
     * </pre>
     */
    public com.google.bigtable.v2.CheckAndMutateRowResponse checkAndMutateRow(com.google.bigtable.v2.CheckAndMutateRowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CHECK_AND_MUTATE_ROW, getCallOptions(), request);
    }

    /**
     * <pre>
     * Modifies a row atomically. The method reads the latest existing timestamp
     * and value from the specified columns and writes a new entry based on
     * pre-defined read/modify/write rules. The new value for the timestamp is the
     * greater of the existing timestamp or the current server time. The method
     * returns the new contents of all modified cells.
     * </pre>
     */
    public com.google.bigtable.v2.ReadModifyWriteRowResponse readModifyWriteRow(com.google.bigtable.v2.ReadModifyWriteRowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_READ_MODIFY_WRITE_ROW, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Service for reading from and writing to existing Bigtable tables.
   * </pre>
   */
  public static final class BigtableFutureStub extends io.grpc.stub.AbstractStub<BigtableFutureStub> {
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

    /**
     * <pre>
     * Mutates a row atomically. Cells already present in the row are left
     * unchanged unless explicitly changed by `mutation`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v2.MutateRowResponse> mutateRow(
        com.google.bigtable.v2.MutateRowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_MUTATE_ROW, getCallOptions()), request);
    }

    /**
     * <pre>
     * Mutates a row atomically based on the output of a predicate Reader filter.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v2.CheckAndMutateRowResponse> checkAndMutateRow(
        com.google.bigtable.v2.CheckAndMutateRowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CHECK_AND_MUTATE_ROW, getCallOptions()), request);
    }

    /**
     * <pre>
     * Modifies a row atomically. The method reads the latest existing timestamp
     * and value from the specified columns and writes a new entry based on
     * pre-defined read/modify/write rules. The new value for the timestamp is the
     * greater of the existing timestamp or the current server time. The method
     * returns the new contents of all modified cells.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.bigtable.v2.ReadModifyWriteRowResponse> readModifyWriteRow(
        com.google.bigtable.v2.ReadModifyWriteRowRequest request) {
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
    private final BigtableImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(BigtableImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
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
        case METHODID_MUTATE_ROWS:
          serviceImpl.mutateRows((com.google.bigtable.v2.MutateRowsRequest) request,
              (io.grpc.stub.StreamObserver<com.google.bigtable.v2.MutateRowsResponse>) responseObserver);
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
        METHOD_READ_ROWS,
        METHOD_SAMPLE_ROW_KEYS,
        METHOD_MUTATE_ROW,
        METHOD_MUTATE_ROWS,
        METHOD_CHECK_AND_MUTATE_ROW,
        METHOD_READ_MODIFY_WRITE_ROW);
  }

}
