package com.google.longrunning;

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
 * Manages long-running operations with an API service.
 * When an API method normally takes long time to complete, it can be designed
 * to return [Operation][google.longrunning.Operation] to the client, and the client can use this
 * interface to receive the real response asynchronously by polling the
 * operation resource, or using `google.watcher.v1.Watcher` interface to watch
 * the response, or pass the operation resource to another API (such as Google
 * Cloud Pub/Sub API) to receive the response.  Any API service that returns
 * long-running operations should implement the `Operations` interface so
 * developers can have a consistent client experience.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 0.14.1)",
    comments = "Source: google/longrunning/operations.proto")
public class OperationsGrpc {

  private OperationsGrpc() {}

  public static final String SERVICE_NAME = "google.longrunning.Operations";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.longrunning.GetOperationRequest,
      com.google.longrunning.Operation> METHOD_GET_OPERATION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.longrunning.Operations", "GetOperation"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.GetOperationRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.longrunning.ListOperationsRequest,
      com.google.longrunning.ListOperationsResponse> METHOD_LIST_OPERATIONS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.longrunning.Operations", "ListOperations"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.ListOperationsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.ListOperationsResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.longrunning.CancelOperationRequest,
      com.google.protobuf.Empty> METHOD_CANCEL_OPERATION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.longrunning.Operations", "CancelOperation"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.CancelOperationRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.google.longrunning.DeleteOperationRequest,
      com.google.protobuf.Empty> METHOD_DELETE_OPERATION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "google.longrunning.Operations", "DeleteOperation"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.DeleteOperationRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static OperationsStub newStub(io.grpc.Channel channel) {
    return new OperationsStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static OperationsBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new OperationsBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static OperationsFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new OperationsFutureStub(channel);
  }

  /**
   * <pre>
   * Manages long-running operations with an API service.
   * When an API method normally takes long time to complete, it can be designed
   * to return [Operation][google.longrunning.Operation] to the client, and the client can use this
   * interface to receive the real response asynchronously by polling the
   * operation resource, or using `google.watcher.v1.Watcher` interface to watch
   * the response, or pass the operation resource to another API (such as Google
   * Cloud Pub/Sub API) to receive the response.  Any API service that returns
   * long-running operations should implement the `Operations` interface so
   * developers can have a consistent client experience.
   * </pre>
   */
  public static interface Operations {

    /**
     * <pre>
     * Gets the latest state of a long-running operation.  Clients may use this
     * method to poll the operation result at intervals as recommended by the API
     * service.
     * </pre>
     */
    public void getOperation(com.google.longrunning.GetOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver);

    /**
     * <pre>
     * Lists operations that match the specified filter in the request. If the
     * server doesn't support this method, it returns
     * `google.rpc.Code.UNIMPLEMENTED`.
     * </pre>
     */
    public void listOperations(com.google.longrunning.ListOperationsRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.ListOperationsResponse> responseObserver);

    /**
     * <pre>
     * Starts asynchronous cancellation on a long-running operation.  The server
     * makes a best effort to cancel the operation, but success is not
     * guaranteed.  If the server doesn't support this method, it returns
     * `google.rpc.Code.UNIMPLEMENTED`.  Clients may use
     * [Operations.GetOperation] or other methods to check whether the
     * cancellation succeeded or the operation completed despite cancellation.
     * </pre>
     */
    public void cancelOperation(com.google.longrunning.CancelOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    /**
     * <pre>
     * Deletes a long-running operation.  It indicates the client is no longer
     * interested in the operation result. It does not cancel the operation.
     * </pre>
     */
    public void deleteOperation(com.google.longrunning.DeleteOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);
  }

  @io.grpc.ExperimentalApi
  public static abstract class AbstractOperations implements Operations, io.grpc.BindableService {

    @java.lang.Override
    public void getOperation(com.google.longrunning.GetOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_OPERATION, responseObserver);
    }

    @java.lang.Override
    public void listOperations(com.google.longrunning.ListOperationsRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.ListOperationsResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_OPERATIONS, responseObserver);
    }

    @java.lang.Override
    public void cancelOperation(com.google.longrunning.CancelOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CANCEL_OPERATION, responseObserver);
    }

    @java.lang.Override
    public void deleteOperation(com.google.longrunning.DeleteOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_OPERATION, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return OperationsGrpc.bindService(this);
    }
  }

  /**
   * <pre>
   * Manages long-running operations with an API service.
   * When an API method normally takes long time to complete, it can be designed
   * to return [Operation][google.longrunning.Operation] to the client, and the client can use this
   * interface to receive the real response asynchronously by polling the
   * operation resource, or using `google.watcher.v1.Watcher` interface to watch
   * the response, or pass the operation resource to another API (such as Google
   * Cloud Pub/Sub API) to receive the response.  Any API service that returns
   * long-running operations should implement the `Operations` interface so
   * developers can have a consistent client experience.
   * </pre>
   */
  public static interface OperationsBlockingClient {

    /**
     * <pre>
     * Gets the latest state of a long-running operation.  Clients may use this
     * method to poll the operation result at intervals as recommended by the API
     * service.
     * </pre>
     */
    public com.google.longrunning.Operation getOperation(com.google.longrunning.GetOperationRequest request);

    /**
     * <pre>
     * Lists operations that match the specified filter in the request. If the
     * server doesn't support this method, it returns
     * `google.rpc.Code.UNIMPLEMENTED`.
     * </pre>
     */
    public com.google.longrunning.ListOperationsResponse listOperations(com.google.longrunning.ListOperationsRequest request);

    /**
     * <pre>
     * Starts asynchronous cancellation on a long-running operation.  The server
     * makes a best effort to cancel the operation, but success is not
     * guaranteed.  If the server doesn't support this method, it returns
     * `google.rpc.Code.UNIMPLEMENTED`.  Clients may use
     * [Operations.GetOperation] or other methods to check whether the
     * cancellation succeeded or the operation completed despite cancellation.
     * </pre>
     */
    public com.google.protobuf.Empty cancelOperation(com.google.longrunning.CancelOperationRequest request);

    /**
     * <pre>
     * Deletes a long-running operation.  It indicates the client is no longer
     * interested in the operation result. It does not cancel the operation.
     * </pre>
     */
    public com.google.protobuf.Empty deleteOperation(com.google.longrunning.DeleteOperationRequest request);
  }

  /**
   * <pre>
   * Manages long-running operations with an API service.
   * When an API method normally takes long time to complete, it can be designed
   * to return [Operation][google.longrunning.Operation] to the client, and the client can use this
   * interface to receive the real response asynchronously by polling the
   * operation resource, or using `google.watcher.v1.Watcher` interface to watch
   * the response, or pass the operation resource to another API (such as Google
   * Cloud Pub/Sub API) to receive the response.  Any API service that returns
   * long-running operations should implement the `Operations` interface so
   * developers can have a consistent client experience.
   * </pre>
   */
  public static interface OperationsFutureClient {

    /**
     * <pre>
     * Gets the latest state of a long-running operation.  Clients may use this
     * method to poll the operation result at intervals as recommended by the API
     * service.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> getOperation(
        com.google.longrunning.GetOperationRequest request);

    /**
     * <pre>
     * Lists operations that match the specified filter in the request. If the
     * server doesn't support this method, it returns
     * `google.rpc.Code.UNIMPLEMENTED`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.ListOperationsResponse> listOperations(
        com.google.longrunning.ListOperationsRequest request);

    /**
     * <pre>
     * Starts asynchronous cancellation on a long-running operation.  The server
     * makes a best effort to cancel the operation, but success is not
     * guaranteed.  If the server doesn't support this method, it returns
     * `google.rpc.Code.UNIMPLEMENTED`.  Clients may use
     * [Operations.GetOperation] or other methods to check whether the
     * cancellation succeeded or the operation completed despite cancellation.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> cancelOperation(
        com.google.longrunning.CancelOperationRequest request);

    /**
     * <pre>
     * Deletes a long-running operation.  It indicates the client is no longer
     * interested in the operation result. It does not cancel the operation.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteOperation(
        com.google.longrunning.DeleteOperationRequest request);
  }

  public static class OperationsStub extends io.grpc.stub.AbstractStub<OperationsStub>
      implements Operations {
    private OperationsStub(io.grpc.Channel channel) {
      super(channel);
    }

    private OperationsStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OperationsStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new OperationsStub(channel, callOptions);
    }

    @java.lang.Override
    public void getOperation(com.google.longrunning.GetOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_OPERATION, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void listOperations(com.google.longrunning.ListOperationsRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.ListOperationsResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_OPERATIONS, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void cancelOperation(com.google.longrunning.CancelOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CANCEL_OPERATION, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void deleteOperation(com.google.longrunning.DeleteOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_OPERATION, getCallOptions()), request, responseObserver);
    }
  }

  public static class OperationsBlockingStub extends io.grpc.stub.AbstractStub<OperationsBlockingStub>
      implements OperationsBlockingClient {
    private OperationsBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private OperationsBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OperationsBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new OperationsBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.longrunning.Operation getOperation(com.google.longrunning.GetOperationRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_OPERATION, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.longrunning.ListOperationsResponse listOperations(com.google.longrunning.ListOperationsRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_OPERATIONS, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty cancelOperation(com.google.longrunning.CancelOperationRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CANCEL_OPERATION, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteOperation(com.google.longrunning.DeleteOperationRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_OPERATION, getCallOptions(), request);
    }
  }

  public static class OperationsFutureStub extends io.grpc.stub.AbstractStub<OperationsFutureStub>
      implements OperationsFutureClient {
    private OperationsFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private OperationsFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OperationsFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new OperationsFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> getOperation(
        com.google.longrunning.GetOperationRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_OPERATION, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.ListOperationsResponse> listOperations(
        com.google.longrunning.ListOperationsRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_OPERATIONS, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> cancelOperation(
        com.google.longrunning.CancelOperationRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CANCEL_OPERATION, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteOperation(
        com.google.longrunning.DeleteOperationRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_OPERATION, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_OPERATION = 0;
  private static final int METHODID_LIST_OPERATIONS = 1;
  private static final int METHODID_CANCEL_OPERATION = 2;
  private static final int METHODID_DELETE_OPERATION = 3;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final Operations serviceImpl;
    private final int methodId;

    public MethodHandlers(Operations serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_OPERATION:
          serviceImpl.getOperation((com.google.longrunning.GetOperationRequest) request,
              (io.grpc.stub.StreamObserver<com.google.longrunning.Operation>) responseObserver);
          break;
        case METHODID_LIST_OPERATIONS:
          serviceImpl.listOperations((com.google.longrunning.ListOperationsRequest) request,
              (io.grpc.stub.StreamObserver<com.google.longrunning.ListOperationsResponse>) responseObserver);
          break;
        case METHODID_CANCEL_OPERATION:
          serviceImpl.cancelOperation((com.google.longrunning.CancelOperationRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_DELETE_OPERATION:
          serviceImpl.deleteOperation((com.google.longrunning.DeleteOperationRequest) request,
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
      final Operations serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_GET_OPERATION,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.longrunning.GetOperationRequest,
              com.google.longrunning.Operation>(
                serviceImpl, METHODID_GET_OPERATION)))
        .addMethod(
          METHOD_LIST_OPERATIONS,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.longrunning.ListOperationsRequest,
              com.google.longrunning.ListOperationsResponse>(
                serviceImpl, METHODID_LIST_OPERATIONS)))
        .addMethod(
          METHOD_CANCEL_OPERATION,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.longrunning.CancelOperationRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_CANCEL_OPERATION)))
        .addMethod(
          METHOD_DELETE_OPERATION,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.longrunning.DeleteOperationRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_DELETE_OPERATION)))
        .build();
  }
}
