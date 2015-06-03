package com.google.longrunning;

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
public class OperationsGrpc {

  private static final io.grpc.stub.Method<com.google.longrunning.GetOperationRequest,
      com.google.longrunning.Operation> METHOD_GET_OPERATION =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "GetOperation",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.GetOperationRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.PARSER));
  private static final io.grpc.stub.Method<com.google.longrunning.ListOperationsRequest,
      com.google.longrunning.ListOperationsResponse> METHOD_LIST_OPERATIONS =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "ListOperations",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.ListOperationsRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.ListOperationsResponse.PARSER));
  private static final io.grpc.stub.Method<com.google.longrunning.CancelOperationRequest,
      com.google.protobuf.Empty> METHOD_CANCEL_OPERATION =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "CancelOperation",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.CancelOperationRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.PARSER));
  private static final io.grpc.stub.Method<com.google.longrunning.DeleteOperationRequest,
      com.google.protobuf.Empty> METHOD_DELETE_OPERATION =
      io.grpc.stub.Method.create(
          io.grpc.MethodType.UNARY, "DeleteOperation",
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.DeleteOperationRequest.PARSER),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.PARSER));

  public static OperationsStub newStub(io.grpc.Channel channel) {
    return new OperationsStub(channel, CONFIG);
  }

  public static OperationsBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new OperationsBlockingStub(channel, CONFIG);
  }

  public static OperationsFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new OperationsFutureStub(channel, CONFIG);
  }

  public static final OperationsServiceDescriptor CONFIG =
      new OperationsServiceDescriptor();

  @javax.annotation.concurrent.Immutable
  public static class OperationsServiceDescriptor extends
      io.grpc.stub.AbstractServiceDescriptor<OperationsServiceDescriptor> {
    public final io.grpc.MethodDescriptor<com.google.longrunning.GetOperationRequest,
        com.google.longrunning.Operation> getOperation;
    public final io.grpc.MethodDescriptor<com.google.longrunning.ListOperationsRequest,
        com.google.longrunning.ListOperationsResponse> listOperations;
    public final io.grpc.MethodDescriptor<com.google.longrunning.CancelOperationRequest,
        com.google.protobuf.Empty> cancelOperation;
    public final io.grpc.MethodDescriptor<com.google.longrunning.DeleteOperationRequest,
        com.google.protobuf.Empty> deleteOperation;

    private OperationsServiceDescriptor() {
      getOperation = createMethodDescriptor(
          "google.longrunning.Operations", METHOD_GET_OPERATION);
      listOperations = createMethodDescriptor(
          "google.longrunning.Operations", METHOD_LIST_OPERATIONS);
      cancelOperation = createMethodDescriptor(
          "google.longrunning.Operations", METHOD_CANCEL_OPERATION);
      deleteOperation = createMethodDescriptor(
          "google.longrunning.Operations", METHOD_DELETE_OPERATION);
    }

    @SuppressWarnings("unchecked")
    private OperationsServiceDescriptor(
        java.util.Map<java.lang.String, io.grpc.MethodDescriptor<?, ?>> methodMap) {
      getOperation = (io.grpc.MethodDescriptor<com.google.longrunning.GetOperationRequest,
          com.google.longrunning.Operation>) methodMap.get(
          CONFIG.getOperation.getName());
      listOperations = (io.grpc.MethodDescriptor<com.google.longrunning.ListOperationsRequest,
          com.google.longrunning.ListOperationsResponse>) methodMap.get(
          CONFIG.listOperations.getName());
      cancelOperation = (io.grpc.MethodDescriptor<com.google.longrunning.CancelOperationRequest,
          com.google.protobuf.Empty>) methodMap.get(
          CONFIG.cancelOperation.getName());
      deleteOperation = (io.grpc.MethodDescriptor<com.google.longrunning.DeleteOperationRequest,
          com.google.protobuf.Empty>) methodMap.get(
          CONFIG.deleteOperation.getName());
    }

    @java.lang.Override
    protected OperationsServiceDescriptor build(
        java.util.Map<java.lang.String, io.grpc.MethodDescriptor<?, ?>> methodMap) {
      return new OperationsServiceDescriptor(methodMap);
    }

    @java.lang.Override
    public com.google.common.collect.ImmutableList<io.grpc.MethodDescriptor<?, ?>> methods() {
      return com.google.common.collect.ImmutableList.<io.grpc.MethodDescriptor<?, ?>>of(
          getOperation,
          listOperations,
          cancelOperation,
          deleteOperation);
    }
  }

  public static interface Operations {

    public void getOperation(com.google.longrunning.GetOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver);

    public void listOperations(com.google.longrunning.ListOperationsRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.ListOperationsResponse> responseObserver);

    public void cancelOperation(com.google.longrunning.CancelOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    public void deleteOperation(com.google.longrunning.DeleteOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);
  }

  public static interface OperationsBlockingClient {

    public com.google.longrunning.Operation getOperation(com.google.longrunning.GetOperationRequest request);

    public com.google.longrunning.ListOperationsResponse listOperations(com.google.longrunning.ListOperationsRequest request);

    public com.google.protobuf.Empty cancelOperation(com.google.longrunning.CancelOperationRequest request);

    public com.google.protobuf.Empty deleteOperation(com.google.longrunning.DeleteOperationRequest request);
  }

  public static interface OperationsFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> getOperation(
        com.google.longrunning.GetOperationRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.ListOperationsResponse> listOperations(
        com.google.longrunning.ListOperationsRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> cancelOperation(
        com.google.longrunning.CancelOperationRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteOperation(
        com.google.longrunning.DeleteOperationRequest request);
  }

  public static class OperationsStub extends
      io.grpc.stub.AbstractStub<OperationsStub, OperationsServiceDescriptor>
      implements Operations {
    private OperationsStub(io.grpc.Channel channel,
        OperationsServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected OperationsStub build(io.grpc.Channel channel,
        OperationsServiceDescriptor config) {
      return new OperationsStub(channel, config);
    }

    @java.lang.Override
    public void getOperation(com.google.longrunning.GetOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.getOperation), request, responseObserver);
    }

    @java.lang.Override
    public void listOperations(com.google.longrunning.ListOperationsRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.ListOperationsResponse> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.listOperations), request, responseObserver);
    }

    @java.lang.Override
    public void cancelOperation(com.google.longrunning.CancelOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.cancelOperation), request, responseObserver);
    }

    @java.lang.Override
    public void deleteOperation(com.google.longrunning.DeleteOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          channel.newCall(config.deleteOperation), request, responseObserver);
    }
  }

  public static class OperationsBlockingStub extends
      io.grpc.stub.AbstractStub<OperationsBlockingStub, OperationsServiceDescriptor>
      implements OperationsBlockingClient {
    private OperationsBlockingStub(io.grpc.Channel channel,
        OperationsServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected OperationsBlockingStub build(io.grpc.Channel channel,
        OperationsServiceDescriptor config) {
      return new OperationsBlockingStub(channel, config);
    }

    @java.lang.Override
    public com.google.longrunning.Operation getOperation(com.google.longrunning.GetOperationRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.getOperation), request);
    }

    @java.lang.Override
    public com.google.longrunning.ListOperationsResponse listOperations(com.google.longrunning.ListOperationsRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.listOperations), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty cancelOperation(com.google.longrunning.CancelOperationRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.cancelOperation), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty deleteOperation(com.google.longrunning.DeleteOperationRequest request) {
      return blockingUnaryCall(
          channel.newCall(config.deleteOperation), request);
    }
  }

  public static class OperationsFutureStub extends
      io.grpc.stub.AbstractStub<OperationsFutureStub, OperationsServiceDescriptor>
      implements OperationsFutureClient {
    private OperationsFutureStub(io.grpc.Channel channel,
        OperationsServiceDescriptor config) {
      super(channel, config);
    }

    @java.lang.Override
    protected OperationsFutureStub build(io.grpc.Channel channel,
        OperationsServiceDescriptor config) {
      return new OperationsFutureStub(channel, config);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.Operation> getOperation(
        com.google.longrunning.GetOperationRequest request) {
      return unaryFutureCall(
          channel.newCall(config.getOperation), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.longrunning.ListOperationsResponse> listOperations(
        com.google.longrunning.ListOperationsRequest request) {
      return unaryFutureCall(
          channel.newCall(config.listOperations), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> cancelOperation(
        com.google.longrunning.CancelOperationRequest request) {
      return unaryFutureCall(
          channel.newCall(config.cancelOperation), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteOperation(
        com.google.longrunning.DeleteOperationRequest request) {
      return unaryFutureCall(
          channel.newCall(config.deleteOperation), request);
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final Operations serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder("google.longrunning.Operations")
      .addMethod(createMethodDefinition(
          METHOD_GET_OPERATION,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.longrunning.GetOperationRequest,
                com.google.longrunning.Operation>() {
              @java.lang.Override
              public void invoke(
                  com.google.longrunning.GetOperationRequest request,
                  io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
                serviceImpl.getOperation(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_LIST_OPERATIONS,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.longrunning.ListOperationsRequest,
                com.google.longrunning.ListOperationsResponse>() {
              @java.lang.Override
              public void invoke(
                  com.google.longrunning.ListOperationsRequest request,
                  io.grpc.stub.StreamObserver<com.google.longrunning.ListOperationsResponse> responseObserver) {
                serviceImpl.listOperations(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_CANCEL_OPERATION,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.longrunning.CancelOperationRequest,
                com.google.protobuf.Empty>() {
              @java.lang.Override
              public void invoke(
                  com.google.longrunning.CancelOperationRequest request,
                  io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
                serviceImpl.cancelOperation(request, responseObserver);
              }
            })))
      .addMethod(createMethodDefinition(
          METHOD_DELETE_OPERATION,
          asyncUnaryRequestCall(
            new io.grpc.stub.ServerCalls.UnaryRequestMethod<
                com.google.longrunning.DeleteOperationRequest,
                com.google.protobuf.Empty>() {
              @java.lang.Override
              public void invoke(
                  com.google.longrunning.DeleteOperationRequest request,
                  io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
                serviceImpl.deleteOperation(request, responseObserver);
              }
            }))).build();
  }
}
