package com.google.cloud.bigtable.grpc.async;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.stub.ClientCallStreamObserver;

import javax.annotation.Nullable;

public class CallWrapper<RequestT, ResponseT>
    extends ClientCallStreamObserver<RequestT> {
  @SuppressWarnings("rawtypes")
  private static final ClientCall NULL_CALL = new ClientCall() {

    @Override public void start(Listener responseListener, Metadata headers) {
    }

    @Override public void request(int numMessages) {
    }

    @Override public void cancel(String message, Throwable cause) {
    }

    @Override public void halfClose() {
    }

    @Override public void sendMessage(Object message) {
    }
  };

  private boolean autoFlowControlEnabled = true;
  private ClientCall<RequestT, ResponseT> call = NULL_CALL;

  synchronized void resetCall() {
    call = NULL_CALL;
  }

  synchronized void setCallAndStart(BigtableAsyncRpc<RequestT, ResponseT> rpc,
      CallOptions callOptions, RequestT request, ClientCall.Listener<ResponseT> listener,
      Metadata metadata) {
    // There's a subtle race condition in RetryingStreamOperation which requires a separate
    // newCall/start split. The call variable needs to be set before onMessage() happens; that
    // usually will occur, but some unit tests broke with a merged newCall and start.
    this.call = rpc.newCall(callOptions);
    rpc.start(request, listener, metadata, call);
  }

  @Override public void onNext(RequestT value) {
    call.sendMessage(value);
  }

  @Override public void onError(Throwable t) {
    call.cancel("Cancelled by client with StreamObserver.onError()", t);
  }

  @Override
  public void onCompleted() {
    call.halfClose();
  }

  @Override
  public boolean isReady() {
    return call.isReady();
  }

  @Override
  public void setOnReadyHandler(Runnable onReadyHandler) {
  }

  @Override
  public void disableAutoInboundFlowControl() {
    autoFlowControlEnabled = false;
  }

  @Override
  public void request(int count) {
    call.request(count);
  }

  @Override
  public void setMessageCompression(boolean enable) {
    call.setMessageCompression(enable);
  }

  @Override
  public void cancel(@Nullable String s, @Nullable Throwable throwable) {
    call.cancel(s, throwable);
  }

  public boolean isAutoFlowControlEnabled() {
    return autoFlowControlEnabled;
  }
}