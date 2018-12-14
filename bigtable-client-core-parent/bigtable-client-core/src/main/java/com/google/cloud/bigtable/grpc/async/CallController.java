/*
 * Copyright 2018 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc.async;

import com.google.cloud.bigtable.grpc.scanner.ResponseQueueReader;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.stub.ClientCallStreamObserver;

import javax.annotation.Nullable;

/**
 * Wraps a {@Link ClientCall}, and implements {@ClientCallStreamObserver} to allow access to the call's
 * underlying functionality.
 * <p>
 *   This class is intended to be used by the user to control flow and the life of the call.
 * </p>
 */
public class CallController<RequestT, ResponseT>
    extends ClientCallStreamObserver<RequestT> {
  @SuppressWarnings("rawtypes")
  private static final ClientCall NULL_CALL = new ClientCall() {

    @Override
    public void start(Listener responseListener, Metadata headers) {
    }

    @Override
    public void request(int numMessages) {
    }

    @Override
    public void cancel(String message, Throwable cause) {
    }

    @Override
    public void halfClose() {
    }

    @Override
    public void sendMessage(Object message) {
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

  @Override
  public void onNext(RequestT value) {
    throw new UnsupportedOperationException("onNext() and client-streaming are not supported.");
  }

  @Override
  /**
   * Cancels a request on an error.
   *
   * @see com.google.cloud.bigtable.grpc.scanner.RowMerger#onError(Throwable)
   */
  public void onError(Throwable t) {
    call.cancel("Cancelled by client with StreamObserver.onError()", t);
  }

  @Override
  public void onCompleted() {
    throw new UnsupportedOperationException("onCompleted() and client-streaming are not supported.");
  }

  @Override
  public boolean isReady() {
    return call.isReady();
  }

  @Override
  public void setOnReadyHandler(Runnable onReadyHandler) {
  }

  /**
   * Disable automatic calls to {@link ClientCall#request(int)}, and the user will explicitly request
   * more results using {@link #request(int)}.  Currently, this is only used for reading rows.
   *
   * @see com.google.cloud.bigtable.grpc.scanner.ResponseQueueReader#beforeStart(ClientCallStreamObserver)
   * @see ResponseQueueReader#getNextMergedRow()
   */
  @Override
  public void disableAutoInboundFlowControl() {
    autoFlowControlEnabled = false;
  }

  @Override
  /*
  Note: The request count may not be perfectly right, if there's a situation where request(1) is called
  at the same time as an RPC failure.
   */
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
