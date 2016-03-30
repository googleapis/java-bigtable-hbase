/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import java.util.List;

import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Utilities for creating and executing async methods.
 */
public interface BigtableAsyncUtilities {

  <RequestT, ResponseT, OutputT> BigtableAsyncRpc<RequestT, List<OutputT>> createStreamingAsyncRpc(
      MethodDescriptor<RequestT, ResponseT> method,
      Function<List<ResponseT>, List<OutputT>> function);

  <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT> createAsyncUnaryRpc(
      MethodDescriptor<RequestT, ResponseT> method, Predicate<RequestT> isRetryable);

  <RequestT, ResponseT> void asyncServerStreamingCall(ClientCall<RequestT, ResponseT> call,
      RequestT request, ClientCall.Listener<ResponseT> listener);

  public static class Default implements BigtableAsyncUtilities {
    private final Channel channel;

    public Default(Channel channel) {
      this.channel = channel;
    }

    @Override
    public <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT> createAsyncUnaryRpc(
        final MethodDescriptor<RequestT, ResponseT> method, final Predicate<RequestT> isRetryable) {
      return new BigtableAsyncRpc<RequestT, ResponseT>() {
        @Override
        public ListenableFuture<ResponseT> call(
            RequestT request, CancellationToken cancellationToken) {
          AsyncUnaryOperationObserver<ResponseT> listener = new AsyncUnaryOperationObserver<>();
          ClientCall<RequestT, ResponseT> call = channel.newCall(method, CallOptions.DEFAULT);
          // Initially ask for two responses from flow-control so that if a misbehaving server sends
          // more than one responses, we can catch it and fail it in the listener.
          //
          // See ClientCalls.startCall() for more information.
          start(call, request, listener, 2);
          addCancellationListener(cancellationToken, call);
          return listener.getCompletionFuture();
        }

        @Override
        public boolean isRetryable(RequestT request) {
          return isRetryable.apply(request);
        }
      };
    }

    @Override
    public <RequestT, ResponseT, OutputT>
        BigtableAsyncRpc<RequestT, List<OutputT>> createStreamingAsyncRpc(
            final MethodDescriptor<RequestT, ResponseT> method,
            final Function<List<ResponseT>, List<OutputT>> function) {
      return new BigtableAsyncRpc<RequestT, List<OutputT>>() {
        @Override
        public ListenableFuture<List<OutputT>> call(RequestT request,
            CancellationToken cancellationToken) {
          ClientCall<RequestT, ResponseT> call = channel.newCall(method, CallOptions.DEFAULT);
          addCancellationListener(cancellationToken, call);
          CollectingClientCallListener<ResponseT> responseCollector =
              new CollectingClientCallListener<>(call);
          asyncServerStreamingCall(call, request, responseCollector);
          return Futures.transform(responseCollector.getResponseCompleteFuture(), function);
        }

        @Override
        public boolean isRetryable(RequestT request) {
          return true;
        }
      };
    }

    @Override
    public <RequestT, ResponseT> void asyncServerStreamingCall(ClientCall<RequestT, ResponseT> call,
        RequestT request, ClientCall.Listener<ResponseT> listener) {
      // gRPC treats streaming and unary calls differently for the number of responses to retrieve.
      // See createAsyncUnaryRpc for how unary calls are handled.
      //
      // See ClientCalls.startCall() for more information.
      start(call, request, listener, 1);
    }

    private static <RequestT, ResponseT> void start(ClientCall<RequestT, ResponseT> call,
        RequestT request, ClientCall.Listener<ResponseT> listener, int requestCount) {
      call.start(listener, new Metadata());
      call.request(requestCount);
      try {
        call.sendMessage(request);
        call.halfClose();
      } catch (Throwable t) {
        call.cancel();
        throw Throwables.propagate(t);
      }
    }

    private static void addCancellationListener(CancellationToken token,
        final ClientCall<?, ?> call) {
      if (token != null) {
        token.addListener(new Runnable() {
          @Override
          public void run() {
            call.cancel();
          }
        }, MoreExecutors.directExecutor());
      }
    }
  }
}
