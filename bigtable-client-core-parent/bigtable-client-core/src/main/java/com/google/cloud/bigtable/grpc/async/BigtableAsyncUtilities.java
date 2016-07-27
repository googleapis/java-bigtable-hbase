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


import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Utilities for creating and executing async methods.
 */
public interface BigtableAsyncUtilities {

  <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT> createStreamingAsyncRpc(
      MethodDescriptor<RequestT, ResponseT> method);

  <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT> createAsyncUnaryRpc(
      MethodDescriptor<RequestT, ResponseT> method, Predicate<RequestT> isRetryable);

  <RequestT, ResponseT> void asyncServerStreamingCall(ClientCall<RequestT, ResponseT> call,
      RequestT request, ClientCall.Listener<ResponseT> listener, Metadata metadata);

  public static class RpcMetrics {
    private Counter retriesCounter;
    private Timer operationalLatency;

    public RpcMetrics(MethodDescriptor<?, ?> descriptor) {
      this(descriptor.getFullMethodName());
    }

    public RpcMetrics(String descriptor) {
      this.retriesCounter = BigtableSession.metrics.counter(descriptor + ".retries");
      this.operationalLatency = BigtableSession.metrics.timer(descriptor + ".operation.latency");
    }

    public Timer.Context getTimer() {
      return operationalLatency.time();
    }

    public void incrementRetries() {
      retriesCounter.inc();
    }
  }

  public static class Default implements BigtableAsyncUtilities {
    private abstract class AbstractBigtableAsyncRpc<RequestT, ResponseT>
        implements BigtableAsyncRpc<RequestT, ResponseT> {
      private final MethodDescriptor<RequestT, ResponseT> method;
      private final RpcMetrics metrics;

      private AbstractBigtableAsyncRpc(MethodDescriptor<RequestT, ResponseT> method) {
        this.method = method;
        this.metrics = new RpcMetrics(method);
      }

      @Override
      public ClientCall<RequestT, ResponseT> call(RequestT request,
          ClientCall.Listener<ResponseT> listener, CallOptions callOptions, Metadata metadata) {
        return createCall(channel, callOptions, method, request, listener, 1, metadata);
      }

      @Override
      public MethodDescriptor<RequestT, ResponseT> getMethodDescriptor() {
        return method;
      }

      @Override
      public Context createTimerContext() {
        return metrics.getTimer();
      }

      @Override
      public void incrementRetryCount() {
        metrics.retriesCounter.inc();
      }
    }

    private final Channel channel;

    public Default(Channel channel) {
      this.channel = channel;
    }

    @Override
    public <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT> createAsyncUnaryRpc(
        final MethodDescriptor<RequestT, ResponseT> method, final Predicate<RequestT> isRetryable) {
      return new AbstractBigtableAsyncRpc<RequestT, ResponseT>(method) {
        @Override
        public boolean isRetryable(RequestT request) {
          return isRetryable.apply(request);
        }
      };
    }

    @Override
    public <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT>
        createStreamingAsyncRpc(final MethodDescriptor<RequestT, ResponseT> method) {
      return new AbstractBigtableAsyncRpc<RequestT, ResponseT>(method) {
        @Override
        public boolean isRetryable(RequestT request) {
          // TODO: 
          return true;
        }
      };
    }

    @Override
    public <RequestT, ResponseT> void asyncServerStreamingCall(ClientCall<RequestT, ResponseT> call,
        RequestT request, ClientCall.Listener<ResponseT> listener, Metadata metadata) {
      // gRPC treats streaming and unary calls differently for the number of responses to retrieve.
      // See createAsyncUnaryRpc for how unary calls are handled.
      //
      // See ClientCalls.startCall() for more information.
      start(call, request, listener, 1, metadata);
    }

    private <RequestT, ResponseT> ClientCall<RequestT, ResponseT> createCall(Channel channel,
        CallOptions callOptions, MethodDescriptor<RequestT, ResponseT> method, RequestT request,
        ClientCall.Listener<ResponseT> listener, int count, Metadata metadata) {
      ClientCall<RequestT, ResponseT> call = channel.newCall(method, callOptions);
      start(call, request, listener, count, metadata);
      return call;
    }

    private static <RequestT, ResponseT> void start(ClientCall<RequestT, ResponseT> call,
        RequestT request, ClientCall.Listener<ResponseT> listener, int requestCount,
        Metadata metadata) {
      call.start(listener, metadata);
      call.request(requestCount);
      try {
        call.sendMessage(request);
      } catch (Throwable t) {
        call.cancel("Exception in sendMessage.", t);
        throw Throwables.propagate(t);
      }
      try {
        call.halfClose();
      } catch (Throwable t) {
        call.cancel("Exception in halfClose.", t);
        throw Throwables.propagate(t);
      }
    }
  }
}
