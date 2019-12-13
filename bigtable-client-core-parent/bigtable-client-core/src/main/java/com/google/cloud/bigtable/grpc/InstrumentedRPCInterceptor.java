/*
 * Copyright 2019 Google LLC.
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
package com.google.cloud.bigtable.grpc;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.metrics.OperationMetrics;
import com.google.cloud.bigtable.metrics.Timer;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/** Intercept's RPC call to instrument the veneer client with latency, failure counts. */
@InternalApi("For internal usage only")
public class InstrumentedRPCInterceptor implements ClientInterceptor {

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

    final OperationMetrics metrics =
        OperationMetrics.create(methodDescriptor.getFullMethodName().split("/")[1]);
    final Timer.Context rpcLatency = metrics.timeRpcLatency().time();

    return new ClientInterceptors.CheckedForwardingClientCall<ReqT, RespT>(
        channel.newCall(methodDescriptor, callOptions)) {

      @Override
      protected void checkedStart(Listener<RespT> responseListener, Metadata metadata) {
        ClientCall.Listener<RespT> listener = wrap(responseListener, metrics, rpcLatency);
        metrics.activeRpcCounter().inc();
        metrics.markRpcPerformed();
        delegate().start(listener, metadata);
      }
    };
  }

  protected <RespT> ClientCall.Listener<RespT> wrap(
      final ClientCall.Listener<RespT> delegate,
      final OperationMetrics metrics,
      final Timer.Context rpcLatency) {
    return new SimpleForwardingClientCallListener<RespT>(delegate) {

      @Override
      public void onClose(Status status, Metadata trailers) {
        metrics.activeRpcCounter().dec();
        if (!status.isOk()) {
          metrics.markRpcErrorCode(status.getCode().name());
        }
        rpcLatency.close();
        delegate().onClose(status, trailers);
      }
    };
  }
}
