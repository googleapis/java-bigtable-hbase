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
package com.google.cloud.bigtable.config;

import static com.google.cloud.bigtable.config.BigtableVeneerSettingsFactory.getStats;

import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Timer;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;

/** Intercept's RPC call to instrument the veneer client with latency, failure counts. */
class InstrumentRPCInterceptor implements ClientInterceptor {

  private Timer.Context rpcLatency;

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

    final AtomicBoolean decremented = new AtomicBoolean(false);
    String prefix = "grpc.method." + methodDescriptor.getFullMethodName().split("/")[1];
    rpcLatency =
        BigtableClientMetrics.timer(
                BigtableClientMetrics.MetricLevel.Debug, prefix + ".rpc.latency")
            .time();

    return new ClientInterceptors.CheckedForwardingClientCall<ReqT, RespT>(
        channel.newCall(methodDescriptor, callOptions)) {
      @Override
      protected void checkedStart(Listener<RespT> responseListener, Metadata metadata)
          throws Exception {

        ClientCall.Listener<RespT> listener = wrap(responseListener, rpcLatency, decremented);
        getStats().ACTIVE_RPC_COUNTER.inc();
        getStats().RPC_METER.mark();
        delegate().start(listener, metadata);
      }

      @Override
      public void cancel(String message, Throwable cause) {
        if (!decremented.getAndSet(true)) {
          getStats().ACTIVE_RPC_COUNTER.dec();
        }
        rpcLatency.close();
        super.cancel(message, cause);
      }
    };
  }

  protected <RespT> ClientCall.Listener<RespT> wrap(
      final ClientCall.Listener<RespT> delegate,
      final Timer.Context timeRpcContext,
      final AtomicBoolean decremented) {
    return new ClientCall.Listener<RespT>() {

      @Override
      public void onHeaders(Metadata headers) {
        delegate.onHeaders(headers);
      }

      @Override
      public void onMessage(RespT message) {
        delegate.onMessage(message);
      }

      @Override
      public void onClose(Status status, Metadata trailers) {
        try {
          if (!decremented.getAndSet(true)) {
            getStats().ACTIVE_RPC_COUNTER.dec();
          }
          if (!status.isOk()) {
            BigtableClientMetrics.meter(
                    BigtableClientMetrics.MetricLevel.Info,
                    "grpc.errors." + status.getCode().name())
                .mark();
          }
          delegate.onClose(status, trailers);
        } finally {
          timeRpcContext.close();
        }
      }

      @Override
      public void onReady() {
        delegate.onReady();
      }
    };
  }
}
