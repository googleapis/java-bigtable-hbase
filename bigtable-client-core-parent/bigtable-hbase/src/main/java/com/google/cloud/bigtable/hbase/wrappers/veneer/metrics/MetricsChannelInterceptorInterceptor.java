/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigtable.hbase.wrappers.veneer.metrics;

import com.google.api.gax.grpc.GrpcInterceptorProvider;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.Timer.Context;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricsChannelInterceptorInterceptor implements GrpcInterceptorProvider {

  private final AtomicInteger id;

  public MetricsChannelInterceptorInterceptor() {
    id = new AtomicInteger(0);
  }

  @Override
  public List<ClientInterceptor> getInterceptors() {
    final int currentId = id.getAndIncrement();
    ClientInterceptor interceptor =
        new ClientInterceptor() {
          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            Timer timer =
                BigtableClientMetrics.timer(
                    MetricLevel.Trace, "channels.channel" + currentId + ".rpc.latency");
            final Context timerContext = timer.time();
            final ClientCall<ReqT, RespT> clientCall = next.newCall(method, callOptions);
            return new SimpleForwardingClientCall<ReqT, RespT>(clientCall) {
              @Override
              public void start(Listener<RespT> responseListener, Metadata headers) {
                super.start(
                    new SimpleForwardingClientCallListener<RespT>(responseListener) {
                      @Override
                      public void onClose(Status status, Metadata trailers) {
                        super.onClose(status, trailers);
                        timerContext.close();
                      }
                    },
                    headers);
              }
            };
          }
        };
    return Collections.singletonList(interceptor);
  }
}
