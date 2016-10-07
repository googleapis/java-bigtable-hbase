/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.Timer;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * This interface represents a logical asynchronous RPC end point, including creating a
 * {@link io.grpc.ClientCall} for a new request.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public interface BigtableAsyncRpc<REQUEST, RESPONSE> {

  public static class RpcMetrics {
    private final Timer operationTimer;
    private final Timer rpcTimer;
    private final Meter retryMeter;
    private final Meter failureMeter;
    private final Meter retriesExhastedMeter;

    public static RpcMetrics createRpcMetrics(MethodDescriptor<?, ?> descriptor) {
      String prefix = "grpc.method." + descriptor.getFullMethodName().split("/")[1];
      return new RpcMetrics(
          BigtableClientMetrics.timer(MetricLevel.Info, prefix + ".operation.latency"),
          BigtableClientMetrics.timer(MetricLevel.Debug, prefix + ".rpc.latency"),
          BigtableClientMetrics.meter(MetricLevel.Info, prefix + ".retries.performed"),
          BigtableClientMetrics.meter(MetricLevel.Info, prefix + ".failure"),
          BigtableClientMetrics.meter(MetricLevel.Info, prefix + ".retries.exhausted"));
    }

    private RpcMetrics(Timer operationTimer, Timer rpcTimer, Meter retryCounter,
        Meter failureCounter, Meter retriesExhastedCounter) {
      this.operationTimer = operationTimer;
      this.rpcTimer = rpcTimer;
      this.retryMeter = retryCounter;
      this.failureMeter = failureCounter;
      this.retriesExhastedMeter = retriesExhastedCounter;
    }

    public Timer.Context timeOperation() {
      return operationTimer.time();
    }

    public Timer.Context timeRpc() {
      return rpcTimer.time();
    }

    public void markRetry() {
      retryMeter.mark();
    }

    public void markFailure() {
      failureMeter.mark();
    }

    public void markRetriesExhasted() {
      retriesExhastedMeter.mark();
    }
  }

  /**
   * Creates a {@link io.grpc.ClientCall}.
   *
   * @param callOptions A set of gRPC options to use on this call.
   * @return A ClientCall that represents a new request.
   */
  ClientCall<REQUEST, RESPONSE> newCall(CallOptions callOptions);

  /**
   * Creates a {@link io.grpc.ClientCall}.
   *
   * @param call The ClientCall to use. See {@link BigtableAsyncRpc#newCall(CallOptions)}
   * @param request The request to send.
   * @param listener A listener which handles responses.
   * @param metadata A set of predefined headers to use.
   */
  void start(
      ClientCall<REQUEST, RESPONSE> call,
      REQUEST request,
      ClientCall.Listener<RESPONSE> listener,
      Metadata metadata);

  /**
   * Can this request be retried?
   *
   * @param request The request to send which may require introspection to determine retryability.
   * @return true if the request can be retried.
   */
  boolean isRetryable(REQUEST request);

  /**
   * <p>getMethodDescriptor.</p>
   *
   * @return {@link io.grpc.MethodDescriptor} that describes the logical endpoint.
   */
  MethodDescriptor<REQUEST, RESPONSE> getMethodDescriptor();

  RpcMetrics getRpcMetrics();
}
