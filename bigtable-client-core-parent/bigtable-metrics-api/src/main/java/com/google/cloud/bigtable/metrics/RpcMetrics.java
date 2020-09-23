/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.metrics;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;

@InternalApi
public class RpcMetrics {
  private final String methodName;
  private final Timer operationTimer;
  private final Timer rpcTimer;
  private final Meter retryMeter;
  private final Meter failureMeter;
  private final Meter retriesExhaustedMeter;
  private final Timer readRowsFirstResponseTimer;

  public static RpcMetrics createRpcMetrics(String methodName) {
    String prefix = "grpc.method." + methodName;
    return new RpcMetrics(
        methodName,
        BigtableClientMetrics.timer(MetricLevel.Info, prefix + ".operation.latency"),
        BigtableClientMetrics.timer(MetricLevel.Debug, prefix + ".rpc.latency"),
        BigtableClientMetrics.meter(MetricLevel.Info, prefix + ".retries.performed"),
        BigtableClientMetrics.meter(MetricLevel.Info, prefix + ".failure"),
        BigtableClientMetrics.meter(MetricLevel.Info, prefix + ".retries.exhausted"),
        BigtableClientMetrics.timer(MetricLevel.Info, prefix + ".firstResponse.latency"),
        BigtableClientMetrics.timer(MetricLevel.Info, prefix + ".results.latency"));
  }

  private RpcMetrics(
      String methodName,
      Timer operationTimer,
      Timer rpcTimer,
      Meter retryCounter,
      Meter failureCounter,
      Meter retriesExhaustedCounter,
      Timer readRowsFirstResponseTimer,
      Timer scannerResultsTimer) {
    this.methodName = methodName;
    this.operationTimer = operationTimer;
    this.rpcTimer = rpcTimer;
    this.retryMeter = retryCounter;
    this.failureMeter = failureCounter;
    this.retriesExhaustedMeter = retriesExhaustedCounter;
    this.readRowsFirstResponseTimer = readRowsFirstResponseTimer;
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

  public void markRetriesExhausted() {
    retriesExhaustedMeter.mark();
  }
}
