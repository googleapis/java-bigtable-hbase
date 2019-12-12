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
package com.google.cloud.bigtable.metrics;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;

/** Extracted from BigtableAsyncRpc#RpcMetrics, to instrument veneer client. */
@InternalApi("For internal usage only")
public class OperationMetrics {

  /** Best effort counter of active RPCs. */
  public Counter ACTIVE_RPC_COUNTER =
      BigtableClientMetrics.counter(MetricLevel.Info, "grpc.rpc.active");

  /** Best effort counter of RPCs. */
  public Meter RPC_METER = BigtableClientMetrics.meter(MetricLevel.Info, "grpc.rpc.performed");

  private final Timer operationTimer;
  private final Meter failureMeter;
  private Timer firstResponseTimer;
  private final Timer rpcLatency;

  public static OperationMetrics create(String methodName) {
    String prefix = "grpc.method." + methodName;
    return new OperationMetrics(
        BigtableClientMetrics.timer(MetricLevel.Info, prefix + ".operation.latency"),
        BigtableClientMetrics.meter(MetricLevel.Info, prefix + ".failure"),
        BigtableClientMetrics.timer(MetricLevel.Debug, prefix + ".rpc.latency"));
  }

  private OperationMetrics(Timer operationTimer, Meter failureCounter, Timer rpcLatency) {
    this.operationTimer = operationTimer;
    this.failureMeter = failureCounter;
    this.rpcLatency = rpcLatency;
  }

  public Timer.Context timeOperation() {
    return operationTimer.time();
  }

  public void markFailure() {
    failureMeter.mark();
  }

  public void markBulkMutationAddition() {
    BigtableClientMetrics.meter(
            BigtableClientMetrics.MetricLevel.Info, "bulk-mutator.mutations.added")
        .mark();
  }

  public Timer.Context timeReadRowsFirstResponse() {
    if (firstResponseTimer == null) {
      firstResponseTimer =
          BigtableClientMetrics.timer(
              MetricLevel.Info, "grpc.method.ReadRows.firstResponse.latency");
    }
    return firstResponseTimer.time();
  }

  public Timer timeRpcLatency() {
    return rpcLatency;
  }

  public void markGrpcErrorCode(String failureCode) {
    BigtableClientMetrics.meter(MetricLevel.Info, "grpc.errors." + failureCode).mark();
  }
}
