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
import com.google.common.base.Preconditions;

/** Extracted from BigtableAsyncRpc#RpcMetrics, to instrument veneer client. */
@InternalApi("For internal usage only")
public class OperationMetrics {

  private final Timer operationTimer;
  private final Meter failureMeter;
  private final Timer rpcLatency;
  private final Timer firstResponseTimer;
  private final Meter bulkMutationMeter;
  private final Counter activeRpcCounter;
  private final Meter totalRpcPerformed;

  public static OperationMetrics create(String methodName) {
    Preconditions.checkNotNull(methodName);
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
    this.firstResponseTimer =
        BigtableClientMetrics.timer(MetricLevel.Info, "grpc.method.ReadRows.firstResponse.latency");
    this.bulkMutationMeter =
        BigtableClientMetrics.meter(MetricLevel.Info, "bulk-mutator.mutations.added");
    this.activeRpcCounter = BigtableClientMetrics.counter(MetricLevel.Info, "grpc.rpc.active");
    this.totalRpcPerformed = BigtableClientMetrics.meter(MetricLevel.Info, "grpc.rpc.performed");
  }

  /** Records latency for operation to finish. */
  public Timer.Context timeOperationLatency() {
    return operationTimer.time();
  }

  /** Records latency for each Rpc. */
  public Timer timeRpcLatency() {
    return rpcLatency;
  }

  /** Counter for operation failures. */
  public void markOperationFailure() {
    failureMeter.mark();
  }

  /** Counter for Rpc failure with failure code */
  public void markRpcErrorCode(String failureCode) {
    BigtableClientMetrics.meter(MetricLevel.Info, "grpc.errors." + failureCode).mark();
  }

  /** Counter for mutation addition in BulkMutation. */
  public void markBulkMutationAddition() {
    bulkMutationMeter.mark();
  }

  /** Records latency for first ReadRows response. */
  public Timer.Context timeReadRowsFirstResponse() {
    return firstResponseTimer.time();
  }

  /** Best effort counter of active RPCs. */
  public Counter activeRpcCounter() {
    return activeRpcCounter;
  }

  /** Best effort counter of RPCs. */
  public void markRpcPerformed() {
    totalRpcPerformed.mark();
  }
}
