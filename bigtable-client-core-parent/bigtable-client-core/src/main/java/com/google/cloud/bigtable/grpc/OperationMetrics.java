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

import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.Timer;

/** Extracted from BigtableAsyncRpc#RpcMetrics, to instrument veneer client. */
class OperationMetrics {
  private static Timer firstResponseTimer;
  private final Timer operationTimer;
  private final Meter failureMeter;

  static synchronized Timer getFirstResponseTimer() {
    if (firstResponseTimer == null) {
      firstResponseTimer =
          BigtableClientMetrics.timer(
              BigtableClientMetrics.MetricLevel.Info, "grpc.method.ReadRows.firstResponse.latency");
    }
    return firstResponseTimer;
  }

  static OperationMetrics createOperationMetrics(String methodName) {
    String prefix = "grpc.method." + methodName;
    return new OperationMetrics(
        BigtableClientMetrics.timer(MetricLevel.Info, prefix + ".operation.latency"),
        BigtableClientMetrics.meter(MetricLevel.Info, prefix + ".failure"));
  }

  private OperationMetrics(Timer operationTimer, Meter failureCounter) {
    this.operationTimer = operationTimer;
    this.failureMeter = failureCounter;
  }

  Timer.Context timeOperation() {
    return operationTimer.time();
  }

  void markFailure() {
    failureMeter.mark();
  }
}
