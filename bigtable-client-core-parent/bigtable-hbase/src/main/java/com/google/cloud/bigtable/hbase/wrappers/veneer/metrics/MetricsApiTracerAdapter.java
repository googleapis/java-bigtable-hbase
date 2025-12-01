/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.veneer.metrics;

import com.google.api.core.InternalApi;
import com.google.api.gax.tracing.ApiTracerFactory.OperationType;
import com.google.cloud.bigtable.data.v2.stub.metrics.BigtableTracer;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Counter;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.RpcMetrics;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.Timer.Context;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;
import org.threeten.bp.Duration;

/*
 * Implementation of ApiTracer to trace the logical flow of java-bigtable-hbase calls.
 * A single instance of a tracer represents a logical operation that can be annotated throughout
 * its lifecycle.
 */
@InternalApi
public class MetricsApiTracerAdapter extends BigtableTracer {

  private final Timer firstResponseLatencyTimer =
      BigtableClientMetrics.timer(MetricLevel.Info, "grpc.method.ReadRows.firstResponse.latency");
  private final Counter activeRpcCounter =
      BigtableClientMetrics.counter(MetricLevel.Info, "grpc.rpc.active");
  private final Meter rpcMeter =
      BigtableClientMetrics.meter(MetricLevel.Info, "grpc.rpc.performed");

  private final RpcMetrics rpcMetrics;
  private final Context operationTimer;

  private volatile Context rpcTimer;
  private volatile RetryStatus lastRetryStatus;

  private final AtomicBoolean firstResponseRecorded;
  private volatile Context firstResponseTimer;

  private final AtomicBoolean opDone = new AtomicBoolean();
  private final AtomicBoolean attemptDone = new AtomicBoolean();

  public MetricsApiTracerAdapter(
      RpcMetrics rpcMetrics, String methodName, OperationType operationType) {
    this.rpcMetrics = rpcMetrics;
    operationTimer = rpcMetrics.timeOperation();
    lastRetryStatus = RetryStatus.PERMANENT_FAILURE;
    if (methodName.equals("ReadRows") && operationType == OperationType.ServerStreaming) {
      this.firstResponseTimer = firstResponseLatencyTimer.time();
    }
    firstResponseRecorded = new AtomicBoolean(false);
  }

  @Override
  public Scope inScope() {
    return new Scope() {
      @Override
      public void close() {}
    };
  }

  @Override
  public void operationFinishEarly() {
    if (attemptDone.compareAndSet(false, true)) {
      activeRpcCounter.dec();
      rpcTimer.close();
    }
    if (opDone.compareAndSet(false, true)) {
      operationTimer.close();
    }
  }

  @Override
  public void operationSucceeded() {
    if (!opDone.compareAndSet(false, true)) {
      return;
    }
    operationTimer.close();
  }

  @Override
  public void operationCancelled() {
    if (!opDone.compareAndSet(false, true)) {
      return;
    }
    operationTimer.close();
  }

  @Override
  public void operationFailed(Throwable error) {
    if (!opDone.compareAndSet(false, true)) {
      return;
    }
    if (lastRetryStatus == RetryStatus.RETRIES_EXHAUSTED) {
      rpcMetrics.markRetriesExhasted();
    } else {
      rpcMetrics.markFailure();
    }
    operationTimer.close();
  }

  @Override
  public void connectionSelected(String id) {}

  @Override
  public void attemptStarted(int attemptNumber) {
    attemptDone.set(false);

    lastRetryStatus = RetryStatus.PERMANENT_FAILURE;
    rpcTimer = rpcMetrics.timeRpc();
    activeRpcCounter.inc();
    rpcMeter.mark();
  }

  @Override
  public void attemptSucceeded() {
    if (!attemptDone.compareAndSet(false, true)) {
      return;
    }
    rpcTimer.close();
    activeRpcCounter.dec();
  }

  @Override
  public void attemptCancelled() {
    if (!attemptDone.compareAndSet(false, true)) {
      return;
    }
    rpcTimer.close();
    activeRpcCounter.dec();
  }

  @Override
  public void attemptFailed(Throwable error, Duration delay) {
    if (!attemptDone.compareAndSet(false, true)) {
      return;
    }

    rpcTimer.close();
    lastRetryStatus = RetryStatus.ATTEMPT_RETRYABLE_FAILURE;
    rpcMetrics.markRetry();
    activeRpcCounter.dec();

    Status status = (error == null) ? Status.OK : Status.fromThrowable(error);

    BigtableClientMetrics.meter(MetricLevel.Info, "grpc.errors." + status.getCode()).mark();
  }

  @Override
  public void attemptFailedRetriesExhausted(Throwable error) {
    if (!attemptDone.compareAndSet(false, true)) {
      return;
    }

    rpcTimer.close();
    activeRpcCounter.dec();
    lastRetryStatus = RetryStatus.RETRIES_EXHAUSTED;
  }

  @Override
  public void attemptPermanentFailure(Throwable error) {
    if (!attemptDone.compareAndSet(false, true)) {
      return;
    }
    rpcTimer.close();
    activeRpcCounter.dec();
    lastRetryStatus = RetryStatus.PERMANENT_FAILURE;
  }

  @Override
  public void responseReceived() {
    if (firstResponseTimer != null && firstResponseRecorded.compareAndSet(false, true)) {
      firstResponseTimer.close();
    }
  }

  @Override
  public void requestSent() {}

  @Override
  public void batchRequestSent(long elementCount, long requestSize) {}

  private enum RetryStatus {
    PERMANENT_FAILURE,
    RETRIES_EXHAUSTED,
    ATTEMPT_RETRYABLE_FAILURE
  }
}
