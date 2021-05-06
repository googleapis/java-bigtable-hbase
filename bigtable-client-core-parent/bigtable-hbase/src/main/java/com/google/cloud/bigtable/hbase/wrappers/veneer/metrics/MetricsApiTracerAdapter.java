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
import com.google.api.gax.tracing.ApiTracer;
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
public class MetricsApiTracerAdapter implements ApiTracer {

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

  private AtomicBoolean firstResponseRecorded;
  private volatile Context firstResponseTimer;

  public MetricsApiTracerAdapter(RpcMetrics rpcMetrics, String methodName) {
    this.rpcMetrics = rpcMetrics;
    operationTimer = rpcMetrics.timeOperation();
    lastRetryStatus = RetryStatus.PERMANENT_FAILURE;
    if (methodName.equals("ReadRows")) {
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
  public void operationSucceeded() {
    operationTimer.close();
  }

  @Override
  public void operationCancelled() {
    operationTimer.close();
  }

  @Override
  public void operationFailed(Throwable error) {
    if (lastRetryStatus == RetryStatus.RETRIES_EXHAUSTED) {
      rpcMetrics.markRetriesExhausted();
    } else {
      rpcMetrics.markFailure();
    }
    operationTimer.close();
  }

  @Override
  public void connectionSelected(String id) {}

  @Override
  public void attemptStarted(int attemptNumber) {
    lastRetryStatus = RetryStatus.PERMANENT_FAILURE;
    rpcTimer = rpcMetrics.timeRpc();
    activeRpcCounter.inc();
    rpcMeter.mark();
  }

  @Override
  public void attemptSucceeded() {
    rpcTimer.close();
    activeRpcCounter.dec();
  }

  @Override
  public void attemptCancelled() {
    rpcTimer.close();
    activeRpcCounter.dec();
  }

  @Override
  public void attemptFailed(Throwable error, Duration delay) {
    rpcTimer.close();
    lastRetryStatus = RetryStatus.ATTEMPT_RETRYABLE_FAILURE;
    rpcMetrics.markRetry();
    activeRpcCounter.dec();
    BigtableClientMetrics.meter(
            MetricLevel.Info, "grpc.errors." + Status.fromThrowable(error).getCode())
        .mark();
  }

  @Override
  public void attemptFailedRetriesExhausted(Throwable error) {
    rpcTimer.close();
    activeRpcCounter.dec();
    lastRetryStatus = RetryStatus.RETRIES_EXHAUSTED;
  }

  @Override
  public void attemptPermanentFailure(Throwable error) {
    rpcTimer.close();
    activeRpcCounter.dec();
    lastRetryStatus = RetryStatus.PERMANENT_FAILURE;
  }

  @Override
  public void lroStartFailed(Throwable error) {
    // noop
  }

  @Override
  public void lroStartSucceeded() {
    // noop
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
