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
package com.google.cloud.bigtable.hbase.wrappers.veneer.metrics;

import com.google.api.gax.tracing.ApiTracer;
import com.google.cloud.bigtable.metrics.RpcMetrics;
import com.google.cloud.bigtable.metrics.Timer.Context;
import com.google.common.base.Stopwatch;
import org.threeten.bp.Duration;

public class MetricsApiTracerAdapter implements ApiTracer {

  private final RpcMetrics rpcMetrics;
  private Context operationTimer;
  private Context rpcTimer;

  private int attemptCount = 0;
  private Stopwatch attemptTimer;
  private long attemptResponseCount = 0;
  private volatile RetryStatus call;

  public MetricsApiTracerAdapter(RpcMetrics rpcMetrics) {
    this.rpcMetrics = rpcMetrics;
    operationTimer = rpcMetrics.timeOperation();
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
  public void operationCancelled() {}

  @Override
  public void operationFailed(Throwable error) {
    operationFailed(error, call);
  }

  public void operationFailed(Throwable error, RetryStatus retryStatus) {
    switch (retryStatus) {
      case PERMANENT_FAILURE:
        rpcMetrics.markFailure();
        break;
      case RETRIES_EXHAUSTED:
        rpcMetrics.markRetriesExhausted();
        break;
      default:
        rpcMetrics.markFailure();
    }
  }

  @Override
  public void connectionSelected(String id) {}

  @Override
  public void attemptStarted(int attemptNumber) {
    rpcTimer = rpcMetrics.timeRpc();
    attemptCount++;
    attemptResponseCount = 0;
  }

  @Override
  public void attemptSucceeded() {
    rpcTimer.close();
  }

  @Override
  public void attemptCancelled() {
    rpcTimer.close();
  }

  @Override
  public void attemptFailed(Throwable error, Duration delay) {
    rpcTimer.close();
    rpcMetrics.markFailure();
  }

  @Override
  public void attemptFailedRetriesExhausted(Throwable error) {
    rpcTimer.close();
    call = RetryStatus.RETRIES_EXHAUSTED;
    rpcMetrics.markRetriesExhausted();
  }

  @Override
  public void attemptPermanentFailure(Throwable error) {
    call = RetryStatus.PERMANENT_FAILURE;
    rpcTimer.close();
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
    attemptResponseCount++;
  }

  @Override
  public void requestSent() {}

  @Override
  public void batchRequestSent(long elementCount, long requestSize) {}

  private enum RetryStatus {
    PERMANENT_FAILURE,
    RETRIES_EXHAUSTED
  }
}
