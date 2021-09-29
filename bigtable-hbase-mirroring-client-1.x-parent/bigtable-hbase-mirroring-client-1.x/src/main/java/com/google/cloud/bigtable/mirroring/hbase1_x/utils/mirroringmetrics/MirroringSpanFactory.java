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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics;

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.MIRRORING_LATENCY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.PRIMARY_ERRORS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.PRIMARY_LATENCY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.SECONDARY_ERRORS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.SECONDARY_LATENCY;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.WriteOperationFutureCallback;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOAndInterruptedException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.opencensus.common.Scope;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.Tracer;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

@InternalApi("For internal usage only")
public class MirroringSpanFactory {
  private final Tracer tracer;
  private final MirroringMetricsRecorder mirroringMetricsRecorder;

  public MirroringSpanFactory(Tracer tracer, MirroringMetricsRecorder mirroringMetricsRecorder) {
    this.tracer = tracer;
    this.mirroringMetricsRecorder = mirroringMetricsRecorder;
  }

  public Runnable wrapWithCurrentSpan(final Runnable runnable) {
    final Span span = getCurrentSpan();
    return new Runnable() {
      @Override
      public void run() {
        try (Scope scope = spanAsScope(span)) {
          runnable.run();
        }
      }
    };
  }

  public <T> FutureCallback<? super T> wrapWithCurrentSpan(final FutureCallback<T> callback) {
    final Span span = getCurrentSpan();
    return new FutureCallback<T>() {
      @Override
      public void onSuccess(@NullableDecl T t) {
        try (Scope scope = spanAsScope(span)) {
          callback.onSuccess(t);
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        try (Scope scope = spanAsScope(span)) {
          callback.onFailure(throwable);
        }
      }
    };
  }

  public <T> Callable<T> wrapWithCurrentSpan(final Callable<T> callable) {
    final Span span = getCurrentSpan();
    return new Callable<T>() {
      @Override
      public T call() throws Exception {
        try (Scope scope = spanAsScope(span)) {
          return callable.call();
        }
      }
    };
  }

  public void asyncCloseSpanWhenCompleted(ListenableFuture<Void> onLastReferenceClosed) {
    final Span span = MirroringSpanFactory.this.asyncCloseSpan();
    onLastReferenceClosed.addListener(
        new Runnable() {
          @Override
          public void run() {
            span.end();
          }
        },
        MoreExecutors.directExecutor());
  }

  private Span asyncCloseSpan() {
    return tracer.spanBuilder("asyncClose").startSpan();
  }

  public <T> T wrapPrimaryOperation(
      CallableThrowingIOException<T> operationRunner, HBaseOperation operationName)
      throws IOException {
    try {
      return wrapPrimaryOperationAndMeasure(operationRunner, operationName);
    } catch (InterruptedException e) {
      assert false;
      throw new IllegalStateException();
    }
  }

  public <T> void wrapPrimaryOperation(
      CallableThrowingIOAndInterruptedException<T> operationRunner, HBaseOperation operationName)
      throws IOException, InterruptedException {
    wrapPrimaryOperationAndMeasure(operationRunner, operationName);
  }

  public <T> T wrapSecondaryOperation(
      CallableThrowingIOException<T> operationRunner, HBaseOperation operationName)
      throws IOException {
    try {
      return wrapSecondaryOperationAndMeasure(operationRunner, operationName);
    } catch (InterruptedException e) {
      assert false;
      throw new IllegalStateException();
    }
  }

  public <T> T wrapSecondaryOperation(
      CallableThrowingIOAndInterruptedException<T> operationRunner, HBaseOperation operationName)
      throws IOException, InterruptedException {
    return wrapSecondaryOperationAndMeasure(operationRunner, operationName);
  }

  public <T> FutureCallback<T> wrapReadVerificationCallback(final FutureCallback<T> callback) {
    return new FutureCallback<T>() {
      @Override
      public void onSuccess(@NullableDecl T t) {
        try (Scope scope = MirroringSpanFactory.this.verificationScope()) {
          callback.onSuccess(t);
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        try (Scope scope = MirroringSpanFactory.this.verificationScope()) {
          callback.onFailure(throwable);
        }
      }
    };
  }

  public <T> WriteOperationFutureCallback<T> wrapWriteOperationCallback(
      final WriteOperationFutureCallback<T> callback) {
    return new WriteOperationFutureCallback<T>() {
      @Override
      public void onFailure(Throwable throwable) {
        try (Scope scope = MirroringSpanFactory.this.writeErrorScope()) {
          callback.onFailure(throwable);
        }
      }
    };
  }

  public Scope flowControlScope() {
    return flowControlSpanBuilder().startScopedSpan();
  }

  public Scope verificationScope() {
    return tracer.spanBuilder("verification").startScopedSpan();
  }

  public Scope writeErrorScope() {
    return tracer.spanBuilder("writeErrors").startScopedSpan();
  }

  public Scope operationScope(HBaseOperation name) {
    return new MirroringOperationScope(name);
  }

  public Span getCurrentSpan() {
    return tracer.getCurrentSpan();
  }

  public Scope scheduleFlushScope() {
    return tracer.spanBuilder("scheduleFlush").startScopedSpan();
  }

  public Scope spanAsScope(Span span) {
    return tracer.withSpan(span);
  }

  private <T> T wrapPrimaryOperationAndMeasure(
      CallableThrowingIOAndInterruptedException<T> operationRunner, HBaseOperation operationName)
      throws IOException, InterruptedException {
    return wrapOperationAndMeasure(
        operationRunner,
        PRIMARY_LATENCY,
        PRIMARY_ERRORS,
        this.primaryOperationScope(),
        operationName);
  }

  private <T> T wrapSecondaryOperationAndMeasure(
      CallableThrowingIOAndInterruptedException<T> operationRunner, HBaseOperation operationName)
      throws IOException, InterruptedException {
    return wrapOperationAndMeasure(
        operationRunner,
        SECONDARY_LATENCY,
        SECONDARY_ERRORS,
        this.secondaryOperationsScope(),
        operationName);
  }

  private <T> T wrapOperationAndMeasure(
      CallableThrowingIOAndInterruptedException<T> operationRunner,
      MeasureLong latencyMeasure,
      MeasureLong errorMeasure,
      Scope scope,
      HBaseOperation operationName)
      throws IOException, InterruptedException {
    boolean operationFailed = false;

    Stopwatch stopwatch = Stopwatch.createUnstarted();
    try (Scope scope1 = scope) {
      stopwatch.start();
      return operationRunner.call();
    } catch (IOException | InterruptedException e) {
      operationFailed = true;
      throw e;
    } finally {
      stopwatch.stop();
      mirroringMetricsRecorder.recordOperation(
          operationName,
          latencyMeasure,
          stopwatch.elapsed(TimeUnit.MILLISECONDS),
          errorMeasure,
          operationFailed);
    }
  }

  private Scope primaryOperationScope() {
    return tracer.spanBuilder("primary").startScopedSpan();
  }

  private Scope secondaryOperationsScope() {
    return tracer.spanBuilder("secondary").startScopedSpan();
  }

  private SpanBuilder flowControlSpanBuilder() {
    return tracer.spanBuilder("flowControl");
  }

  private class MirroringOperationScope implements Scope {
    private final Scope scope;
    private final HBaseOperation operation;
    private final Stopwatch stopwatch;

    public MirroringOperationScope(HBaseOperation operation) {
      this.scope =
          MirroringSpanFactory.this.tracer.spanBuilder(operation.getString()).startScopedSpan();
      this.stopwatch = Stopwatch.createStarted();
      this.operation = operation;
    }

    @Override
    public void close() {
      this.stopwatch.stop();
      MirroringSpanFactory.this.mirroringMetricsRecorder.recordOperation(
          this.operation, MIRRORING_LATENCY, this.stopwatch.elapsed(TimeUnit.MILLISECONDS));
      this.scope.close();
    }
  }
}
