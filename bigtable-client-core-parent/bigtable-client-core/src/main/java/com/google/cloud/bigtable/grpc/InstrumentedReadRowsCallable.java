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

import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.StateCheckingResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.metrics.OperationMetrics;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/** Wraps {@code ServerStreamingCallable} of data client's readRows to instrument with Timer. */
class InstrumentedReadRowsCallable<RequestT, ResponseT>
    extends ServerStreamingCallable<RequestT, ResponseT> {

  private final ServerStreamingCallable<RequestT, ResponseT> delegate;
  private final OperationMetrics metrics;

  InstrumentedReadRowsCallable(
      @Nonnull ServerStreamingCallable<RequestT, ResponseT> delegate,
      @Nonnull OperationMetrics metrics) {
    Preconditions.checkNotNull(delegate);
    Preconditions.checkNotNull(metrics);
    this.delegate = delegate;
    this.metrics = metrics;
  }

  @Override
  public void call(
      RequestT requestT,
      final ResponseObserver<ResponseT> delegate,
      ApiCallContext apiCallContext) {

    final Timer.Context rpcTimeOperation = metrics.timeOperationLatency();
    final AtomicReference<Timer.Context> firstResponseTimerRef =
        new AtomicReference<>(metrics.timeReadRowsFirstResponse());

    this.delegate.call(
        requestT,
        new StateCheckingResponseObserver<ResponseT>() {

          @Override
          protected void onStartImpl(StreamController streamController) {
            delegate.onStart(streamController);
          }

          @Override
          protected void onResponseImpl(ResponseT responseT) {
            Timer.Context firstResponseTimer = firstResponseTimerRef.getAndSet(null);
            if (firstResponseTimer != null) {
              firstResponseTimer.close();
            }
            delegate.onResponse(responseT);
          }

          @Override
          protected void onErrorImpl(Throwable throwable) {
            metrics.markOperationFailure();
            rpcTimeOperation.close();
            delegate.onError(throwable);
          }

          @Override
          protected void onCompleteImpl() {
            rpcTimeOperation.close();
            delegate.onComplete();
          }
        });
  }
}
