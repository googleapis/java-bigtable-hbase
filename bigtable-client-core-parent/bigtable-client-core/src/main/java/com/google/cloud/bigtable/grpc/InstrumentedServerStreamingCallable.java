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
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Timer;
import java.util.concurrent.TimeUnit;

/** Wraps {@code ServerStreamingCallable} of data client's readRows to instrument with Timer. */
class InstrumentedServerStreamingCallable<RequestT, ResponseT>
    extends ServerStreamingCallable<RequestT, ResponseT> {

  private final ServerStreamingCallable<RequestT, ResponseT> delegate;
  private final OperationMetrics metrics;
  private volatile Long startTime;

  InstrumentedServerStreamingCallable(
      ServerStreamingCallable<RequestT, ResponseT> delegate, String methodName) {
    this.delegate = delegate;
    this.metrics = OperationMetrics.createOperationMetrics(methodName);
  }

  @Override
  public void call(
      RequestT requestT,
      final ResponseObserver<ResponseT> delegate,
      ApiCallContext apiCallContext) {

    final Timer.Context rpcTimeOperation = metrics.timeOperation();
    if (BigtableClientMetrics.isEnabled(BigtableClientMetrics.MetricLevel.Info)) {
      startTime = System.nanoTime();
    }

    this.delegate.call(
        requestT,
        new ResponseObserver<ResponseT>() {
          @Override
          public void onStart(StreamController streamController) {
            delegate.onStart(streamController);
          }

          @Override
          public void onResponse(ResponseT responseT) {
            if (startTime != null) {
              OperationMetrics.getFirstResponseTimer()
                  .update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
              startTime = null;
            }
            delegate.onResponse(responseT);
          }

          @Override
          public void onError(Throwable throwable) {
            metrics.markFailure();
            rpcTimeOperation.close();
            delegate.onError(throwable);
          }

          @Override
          public void onComplete() {
            rpcTimeOperation.close();
            delegate.onComplete();
          }
        });
  }
}
