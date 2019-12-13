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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigtable.metrics.OperationMetrics;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import javax.annotation.Nonnull;

/** Wraps data client's {@code UnaryCallable} to instrument them. */
class InstrumentedUnaryCallable<RequestT, ResponseT> extends UnaryCallable<RequestT, ResponseT> {

  private final UnaryCallable<RequestT, ResponseT> delegate;
  private final OperationMetrics metrics;

  InstrumentedUnaryCallable(
      @Nonnull UnaryCallable<RequestT, ResponseT> delegate, @Nonnull OperationMetrics metrics) {
    Preconditions.checkNotNull(delegate);
    Preconditions.checkNotNull(metrics);
    this.delegate = delegate;
    this.metrics = metrics;
  }

  @Override
  public ApiFuture<ResponseT> futureCall(RequestT requestT, ApiCallContext apiCallContext) {
    final Timer.Context rpcTimeOperation = metrics.timeOperationLatency();
    ApiFuture<ResponseT> response = delegate.futureCall(requestT, apiCallContext);

    ApiFutures.addCallback(
        response,
        new ApiFutureCallback<ResponseT>() {
          @Override
          public void onSuccess(ResponseT result) {
            rpcTimeOperation.close();
          }

          @Override
          public void onFailure(Throwable t) {
            metrics.markOperationFailure();
            rpcTimeOperation.close();
          }
        },
        MoreExecutors.directExecutor());
    return response;
  }
}
