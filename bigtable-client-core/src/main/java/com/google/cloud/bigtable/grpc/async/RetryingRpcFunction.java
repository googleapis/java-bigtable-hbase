/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.async;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * A {@link AsyncFunction} that retries a {@link BigtableAsyncRpc} request.
 */
public class RetryingRpcFunction<RequestT, ResponseT>
    implements AsyncFunction<StatusRuntimeException, ResponseT> {

  protected final static Logger LOG = new Logger(RetryingRpcFunction.class);

  private final RequestT request;

  @VisibleForTesting
  BackOff currentBackoff;
  @VisibleForTesting
  Sleeper sleeper = Sleeper.DEFAULT;

  private final BigtableAsyncRpc<RequestT, ResponseT> rpc;
  private final RetryOptions retryOptions;
  private final Predicate<RequestT> isRetryable;
  private final ScheduledExecutorService retryExecutorService;
  private int failedCount;
  private final CancellationToken cancellationToken;

  public RetryingRpcFunction(
          RetryOptions retryOptions,
          RequestT request,
          BigtableAsyncRpc<RequestT, ResponseT> retryableRpc,
          Predicate<RequestT> isRetryable,
          ScheduledExecutorService retryExecutorService,
          CancellationToken cancellationToken) {
    this.retryOptions = retryOptions;
    this.request = request;
    this.rpc = retryableRpc;
    this.isRetryable = isRetryable;
    this.retryExecutorService = retryExecutorService;
    this.cancellationToken = cancellationToken;
  }

  @Override
  public ListenableFuture<ResponseT> apply(StatusRuntimeException statusException) throws Exception {
    final Status status = statusException.getStatus();
    Status.Code code = status.getCode();
    if (retryOptions.isRetryable(code) && isRetryable.apply(request)) {
      return backOffAndRetry(statusException, status);
    } else {
      return Futures.immediateFailedCheckedFuture(statusException);
    }
  }

  private ListenableFuture<ResponseT> backOffAndRetry(StatusRuntimeException cause, Status status)
      throws IOException, BigtableRetriesExhaustedException {
    if (this.currentBackoff == null) {
      this.currentBackoff = retryOptions.createBackoff();
    }
    long nextBackOff = currentBackoff.nextBackOffMillis();
    if (nextBackOff == BackOff.STOP) {
      throw new BigtableRetriesExhaustedException("Exhausted retries.", cause);
    }

    // TODO: Should this be done via a scheduled executor rather than holding on to a thread?
    sleep(nextBackOff);
    // A retryable error.
    failedCount += 1;
    LOG.info("Retrying failed call. Failure #%d, got: %s", status.getCause(), failedCount, status);
    return callRpcWithRetry();
  }

  /**
   * Calls {@link BigtableAsyncRpc#call(Object, CancellationToken)} to get a
   * {@link ListenableFuture} and adds this to that future via
   * {@link Futures#catchingAsync(ListenableFuture, Class, AsyncFunction, Executor)} so that
   * retries happen correctly.
   * @return a {@link ListenableFuture} that will retry on exceptions that are deemed retryable.
   */
  public ListenableFuture<ResponseT> callRpcWithRetry() {
    return addRetry(rpc.call(request, cancellationToken));
  }

  public ListenableFuture<ResponseT> addRetry(final ListenableFuture<ResponseT> future) {
    return Futures.catchingAsync(future, StatusRuntimeException.class, this, retryExecutorService);
  }

  public CancellationToken getCancellationToken() {
    return cancellationToken;
  }

  private void sleep(long millis) throws IOException {
    try {
      sleeper.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while sleeping for resume", e);
    }
  }
}
