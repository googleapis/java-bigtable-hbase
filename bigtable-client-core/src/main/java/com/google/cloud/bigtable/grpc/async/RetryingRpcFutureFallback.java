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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.util.BackOff;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.scanner.ScanRetriesExhaustedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A {@link FutureFallback} that retries a {@link RetryableRpc} request.
 */
public class RetryingRpcFutureFallback<RequestT, ResponseT> implements FutureFallback<ResponseT> {

  public static <RequestT, ResponseT> RetryingRpcFutureFallback<RequestT, ResponseT> create(
      RetryOptions retryOptions, RequestT request, RetryableRpc<RequestT, ResponseT> retryableRpc) {
    return new RetryingRpcFutureFallback<RequestT, ResponseT>(retryOptions, request, retryableRpc);
  }

  protected final Log LOG = LogFactory.getLog(RetryingRpcFutureFallback.class);

  private final RequestT request;

  @VisibleForTesting
  BackOff currentBackoff;
  @VisibleForTesting
  Sleeper sleeper = Sleeper.DEFAULT;

  private final RetryableRpc<RequestT, ResponseT> retryableRpc;
  private final RetryOptions retryOptions;

  private RetryingRpcFutureFallback(RetryOptions retryOptions, RequestT request,
      RetryableRpc<RequestT, ResponseT> retryableRpc) {
    this.retryOptions = retryOptions;
    this.request = request;
    this.retryableRpc = retryableRpc;
  }

  @Override
  public ListenableFuture<ResponseT> create(Throwable t)
      throws Exception {
    if (t instanceof StatusRuntimeException) {
      StatusRuntimeException statusException = (StatusRuntimeException) t;
      Status.Code code = statusException.getStatus().getCode();
      if (retryOptions.isRetryableRead(code)) {
        return backOffAndRetry(t);
      }
    }
    return Futures.immediateFailedFuture(t);
  }

  private ListenableFuture<ResponseT> backOffAndRetry(Throwable cause) throws IOException,
      ScanRetriesExhaustedException {
    if (this.currentBackoff == null) {
      this.currentBackoff = retryOptions.createBackoff();
    }
    long nextBackOff = currentBackoff.nextBackOffMillis();
    if (nextBackOff == BackOff.STOP) {
      throw new ScanRetriesExhaustedException("Exhausted streaming retries.", cause);
    }

    sleep(nextBackOff);
    return retryableRpc.call(request);
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