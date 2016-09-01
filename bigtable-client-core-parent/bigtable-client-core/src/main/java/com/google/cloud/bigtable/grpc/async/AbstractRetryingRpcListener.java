/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * A {@link com.google.common.util.concurrent.AsyncFunction} that retries a {@link com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc} request.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public abstract class AbstractRetryingRpcListener<RequestT, ResponseT, ResultT>
    extends ClientCall.Listener<ResponseT> implements Runnable {

  /** Constant <code>LOG</code> */
  protected final static Logger LOG = new Logger(AbstractRetryingRpcListener.class);

  protected class GrpcFuture<RespT> extends AbstractFuture<RespT> {
    @Override
    protected void interruptTask() {
      if (call != null) {
        call.cancel("Request interrupted.", null);
      }
    }

    @Override
    protected boolean set(@Nullable RespT resp) {
      return super.set(resp);
    }

    @Override
    protected boolean setException(Throwable throwable) {
      return super.setException(throwable);
    }
  }

  @VisibleForTesting
  BackOff currentBackoff;
  @VisibleForTesting
  Sleeper sleeper = Sleeper.DEFAULT;

  private final BigtableAsyncRpc<RequestT, ResponseT> rpc;
  private final RetryOptions retryOptions;
  private final RequestT request;
  private final CallOptions callOptions;
  private final ScheduledExecutorService retryExecutorService;
  private int failedCount;
  private final Metadata originalMetadata;

  protected final GrpcFuture<ResultT> completionFuture = new GrpcFuture<>();
  protected ClientCall<RequestT, ResponseT> call;
  private Timer.Context operationTimerContext;
  private Timer.Context rpcTimerContext;

  /**
   * <p>Constructor for AbstractRetryingRpcListener.</p>
   *
   * @param retryOptions a {@link com.google.cloud.bigtable.config.RetryOptions} object.
   * @param request a RequestT object.
   * @param retryableRpc a {@link com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc} object.
   * @param callOptions a {@link io.grpc.CallOptions} object.
   * @param retryExecutorService a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @param originalMetadata a {@link io.grpc.Metadata} object.
   */
  public AbstractRetryingRpcListener(
          RetryOptions retryOptions,
          RequestT request,
          BigtableAsyncRpc<RequestT, ResponseT> retryableRpc,
          CallOptions callOptions,
          ScheduledExecutorService retryExecutorService,
          Metadata originalMetadata) {
    this.retryOptions = retryOptions;
    this.request = request;
    this.rpc = retryableRpc;
    this.callOptions = callOptions;
    this.retryExecutorService = retryExecutorService;
    this.originalMetadata = originalMetadata;
  }

  /** {@inheritDoc} */
  @Override
  public void onClose(Status status, Metadata trailers) {
    rpcTimerContext.close();

    Status.Code code = status.getCode();

    // OK
    if (code == Status.Code.OK) {
      operationTimerContext.close();
      onOK();
      return;
    }

    // Non retry scenario
    if (!retryOptions.enableRetries() || !retryOptions.isRetryable(code)
        || !rpc.isRetryable(getRetryRequest())) {
      this.rpc.getRpcMetrics().markFailure();
      this.operationTimerContext.close();
      completionFuture.setException(status.asRuntimeException());
      return;
    }

    // Attempt retry with backoff
    long nextBackOff = getNextBackoff();
    failedCount += 1;

    // Backoffs timed out.
    if (nextBackOff == BackOff.STOP) {
      this.rpc.getRpcMetrics().markRetriesExhasted();
      this.operationTimerContext.close();

      String message = String.format("Exhausted retries after %d failures.", failedCount);
      StatusRuntimeException cause = status.asRuntimeException();
      completionFuture.setException(new BigtableRetriesExhaustedException(message, cause));
      return;
    }

    // Perform Retry
    LOG.info("Retrying failed call. Failure #%d, got: %s", status.getCause(), failedCount, status);

    // TODO: This is probably not needed.  Consider removing the cancel here.
    if (this.call != null) {
      call.cancel(
          String.format(
              "Request being retried. Previous call failed with status %s.",
              status.getCode().name()),
          null);
    }
    call = null;

    rpc.getRpcMetrics().markRetry();
    retryExecutorService.schedule(this, nextBackOff, TimeUnit.MILLISECONDS);
  }

  /**
   * A subclass has the opportunity to perform the final operations it needs now that the RPC is
   * successfully complete.
   */
  protected abstract void onOK();

  private long getNextBackoff() {
    if (this.currentBackoff == null) {
      this.currentBackoff = retryOptions.createBackoff();
    }
    try {
      return currentBackoff.nextBackOffMillis();
    } catch (IOException e) {
      return BackOff.STOP;
    }
  }

  /**
   * <p>Getter for the field <code>completionFuture</code>.</p>
   *
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} object.
   */
  public ListenableFuture<ResultT> getCompletionFuture() {
    return completionFuture;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Calls {@link BigtableAsyncRpc#newCall(CallOptions)} and {@link
   * BigtableAsyncRpc#start(ClientCall, Object, io.grpc.ClientCall.Listener, Metadata)} with this as
   * the listener so that retries happen correctly.
   */
  @Override
  public void run() {
    this.rpcTimerContext = this.rpc.getRpcMetrics().timeRpc();
    Metadata metadata = new Metadata();
    metadata.merge(originalMetadata);
    this.call = rpc.newCall(callOptions);
    rpc.start(this.call, getRetryRequest(), this, metadata);
  }

  protected RequestT getRetryRequest() {
    return request;
  }

  public void start() {
    this.operationTimerContext = this.rpc.getRpcMetrics().timeOperation();
    run();
  }

  /**
   * <p>cancel.</p>
   */
  public void cancel() {
    if (this.call != null) {
      call.cancel("User requested cancelation.", null);
    }
  }
}
