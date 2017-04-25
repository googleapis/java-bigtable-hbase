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

import io.grpc.Status.Code;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
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
    extends ClientCall.Listener<ResponseT>  {

  /** Constant <code>LOG</code> */
  protected final static Logger LOG = new Logger(AbstractRetryingRpcListener.class);

  protected class GrpcFuture<RespT> extends AbstractFuture<RespT> {
    @Override
    protected void interruptTask() {
      AbstractRetryingRpcListener.this.cancel("Request interrupted.");
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

  protected BackOff currentBackoff;
  @VisibleForTesting
  Sleeper sleeper = Sleeper.DEFAULT;

  protected final BigtableAsyncRpc<RequestT, ResponseT> rpc;
  protected final RetryOptions retryOptions;
  protected final ScheduledExecutorService retryExecutorService;

  private final RequestT request;
  private final CallOptions callOptions;
  private final Metadata originalMetadata;

  protected int failedCount = 0;

  protected final GrpcFuture<ResultT> completionFuture;
  protected Object callLock = new String("");
  protected ClientCall<RequestT, ResponseT> call;
  private Timer.Context operationTimerContext;
  protected Timer.Context rpcTimerContext;

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
    this.completionFuture = createCompletionFuture();
  }

  protected GrpcFuture<ResultT> createCompletionFuture() {
    return new GrpcFuture<>();
  }

  /** {@inheritDoc} */
  @Override
  public void onClose(Status status, Metadata trailers) {
    synchronized (callLock) {
      call = null;
    }
    rpcTimerContext.close();

    Status.Code code = status.getCode();

    // OK
    if (code == Status.Code.OK) {
      operationTimerContext.close();
      onOK();
      return;
    }

    // CANCELLED
    if (code == Status.Code.CANCELLED) {
      // An explicit user cancellation is not considered a failure.
      operationTimerContext.close();
      return;
    }

    // Non retry scenario
    if (!retryOptions.enableRetries()
        || !retryOptions.isRetryable(code)
        // Unauthenticated is special because the request never made it to
        // to the server, so all requests are retryable
        || !(isRequestRetryable() || code == Code.UNAUTHENTICATED)) {
      rpc.getRpcMetrics().markFailure();
      operationTimerContext.close();
      setException(status.asRuntimeException());
      return;
    }

    // Attempt retry with backoff
    long nextBackOff = getNextBackoff();
    failedCount += 1;

    // Backoffs timed out.
    if (nextBackOff == BackOff.STOP) {
      rpc.getRpcMetrics().markRetriesExhasted();
      operationTimerContext.close();

      String message = String.format("Exhausted retries after %d failures.", failedCount);
      StatusRuntimeException cause = status.asRuntimeException();
      setException(new BigtableRetriesExhaustedException(message, cause));
      return;
    }

    // Perform Retry
    String channelId = ChannelPool.extractIdentifier(trailers);
    LOG.info("Retrying failed call. Failure #%d, got: %s on channel %s",
        status.getCause(), failedCount, status, channelId);

    rpc.getRpcMetrics().markRetry();
    retryExecutorService.schedule(getRunnable(), nextBackOff, TimeUnit.MILLISECONDS);
  }

  protected Runnable getRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        AbstractRetryingRpcListener.this.run();
      }
    };
  }

  protected boolean isRequestRetryable() {
    return rpc.isRetryable(getRetryRequest());
  }

  protected void setException(Exception exception) {
    completionFuture.setException(exception);
  }

  /**
   * A subclass has the opportunity to perform the final operations it needs now that the RPC is
   * successfully complete.
   */
  protected abstract void onOK();

  private long getNextBackoff() {
    if (currentBackoff == null) {
      currentBackoff = retryOptions.createBackoff();
    }
    try {
      return currentBackoff.nextBackOffMillis();
    } catch (IOException e) {
      return BackOff.STOP;
    }
  }

  /**
   * <p>Calls {@link BigtableAsyncRpc#newCall(CallOptions)} and {@link
   * BigtableAsyncRpc#start(ClientCall, Object, io.grpc.ClientCall.Listener, Metadata)} with this as
   * the listener so that retries happen correctly.
   */
  protected void run() {
    rpcTimerContext = rpc.getRpcMetrics().timeRpc();
    Metadata metadata = new Metadata();
    metadata.merge(originalMetadata);
    synchronized (callLock) {
      call = rpc.newCall(callOptions);
      rpc.start(call, getRetryRequest(), this, metadata);
    }
  }

  protected RequestT getRetryRequest() {
    return request;
  }

  /**
   * Initial execution of the RPC.
   */
  public ListenableFuture<ResultT> getAsyncResult() {
    operationTimerContext = rpc.getRpcMetrics().timeOperation();
    run();
    return completionFuture;
  }

  /**
   * Cancels the RPC.
   */
  public void cancel() {
    cancel("User requested cancelation.");
  }

  public ResultT getBlockingResult() {
    try {
      return getAsyncResult().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      cancel();
      throw Status.CANCELLED.withCause(e).asRuntimeException();
    } catch (ExecutionException e) {
      cancel();
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  /**
   * Cancels the RPC with a specific message.
   *
   * @param message
   */
  protected void cancel(final String message) {
    synchronized (callLock) {
      if (call != null) {
        call.cancel(message, null);
        call = null;
      }
    }
  }
}
