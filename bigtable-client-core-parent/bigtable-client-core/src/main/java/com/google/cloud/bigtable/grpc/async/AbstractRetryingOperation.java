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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.CallOptionsFactory;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.opencensus.common.NonThrowingCloseable;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.EndSpanOptions;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

/**
 * A {@link ClientCall.Listener} that retries a {@link BigtableAsyncRpc} request.
 */
public abstract class AbstractRetryingOperation<RequestT, ResponseT, ResultT>
    extends ClientCall.Listener<ResponseT>  {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(AbstractRetryingOperation.class);
  private static final Tracer TRACER = Tracing.getTracer();

  // The server-side has a 5 minute timeout. Unary operations should be timed-out on the client side
  // after 6 minutes.
  protected static final long UNARY_DEADLINE_MINUTES = 6l;

  private static String makeSpanName(String prefix, String fullMethodName) {
    return prefix + "." + fullMethodName.replace('/', '.');
  }

  protected class GrpcFuture<RespT> extends AbstractFuture<RespT> {
    /**
     * This gets called from {@link Future#cancel(boolean)} for cancel(true). If a user explicitly
     * cancels the Future, that should trigger a cancellation of the RPC.
     */
    @Override
    protected void interruptTask() {
      if (!isDone()) {
        AbstractRetryingOperation.this.cancel("Request interrupted.");
      }
    }

    @Override
    public boolean set(@Nullable RespT resp) {
      return super.set(resp);
    }

    @Override
    public boolean setException(Throwable throwable) {
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
  protected Timer.Context operationTimerContext;
  protected Timer.Context rpcTimerContext;

  protected final Span operationSpan;

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
  public AbstractRetryingOperation(
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
    this.completionFuture = new GrpcFuture<>();
    String spanName = makeSpanName("Operation", rpc.getMethodDescriptor().getFullMethodName());
    operationSpan = TRACER.spanBuilder(spanName).setRecordEvents(true).startSpan();
  }

  /** {@inheritDoc} */
  @Override
  public void onClose(Status status, Metadata trailers) {
    try (NonThrowingCloseable s = TRACER.withSpan(operationSpan)) {
      synchronized (callLock) {
        call = null;
      }
      rpcTimerContext.close();
      // OK
      if (status.isOk()) {
        if (onOK(trailers)) {
          finalizeStats(status);
        }
      } else {
        onError(status, trailers);
      }
    }
  }

  protected void finalizeStats(Status status) {
    operationTimerContext.close();
    if (operationSpan != null) {
      operationSpan.end(
        EndSpanOptions.builder().setStatus(opencensusStatusFromGrpcCode(status.getCode())).build());
    }
  }

  // TODO: Replace this with StatusConverter.fromGrpcStatus(status) when the opencensus 0.6.0 is
  // released.
  private static io.opencensus.trace.Status opencensusStatusFromGrpcCode(io.grpc.Status.Code
      grpcCanonicaleCode) {
    switch (grpcCanonicaleCode) {
      case OK:
        return io.opencensus.trace.Status.OK;
      case CANCELLED:
        return io.opencensus.trace.Status.CANCELLED;
      case UNKNOWN:
        return io.opencensus.trace.Status.UNKNOWN;
      case INVALID_ARGUMENT:
        return io.opencensus.trace.Status.INVALID_ARGUMENT;
      case DEADLINE_EXCEEDED:
        return io.opencensus.trace.Status.DEADLINE_EXCEEDED;
      case NOT_FOUND:
        return io.opencensus.trace.Status.NOT_FOUND;
      case ALREADY_EXISTS:
        return io.opencensus.trace.Status.ALREADY_EXISTS;
      case PERMISSION_DENIED:
        return io.opencensus.trace.Status.PERMISSION_DENIED;
      case RESOURCE_EXHAUSTED:
        return io.opencensus.trace.Status.RESOURCE_EXHAUSTED;
      case FAILED_PRECONDITION:
        return io.opencensus.trace.Status.FAILED_PRECONDITION;
      case ABORTED:
        return io.opencensus.trace.Status.ABORTED;
      case OUT_OF_RANGE:
        return io.opencensus.trace.Status.OUT_OF_RANGE;
      case UNIMPLEMENTED:
        return io.opencensus.trace.Status.UNIMPLEMENTED;
      case INTERNAL:
        return io.opencensus.trace.Status.INTERNAL;
      case UNAVAILABLE:
        return io.opencensus.trace.Status.UNAVAILABLE;
      case DATA_LOSS:
        return io.opencensus.trace.Status.DATA_LOSS;
      case UNAUTHENTICATED:
        return io.opencensus.trace.Status.UNAUTHENTICATED;
    }
    throw new AssertionError("Unhandled status code " + grpcCanonicaleCode);
  }
  protected void onError(Status status, Metadata trailers) {
    Code code = status.getCode();
    // CANCELLED
    if (code == Status.Code.CANCELLED) {
      completionFuture.setException(status.asRuntimeException());
      // An explicit user cancellation is not considered a failure.
      finalizeStats(status);
      return;
    }

    // Non retry scenario
    if (!retryOptions.enableRetries() || !retryOptions.isRetryable(code)
    // Unauthenticated is special because the request never made it to
    // to the server, so all requests are retryable
        || !(isRequestRetryable() || code == Code.UNAUTHENTICATED || code == Code.UNAVAILABLE)) {
      rpc.getRpcMetrics().markFailure();
      finalizeStats(status);
      setException(status.asRuntimeException());
      return;
    }

    // Attempt retry with backoff
    long nextBackOff = getNextBackoff();
    failedCount += 1;

    // Backoffs timed out.
    if (nextBackOff == BackOff.STOP) {
      setException(getExhaustedRetriesException(status));
    } else {
      String channelId = ChannelPool.extractIdentifier(trailers);
      LOG.info("Retrying failed call. Failure #%d, got: %s on channel %s", status.getCause(),
        failedCount, status, channelId);
      performRetry(nextBackOff);
    }
  }

  protected BigtableRetriesExhaustedException getExhaustedRetriesException(Status status) {
    operationSpan.addAnnotation("exhaustedRetries");
    rpc.getRpcMetrics().markRetriesExhasted();
    finalizeStats(status);
    String message = String.format("Exhausted retries after %d failures.", failedCount);
    return new BigtableRetriesExhaustedException(message, status.asRuntimeException());
  }

  protected void performRetry(long nextBackOff) {
    operationSpan.addAnnotation("retryWithBackoff",
      ImmutableMap.of("backoff", AttributeValue.longAttributeValue(nextBackOff)));
    rpc.getRpcMetrics().markRetry();
    retryExecutorService.schedule(getRunnable(), nextBackOff, TimeUnit.MILLISECONDS);
  }

  protected Runnable getRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        AbstractRetryingOperation.this.run();
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
   * successfully complete. If a subclass has to retry, due to the message, this method will return
   * false
   * @return true if the operation was really completed.
   */
  protected abstract boolean onOK(Metadata trailers);

  protected long getNextBackoff() {
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
   * Calls {@link BigtableAsyncRpc#newCall(CallOptions)} and
   * {@link BigtableAsyncRpc#start(Object, io.grpc.ClientCall.Listener, Metadata, ClientCall)} }
   * with this as the listener so that retries happen correctly.
   */
  protected void run() {
    try (NonThrowingCloseable s = TRACER.withSpan(operationSpan)) {
      rpcTimerContext = rpc.getRpcMetrics().timeRpc();
      operationSpan.addAnnotation(Annotation.fromDescriptionAndAttributes("rpcStart",
        ImmutableMap.of("attempt", AttributeValue.longAttributeValue(failedCount))));
      Metadata metadata = new Metadata();
      metadata.merge(originalMetadata);
      synchronized (callLock) {
        // There's a subtle race condition in RetryingStreamOperation which requires a separate
        // newCall/start split. The call variable needs to be set before onMessage() happens; that
        // usually will occur, but some unit tests broke with a merged newCall and start.
        call = rpc.newCall(getCallOptions());
        rpc.start(getRetryRequest(), this, metadata, call);
      }
    }
  }

  protected CallOptions getCallOptions() {
    if (callOptions.getDeadline() != null) {
      return callOptions;
    }
    MethodDescriptor<RequestT, ResponseT> methodDescriptor = rpc.getMethodDescriptor();
    if (methodDescriptor.getType() != MethodType.UNARY) {
      if (methodDescriptor == BigtableGrpc.METHOD_READ_ROWS
          && !CallOptionsFactory.ConfiguredCallOptionsFactory.isGet(request)) {
        // This is a streaming read.
        return callOptions;
      } // else this is a 1) mutateRows or 2) SampleRowKeys or 3) a get, all of which can benefit
        // from a timeout equal to a unary operation.
    }
    return callOptions.withDeadlineAfter(UNARY_DEADLINE_MINUTES, TimeUnit.MINUTES);
  }

  protected RequestT getRetryRequest() {
    return request;
  }

  public final RequestT getOriginalRequest() {
    return request;
  }

  /**
   * Initial execution of the RPC.
   */
  public ListenableFuture<ResultT> getAsyncResult() {
    Preconditions.checkState(operationTimerContext == null);
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
