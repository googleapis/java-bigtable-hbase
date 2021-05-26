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

import com.google.api.core.ApiClock;
import com.google.api.core.InternalApi;
import com.google.api.gax.retrying.ExponentialRetryAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.DeadlineGenerator;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Deadline;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.opencensus.common.Scope;
import io.opencensus.contrib.grpc.util.StatusConverter;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.EndSpanOptions;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

/**
 * A {@link ClientCall.Listener} that retries a {@link BigtableAsyncRpc} request.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
@SuppressWarnings("unchecked")
public abstract class AbstractRetryingOperation<RequestT, ResponseT, ResultT>
    extends ClientCall.Listener<ResponseT> {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(AbstractRetryingOperation.class);

  private static final Tracer TRACER = Tracing.getTracer();
  private static final EndSpanOptions END_SPAN_OPTIONS_WITH_SAMPLE_STORE =
      EndSpanOptions.builder().setSampleToLocalSpanStore(true).build();

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

  private final ExponentialRetryAlgorithm exponentialRetryAlgorithm;
  private final ApiClock clock;
  private TimedAttemptSettings currentBackoff;

  protected final BigtableAsyncRpc<RequestT, ResponseT> rpc;
  protected final RetryOptions retryOptions;
  protected final ScheduledExecutorService retryExecutorService;

  private final RequestT request;
  /**
   * gRPC callOptionsFactory meant to use for both the full RPC operation and the individual RPC
   * attempts.
   */
  private final DeadlineGenerator deadlineGenerator;

  private final Metadata originalMetadata;

  protected int failedCount = 0;

  protected final GrpcFuture<ResultT> completionFuture;

  protected final CallController<RequestT, ResponseT> callWrapper;

  protected Timer.Context operationTimerContext;
  protected Timer.Context rpcTimerContext;

  protected final Span operationSpan;

  /**
   * Constructor for AbstractRetryingRpcListener.
   *
   * @param retryOptions a {@link com.google.cloud.bigtable.config.RetryOptions} object.
   * @param request a RequestT object.
   * @param retryableRpc a {@link com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc} object.
   * @param deadlineGenerator a {@link DeadlineGenerator} object.
   * @param retryExecutorService a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @param originalMetadata a {@link io.grpc.Metadata} object.
   * @param clock a {@link ApiClock} object
   */
  public AbstractRetryingOperation(
      RetryOptions retryOptions,
      RequestT request,
      BigtableAsyncRpc<RequestT, ResponseT> retryableRpc,
      DeadlineGenerator deadlineGenerator,
      ScheduledExecutorService retryExecutorService,
      Metadata originalMetadata,
      ApiClock clock) {
    this.retryOptions = retryOptions;
    this.request = request;
    this.rpc = retryableRpc;
    this.deadlineGenerator = deadlineGenerator;
    this.retryExecutorService = retryExecutorService;
    this.originalMetadata = originalMetadata;
    this.completionFuture = new GrpcFuture<>();
    String spanName = makeSpanName("Operation", rpc.getMethodDescriptor().getFullMethodName());
    this.operationSpan = TRACER.spanBuilder(spanName).setRecordEvents(true).startSpan();
    this.clock = clock;
    this.exponentialRetryAlgorithm = createRetryAlgorithm(clock);
    this.callWrapper = createCallController();
  }

  protected CallController<RequestT, ResponseT> createCallController() {
    return new CallController<>();
  }

  /** {@inheritDoc} */
  @Override
  public void onClose(Status status, Metadata trailers) {
    try (Scope scope = TRACER.withSpan(operationSpan)) {
      callWrapper.resetCall();
      rpcTimerContext.close();
      // OK
      if (status.isOk()) {
        if (onOK(trailers)) {
          finalizeStats(status);
        }
      } else {
        onError(status, trailers);
      }
    } catch (Exception e) {
      setException(e);
    }
  }

  protected void finalizeStats(Status status) {
    operationTimerContext.close();
    if (operationSpan != null) {
      operationSpan.setStatus(StatusConverter.fromGrpcStatus(status));
      operationSpan.end(END_SPAN_OPTIONS_WITH_SAMPLE_STORE);
    }
  }

  protected void onError(Status status, Metadata trailers) {
    Code code = status.getCode();
    // CANCELLED
    if (code == Status.Code.CANCELLED) {
      setException(status.asRuntimeException());
      // An explicit user cancellation is not considered a failure.
      finalizeStats(status);
      return;
    }

    String channelId = ChannelPool.extractIdentifier(trailers);
    // Non retry scenario
    if (!retryOptions.enableRetries()
        // Rst stream error has INTERNAL code but it's retryable
        || !(retryOptions.isRetryable(code) || isRstStream(status))
        // Unauthenticated is special because the request never made it to
        // to the server, so all requests are retryable
        || !(isRequestRetryable() || code == Code.UNAUTHENTICATED || code == Code.UNAVAILABLE)) {
      LOG.error(
          "Could not complete RPC. Failure #%d, got: %s on channel %s.\nTrailers: %s",
          status.getCause(), failedCount, status, channelId, trailers);
      rpc.getRpcMetrics().markFailure();
      finalizeStats(status);
      setException(status.asRuntimeException());
      return;
    }

    // Attempt retry with backoff
    Long nextBackOff = getNextBackoff();
    failedCount += 1;

    // Backoffs timed out.
    if (nextBackOff == null) {
      LOG.error(
          "All retries were exhausted. Failure #%d, got: %s on channel %s.\nTrailers: %s",
          status.getCause(), failedCount, status, channelId, trailers);
      setException(getExhaustedRetriesException(status));
    } else {
      LOG.warn(
          "Retrying failed call. Failure #%d, got: %s on channel %s.\nTrailers: %s",
          status.getCause(), failedCount, status, channelId, trailers);
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
    operationSpan.addAnnotation(
        "retryWithBackoff",
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

  private boolean isRstStream(Status status) {
    if (status.getCode() != Code.INTERNAL) {
      return false;
    }
    String description = status.getDescription();
    if (description != null) {
      return description.contains("Received Rst stream")
          || description.contains("RST_STREAM closed stream")
          || description.contains("Received RST_STREAM");
    }
    return false;
  }

  protected void setException(Exception exception) {
    completionFuture.setException(exception);
  }

  /**
   * A subclass has the opportunity to perform the final operations it needs now that the RPC is
   * successfully complete. If a subclass has to retry, due to the message, this method will return
   * false
   *
   * @return true if the operation was really completed.
   */
  protected abstract boolean onOK(Metadata trailers);

  protected Long getNextBackoff() {
    currentBackoff = exponentialRetryAlgorithm.createNextAttempt(currentBackoff);
    if (!exponentialRetryAlgorithm.shouldRetry(currentBackoff)) {

      // TODO: consider creating a subclass of exponentialRetryAlgorithm to encapsulate this logic
      long timeLeftNs =
          currentBackoff.getGlobalSettings().getTotalTimeout().toNanos()
              - (clock.nanoTime() - currentBackoff.getFirstAttemptStartTimeNanos());
      long timeLeftMs = TimeUnit.NANOSECONDS.toMillis(timeLeftNs);

      if (timeLeftMs > currentBackoff.getGlobalSettings().getInitialRetryDelay().toMillis()) {
        // The backoff algorithm doesn't always wait until the timeout is achieved.  Wait
        // one final time so that retries hit
        return timeLeftMs;
      } else {

        // Finish for real.
        return null;
      }
    } else {
      return currentBackoff.getRandomizedRetryDelay().toMillis();
    }
  }

  @VisibleForTesting
  public boolean inRetryMode() {
    // ResetStatusBasedBackoff will create the first attempt which creates currentBackoff with 0
    // attempts.
    return currentBackoff != null && currentBackoff.getAttemptCount() > 0;
  }

  /**
   * Either a response was found, or a timeout event occurred. Reset the information relating to
   * Status oriented exception handling.
   */
  protected void resetStatusBasedBackoff() {
    // Reset the backoff parameters. CreateFirstAttempt will log the current time as the time of
    // first call and enforce timeout from this timeOfFirstCall.
    this.currentBackoff = exponentialRetryAlgorithm.createFirstAttempt();
    this.failedCount = 0;
  }

  /**
   * createBackoff.
   *
   * @return a {@link ExponentialRetryAlgorithm} object.
   */
  private ExponentialRetryAlgorithm createRetryAlgorithm(ApiClock clock) {
    Optional<Long> operationTimeoutMs = deadlineGenerator.getOperationTimeoutMs();

    // TODO(stepanian): Consider if the maxElapsedBackoffMillis logic could/should be included in
    // DeadlineGenerator.
    long timeoutMs = operationTimeoutMs.or((long) retryOptions.getMaxElapsedBackoffMillis());

    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setJittered(true)

            // How long should the sleep be between RPC failure and the next RPC retry?
            .setInitialRetryDelay(toDuration(retryOptions.getInitialBackoffMillis()))

            // How fast should the retry delay increase?
            .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())

            // What is the maximum amount of sleep time between retries?
            // There needs to be some sane number for max retry delay, and it's unclear what that
            // number ought to be.  1 Minute time was chosen because some number is needed.
            .setMaxRetryDelay(Duration.of(1, ChronoUnit.MINUTES))

            // How long should we wait before giving up retries after the first failure?
            .setTotalTimeout(toDuration(timeoutMs))
            .build();
    return new ExponentialRetryAlgorithm(retrySettings, clock);
  }

  private static Duration toDuration(long millis) {
    return Duration.of(millis, ChronoUnit.MILLIS);
  }

  /**
   * Calls {@link BigtableAsyncRpc#newCall(CallOptions)} and {@link BigtableAsyncRpc#start(Object,
   * io.grpc.ClientCall.Listener, Metadata, ClientCall)} } with this as the listener so that retries
   * happen correctly.
   */
  protected void run() {

    if (currentBackoff == null) {
      // CreateFirstAttempt establishes the time when first call was made and the deadline is set to
      // `timeOfFirstCall + timeout`. Hence, its important to create first attempt before any RPCs
      // go out of client.
      currentBackoff = exponentialRetryAlgorithm.createFirstAttempt();
    }

    try (Scope scope = TRACER.withSpan(operationSpan)) {
      rpcTimerContext = rpc.getRpcMetrics().timeRpc();
      operationSpan.addAnnotation(
          Annotation.fromDescriptionAndAttributes(
              "rpcStart",
              ImmutableMap.of("attempt", AttributeValue.longAttributeValue(failedCount))));
      Metadata metadata = new Metadata();
      metadata.merge(originalMetadata);

      Optional<Deadline> rpcAttemptDeadline = deadlineGenerator.getRpcAttemptDeadline();
      CallOptions callOptions =
          rpcAttemptDeadline.isPresent()
              ? CallOptions.DEFAULT.withDeadline(rpcAttemptDeadline.get())
              : CallOptions.DEFAULT;
      callWrapper.setCallAndStart(rpc, callOptions, getRetryRequest(), this, metadata);
    } catch (Exception e) {
      setException(e);
    }
  }

  protected RequestT getRetryRequest() {
    return request;
  }

  /** Initial execution of the RPC. */
  public ListenableFuture<ResultT> getAsyncResult() {
    Preconditions.checkState(operationTimerContext == null);
    operationTimerContext = rpc.getRpcMetrics().timeOperation();

    run();
    return completionFuture;
  }

  /** Cancels the RPC. */
  public void cancel() {
    cancel("User requested cancellation.");
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
    callWrapper.cancel(message, null);
  }
}
