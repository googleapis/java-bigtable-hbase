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
import com.google.bigtable.v2.ReadRowsRequest;
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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

  public static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(AbstractRetryingOperation.class);

  private static final Tracer TRACER = Tracing.getTracer();
  private static final EndSpanOptions END_SPAN_OPTIONS_WITH_SAMPLE_STORE =
      EndSpanOptions.builder().setSampleToLocalSpanStore(true).build();

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

  private final ExponentialRetryAlgorithm exponentialRetryAlgorithm;
  private final ApiClock clock;
  private TimedAttemptSettings currentBackoff;

  protected final BigtableAsyncRpc<RequestT, ResponseT> rpc;
  protected final RetryOptions retryOptions;
  protected final ScheduledExecutorService retryExecutorService;

  private final RequestT request;
  private final CallOptions callOptions;
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
   * @param callOptions a {@link io.grpc.CallOptions} object.
   * @param retryExecutorService a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @param originalMetadata a {@link io.grpc.Metadata} object.
   * @param clock a {@link ApiClock} object
   */
  public AbstractRetryingOperation(
      RetryOptions retryOptions,
      RequestT request,
      BigtableAsyncRpc<RequestT, ResponseT> retryableRpc,
      CallOptions callOptions,
      ScheduledExecutorService retryExecutorService,
      Metadata originalMetadata,
      ApiClock clock) {
    this.retryOptions = retryOptions;
    this.request = request;
    this.rpc = retryableRpc;
    this.callOptions = callOptions;
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
    LOG.warn("entering onError at %s with status: %s", sdf.format(new Date()), status.toString());
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
        || !retryOptions.isRetryable(code)
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
          "Retrying failed call. Failure #%d, got: %s on channel %s.\nTrailers: %s at %s",
          status.getCause(), failedCount, status, channelId, trailers, sdf.format(new Date()));
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
    if (currentBackoff == null) {
      // Historically, the client waited for "total timeout" after the first failure.  For now,
      // that behavior is preserved, even though that's not the ideal.
      //
      // TODO: Think through retries, and create policy that works with the mental model most
      //       users would have of relating to retries.  That would likely involve updating some
      //       default settings in addition to changing the algorithm.
      currentBackoff = exponentialRetryAlgorithm.createFirstAttempt();
      LOG.warn("Created first attempt at: " + sdf.format(new Date()) + " with value: " + currentBackoff.toString());
    }
    currentBackoff = exponentialRetryAlgorithm.createNextAttempt(currentBackoff);
    LOG.warn("Created next attempt at: " + sdf.format(new Date()) + " with value: " + currentBackoff.toString());
    LOG.warn("shouldRetry is seeing maxTimeout: " + currentBackoff.getGlobalSettings().getTotalTimeout().toNanos()
            + " and first attempt starttime " + currentBackoff.getFirstAttemptStartTimeNanos() + " current time " + clock.nanoTime()  + " " + sdf.format(new Date()) +
            " and using timeSpent = " + (clock.nanoTime()
            - currentBackoff.getFirstAttemptStartTimeNanos()
            + currentBackoff.getRandomizedRetryDelay().toNanos()));
    if (!exponentialRetryAlgorithm.shouldRetry(currentBackoff)) {
      LOG.warn("Should retry was false, so not retrying at " + sdf.format(new Date()));

      // TODO: consider creating a subclass of exponentialRetryAlgorithm to encapsulate this logic
      long timeLeftNs =
          currentBackoff.getGlobalSettings().getTotalTimeout().toNanos()
              - (clock.nanoTime() - currentBackoff.getFirstAttemptStartTimeNanos());
      long timeLeftMs = TimeUnit.NANOSECONDS.toMillis(timeLeftNs);

      if (timeLeftMs > currentBackoff.getGlobalSettings().getInitialRetryDelay().toMillis()) {
        // The backoff algorithm doesn't always wait until the timeout is achieved.  Wait
        // one final time so that retries hit
        LOG.warn("Returning a value even though shouldRetry is false: " + timeLeftMs);
        return timeLeftMs;
      } else {

        // Finish for real.
        return null;
      }
    } else {
      long backoff = currentBackoff.getRandomizedRetryDelay().toMillis();
      LOG.warn("Should retry is true: retrying after %d/%s", backoff, currentBackoff.getRetryDelay().toString());
      return  backoff;
    }
  }

  @VisibleForTesting
  public boolean inRetryMode() {
    return currentBackoff != null;
  }

  /**
   * Either a response was found, or a timeout event occurred. Reset the information relating to
   * Status oriented exception handling.
   */
  protected void resetStatusBasedBackoff() {
    this.currentBackoff = null;
    this.failedCount = 0;
  }

  /**
   * createBackoff.
   *
   * @return a {@link ExponentialRetryAlgorithm} object.
   */
  private ExponentialRetryAlgorithm createRetryAlgorithm(ApiClock clock) {
    long timeoutMs = retryOptions.getMaxElapsedBackoffMillis();

    Deadline deadline = getOperationCallOptions().getDeadline();
    if (deadline != null) {
      LOG.warn("opearational call options deadline is not null: " + deadline.toString() + " current timeout " + timeoutMs);
      timeoutMs = deadline.timeRemaining(TimeUnit.MILLISECONDS);
    }
    LOG.warn("TimeoutMs: %d with deadline: %s at now (%s)", timeoutMs , deadline.toString(), sdf.format(new Date()) );

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
    try (Scope scope = TRACER.withSpan(operationSpan)) {
      rpcTimerContext = rpc.getRpcMetrics().timeRpc();
      operationSpan.addAnnotation(
          Annotation.fromDescriptionAndAttributes(
              "rpcStart",
              ImmutableMap.of("attempt", AttributeValue.longAttributeValue(failedCount))));
      Metadata metadata = new Metadata();
      metadata.merge(originalMetadata);
      LOG.warn("Issuing the RPC call at: " + sdf.format(new Date()));
      callWrapper.setCallAndStart(rpc, getRpcCallOptions(), getRetryRequest(), this, metadata);
    } catch (Exception e) {
      setException(e);
    }
  }

  /**
   * Returns the {@link CallOptions} that a user set for the entire Operation, which can span
   * multiple RPCs/retries.
   *
   * @return The {@link CallOptions}
   */
  protected CallOptions getOperationCallOptions() {
    return callOptions;
  }

  /**
   * Create an {@link CallOptions} that has a fail safe RPC deadline to make sure that unary
   * operations don't hang. This will have to be overridden for streaming RPCs like read rows.
   *
   * <p>The logic is as follows:
   *
   * <ol>
   *   <li>If the user provides a deadline, use the deadline
   *   <li>Else If this is a streaming read, don't set an explicit deadline. The {@link
   *       com.google.cloud.bigtable.grpc.io.Watchdog} will handle hanging
   *   <li>Else Set a deadline of {@link #UNARY_DEADLINE_MINUTES} minutes deadline.
   * </ol>
   *
   * @see com.google.cloud.bigtable.grpc.io.Watchdog Watchdog which handles hanging for streaming
   *     reads.
   * @return a {@link CallOptions}
   */
  protected CallOptions getRpcCallOptions() {
    if (callOptions.getDeadline() != null || isStreamingRead()) {
      // If the user set a deadline, honor it.
      // If this is a streaming read, then the Watchdog will take affect and ensure that hanging
      // does not occur.
      return getOperationCallOptions();
    } else {
      // Unary calls should fail after 6 minutes, if there isn't any response from the server.
      return callOptions.withDeadlineAfter(UNARY_DEADLINE_MINUTES, TimeUnit.MINUTES);
    }
  }

  // TODO(sduskis): This is only required because BigtableDataGrpcClient doesn't always use
  //      RetryingReadRowsOperation like it should.
  protected boolean isStreamingRead() {
    return request instanceof ReadRowsRequest
        && !CallOptionsFactory.ConfiguredCallOptionsFactory.isGet((ReadRowsRequest) request);
  }

  protected RequestT getRetryRequest() {
    return request;
  }

  /** Initial execution of the RPC. */
  public ListenableFuture<ResultT> getAsyncResult() {
    Preconditions.checkState(operationTimerContext == null);
    operationTimerContext = rpc.getRpcMetrics().timeOperation();
    currentBackoff = exponentialRetryAlgorithm.createFirstAttempt();
    LOG.warn("Created first attempt at: " + sdf.format(new Date()) + " with value: " + currentBackoff.toString());
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
