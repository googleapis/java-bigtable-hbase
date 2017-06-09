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
package com.google.cloud.bigtable.grpc.scanner;

import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Clock;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.AbstractRetryingOperation;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * An extension of {@link AbstractRetryingOperation} that manages retries for the readRows
 * streaming RPC. This class will keep track of the last returned row key via
 * {@link ReadRowsRequestManager} and automatically retry from the last row key .
 *
 * @author sduskis
 */
@NotThreadSafe
public class RetryingReadRowsOperation extends
    AbstractRetryingOperation<ReadRowsRequest, ReadRowsResponse, Void> implements ScanHandler {

  private static final String TIMEOUT_CANCEL_MSG = "Client side timeout induced cancellation";

  @VisibleForTesting
  Clock clock = Clock.SYSTEM;
  private final ReadRowsRequestManager requestManager;
  private final StreamObserver<FlatRow> rowObserver;
  private RowMerger rowMerger;
  private long lastResponseMs;

  // The number of times we've retried after a timeout
  private int timeoutRetryCount = 0;

  public RetryingReadRowsOperation(
      StreamObserver<FlatRow> observer,
      RetryOptions retryOptions,
      ReadRowsRequest request,
      BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> retryableRpc,
      CallOptions callOptions,
      ScheduledExecutorService retryExecutorService,
      Metadata originalMetadata) {
    super(retryOptions, request, retryableRpc, callOptions, retryExecutorService, originalMetadata);
    this.rowObserver = observer;
    this.requestManager = new ReadRowsRequestManager(request);
  }

  /**
   * The stream observer handles responses. Return null here, since a Future is not needed.
   *
   * <p>TODO(sduskis): Move {@link
   * com.google.cloud.bigtable.grpc.async.AbstractRetryingOperation.GrpcFuture} functionality into
   * a {@link StreamObserver}, and use {@link StreamObserver} in {@link
   * AbstractRetryingOperation}.
   */
  @Override
  protected GrpcFuture<Void> createCompletionFuture() {
    return null;
  }

  /**
   * Updates the original request via {@link ReadRowsRequestManager#buildUpdatedRequest()}.
   */
  @Override
  protected ReadRowsRequest getRetryRequest() {
    return requestManager.buildUpdatedRequest();
  }

  @Override
  public void run() {
    // restart the clock.
    lastResponseMs = clock.currentTimeMillis();
    this.rowMerger = new RowMerger(rowObserver);
    super.run();
  }

  /** {@inheritDoc} */
  @Override
  public void onMessage(ReadRowsResponse message) {
    // Pre-fetch the next request
    call.request(1);
    resetStatusBasedBackoff();
    lastResponseMs = clock.currentTimeMillis();
    // We've had at least one successful RPC, reset the backoff and retry counter
    timeoutRetryCount = 0;

    ByteString previouslyProcessedKey = rowMerger.getLastCompletedRowKey();

    // This may take some time.
    rowMerger.onNext(message);

    ByteString lastProcessedKey = rowMerger.getLastCompletedRowKey();
    if (previouslyProcessedKey != lastProcessedKey) {
      // There was a full row found in the response.
      updateLastFoundKey(lastProcessedKey);
    } else {
      // The service indicates that it processed rows that did not match the filter, and will not
      // need to be reprocessed.
      updateLastFoundKey(message.getLastScannedRowKey());
    }
  }

  private void updateLastFoundKey(ByteString lastProcessedKey) {
    if (lastProcessedKey != null && !lastProcessedKey.isEmpty()) {
      requestManager.updateLastFoundKey(lastProcessedKey);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onClose(Status status, Metadata trailers) {
    if (status.getCode() == Status.Code.CANCELLED
        && status.getDescription().contains(TIMEOUT_CANCEL_MSG)) {
      // If this was canceled because of handleTimeout().  The cancel is immediately
      return;
    }
    super.onClose(status, trailers);
  }

  /** {@inheritDoc} */
  @Override
  public void setException(Exception exception) {
    rowObserver.onError(exception);
    // cleanup any state that was in RowMerger. There may be a partial row in progress which needs
    // to be reset.
    rowMerger = new RowMerger(rowObserver);
  }

  /**
   * All read rows requests are retryable.
   */
  @Override
  protected boolean isRequestRetryable() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  protected boolean onOK() {
    rowMerger.onCompleted();
    return true;
  }

  /**
   * Either a response was found, or a timeout event occurred. Reset the information relating to
   * Status oriented exception handling.
   */
  void resetStatusBasedBackoff() {
    this.currentBackoff = null;
    this.failedCount = 0;
    this.lastResponseMs = clock.currentTimeMillis();
  }

  /**
   * This gets called by {@link ResumingStreamingResultScanner} when a queue is empty via {@link
   * ResponseQueueReader#getNext()}.
   *
   * @param rte a {@link ScanTimeoutException}
   * @throws BigtableRetriesExhaustedException
   */
  @Override
  public void handleTimeout(ScanTimeoutException rte) throws BigtableRetriesExhaustedException {
    if ((clock.currentTimeMillis() - lastResponseMs) < retryOptions
        .getReadPartialRowTimeoutMillis()) {
      // This gets called from ResumingStreamingResultScanner which does not have knowledge
      // about neither partial cellChunks nor responses with a lastScannedRowKey set. In either
      // case, a streaming response was sent, but the queue was not filled. In that case, just
      // continue on.
      //
      // In other words, the timeout has not occurred.  Proceed as normal, and wait for the RPC to proceed.
      return;
    } else {
      retryOnTimeout(rte);
    }
  }

  private void retryOnTimeout(ScanTimeoutException rte) throws BigtableRetriesExhaustedException {
    LOG.info("The client could not get a response in %d ms. Retrying the scan.",
      retryOptions.getReadPartialRowTimeoutMillis());

    // Cancel the existing rpc.
    cancel(TIMEOUT_CANCEL_MSG);
    rpcTimerContext.close();

    // Can this request be retried
    if (retryOptions.enableRetries()
        && ++timeoutRetryCount <= retryOptions.getMaxScanTimeoutRetries()) {
      this.rpc.getRpcMetrics().markRetry();
      resetStatusBasedBackoff();
      // run the rpc asynchronously.
      retryExecutorService.execute(getRunnable());
    } else {
      // terminate
      this.rpc.getRpcMetrics().markRetriesExhasted();
      throw new BigtableRetriesExhaustedException(
          "Exhausted streaming retries after too many timeouts", rte);
    }
  }

  @VisibleForTesting
  int getTimeoutRetryCount() {
    return timeoutRetryCount;
  }

  @VisibleForTesting
  BackOff getCurrentBackoff() {
    return currentBackoff;
  }

  /** @return the rowMerger */
  public RowMerger getRowMerger() {
    return rowMerger;
  }
}
