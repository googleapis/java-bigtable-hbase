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
    AbstractRetryingOperation<ReadRowsRequest, ReadRowsResponse, String> implements ScanHandler {

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
    synchronized (callLock) {
      super.run();
      // pre-fetch one more result, for performance reasons.
      call.request(1);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onMessage(ReadRowsResponse message) {
    resetStatusBasedBackoff();
    if (rowObserver instanceof ResponseQueueReader) {
      // ResponseQueueReader will only read more results if prior FlatRows were retrieved from the
      // queue.
      ((ResponseQueueReader) rowObserver).addRequestResultMarker();
    } else {
      call.request(1);
    }
    lastResponseMs = clock.currentTimeMillis();
    // We've had at least one successful RPC, reset the backoff and retry counter
    timeoutRetryCount = 0;

    ByteString previouslyProcessedKey = rowMerger.getLastCompletedRowKey();

    // This may take some time. This must not block so that gRPC worker threads don't leak.
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
      // If this was canceled because of handleTimeout(). The cancel is immediately retried or
      // completed in another fashion.
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
  protected boolean onOK(Metadata trailers) {
    rowMerger.onCompleted();
    completionFuture.set("");
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

  @Override
  public void requestResult() {
    synchronized(callLock) {
      if (call != null) {
        call.request(1);
      }
    }
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
    if ((clock.currentTimeMillis() - lastResponseMs) >= retryOptions
        .getReadPartialRowTimeoutMillis()) {
      // This gets called from ResumingStreamingResultScanner which does not have knowledge
      // about neither partial cellChunks nor responses with a lastScannedRowKey set. In either
      // case, a streaming response was sent, but the queue was not filled. In that case, just
      // continue on.
      //
      // In other words, the timeout has not occurred. Proceed as normal, and wait for the RPC to
      // proceed.
      retryOnTimeout(rte);
    }
  }

  private void retryOnTimeout(ScanTimeoutException rte) throws BigtableRetriesExhaustedException {
    LOG.info("The client could not get a response in %d ms. Retrying the scan.",
      retryOptions.getReadPartialRowTimeoutMillis());

    // Cancel the existing rpc.
    cancel(TIMEOUT_CANCEL_MSG);
    rpcTimerContext.close();
    failedCount++;

    // Can this request be retried
    int maxRetries = retryOptions.getMaxScanTimeoutRetries();
    if (retryOptions.enableRetries() && ++timeoutRetryCount <= maxRetries) {
      rpc.getRpcMetrics().markRetry();
      resetStatusBasedBackoff();
      run();
    } else {
      throw getExhaustedRetriesException(Status.ABORTED);
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

  @VisibleForTesting
  RowMerger getRowMerger() {
    return rowMerger;
  }
}
