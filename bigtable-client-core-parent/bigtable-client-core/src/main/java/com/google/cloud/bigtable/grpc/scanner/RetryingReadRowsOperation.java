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

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Clock;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.AbstractRetryingOperation;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.opencensus.trace.AttributeValue;

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

  private class CallToStreamObserverAdapter extends ClientCallStreamObserver<ReadRowsRequest> {
    private boolean autoFlowControlEnabled = true;

    @Override
    public void onNext(ReadRowsRequest value) {
      call.sendMessage(value);
    }

    @Override
    public void onError(Throwable t) {
      call.cancel("Cancelled by client with StreamObserver.onError()", t);
    }

    @Override
    public void onCompleted() {
      call.halfClose();
    }

    @Override
    public boolean isReady() {
      return call.isReady();
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {
    }

    @Override
    public void disableAutoInboundFlowControl() {
      autoFlowControlEnabled = false;
    }

    @Override
    public void request(int count) {
      call.request(count);
    }

    @Override
    public void setMessageCompression(boolean enable) {
      call.setMessageCompression(enable);
    }

    @Override
    public void cancel(@Nullable String s, @Nullable Throwable throwable) {
      call.cancel(s, throwable);
    }
  }

  @VisibleForTesting
  Clock clock = Clock.SYSTEM;
  private final ReadRowsRequestManager requestManager;
  private final StreamObserver<FlatRow> rowObserver;

  private final RowMerger rowMerger;
  private final CallToStreamObserverAdapter adapter;
  private final StreamObserver<ReadRowsResponse> responseObserver;

  private int totalRowsProcessed = 0;

  // The number of times we've retried after a timeout
  private volatile int timeoutRetryCount = 0;
  private volatile long lastResponseMs;
  private volatile ReadRowsRequest nextRequest;

  public RetryingReadRowsOperation(
      StreamObserver<FlatRow> rowObserver,
      StreamObserver<ReadRowsResponse> responseObserver,
      RetryOptions retryOptions,
      ReadRowsRequest request,
      BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> retryableRpc,
      CallOptions callOptions,
      ScheduledExecutorService retryExecutorService,
      Metadata originalMetadata) {
    super(retryOptions, request, retryableRpc, callOptions, retryExecutorService, originalMetadata);
    this.rowObserver = rowObserver;
    this.responseObserver = responseObserver;
    this.rowMerger = new RowMerger(rowObserver);
    this.requestManager = new ReadRowsRequestManager(request);
    this.nextRequest = request;
    this.adapter = new CallToStreamObserverAdapter();
  }

  /**
   * Updates the original request via {@link ReadRowsRequestManager#buildUpdatedRequest()}.
   */
  @Override
  protected ReadRowsRequest getRetryRequest() {
    return nextRequest;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    // restart the clock.
    rowMerger.reset();
    super.run();
     // pre-fetch one more result, for performance reasons.
    adapter.request(1);
    if (rowObserver instanceof ClientResponseObserver) {
      ((ClientResponseObserver<ReadRowsRequest, FlatRow>) rowObserver).beforeStart(adapter);
    }
    lastResponseMs = clock.currentTimeMillis();
  }

  /** {@inheritDoc} */
  @Override
  public void onMessage(ReadRowsResponse message) {
    try {
      lastResponseMs = clock.currentTimeMillis();
      resetStatusBasedBackoff();
      // We've had at least one successful RPC, reset the backoff and retry counter
      timeoutRetryCount = 0;
      if (adapter.autoFlowControlEnabled) {
        adapter.request(1);
      }
      operationSpan.addAnnotation("Got a response");
      // This may take some time. This must not block so that gRPC worker threads don't leak.
      rowMerger.onNext(message);

      updateRowCounts();
      updateLastProcessedKey(message);

      if (responseObserver != null) {
        responseObserver.onNext(message);
      }
    } catch (Exception e) {
      setException(e);
    }
  }

  private void updateRowCounts() {
    // Add an annotation for the number of rows that were returned in the previous response.
    int rowCountInLastMessage = rowMerger.getRowCountInLastMessage();
    operationSpan.addAnnotation("Processed Response", ImmutableMap.of("rowCount",
            AttributeValue.longAttributeValue(rowCountInLastMessage)));

    totalRowsProcessed += rowCountInLastMessage;
    requestManager.incrementRowCount(rowCountInLastMessage);
  }

  private void updateLastProcessedKey(ReadRowsResponse message) {
    ByteString previouslyProcessedKey = null;
    if (!message.getLastScannedRowKey().isEmpty()) {
      // The service indicates that it processed rows that did not match the filter, and will not
      // need to be reprocessed.
      previouslyProcessedKey = message.getLastScannedRowKey();
    } else {
      previouslyProcessedKey = rowMerger.getLastCompletedRowKey();
    }

    if (previouslyProcessedKey != null && !previouslyProcessedKey.isEmpty()) {
      requestManager.updateLastFoundKey(previouslyProcessedKey);
    }
  }

  @Override
  protected void finalizeStats(Status status) {
    super.finalizeStats(status);
    // Add an annotation for the total number of rows that were returned across all responses.
    operationSpan.addAnnotation("Total Rows Processed",
      ImmutableMap.of("rowCount", AttributeValue.longAttributeValue(totalRowsProcessed)));
  }

  /** {@inheritDoc} */
  @Override
  public void onClose(Status status, Metadata trailers) {
    if (status.getCode() == Status.Code.CANCELLED
        && status.getDescription() != null
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
    rowMerger.reset();
    super.setException(exception);
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
    if (!completionFuture.isDone()) {
      completionFuture.set("");
    }
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
      resetStatusBasedBackoff();
      performRetry(0);
    } else {
      throw getExhaustedRetriesException(Status.ABORTED);
    }
  }

  @Override
  protected void performRetry(long nextBackOff) {
    buildUpdatedRequst();
    super.performRetry(nextBackOff);
  }

  @VisibleForTesting
  ReadRowsRequest buildUpdatedRequst() {
    return nextRequest = requestManager.buildUpdatedRequest();
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
