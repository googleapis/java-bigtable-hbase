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

import com.google.api.client.util.Preconditions;
import com.google.cloud.bigtable.grpc.CallOptionsFactory;
import com.google.cloud.bigtable.grpc.io.Watchdog.State;
import com.google.cloud.bigtable.grpc.io.Watchdog.StreamWaitTimeoutException;
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

  private class CallToStreamObserverAdapter extends ClientCallStreamObserver<ReadRowsRequest> {
    private boolean autoFlowControlEnabled = true;

    @Override
    public void onNext(ReadRowsRequest value) {
      getCall().sendMessage(value);
    }

    @Override
    public void onError(Throwable t) {
      getCall().cancel("Cancelled by client with StreamObserver.onError()", t);
    }

    @Override
    public void onCompleted() {
      getCall().halfClose();
    }

    @Override
    public boolean isReady() {
      return getCall().isReady();
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
      getCall().request(count);
    }

    @Override
    public void setMessageCompression(boolean enable) {
      getCall().setMessageCompression(enable);
    }

    @Override
    public void cancel(@Nullable String s, @Nullable Throwable throwable) {
      getCall().cancel(s, throwable);
    }
  }

  @VisibleForTesting
  Clock clock = Clock.SYSTEM;
  private final ReadRowsRequestManager requestManager;
  private final StreamObserver<FlatRow> rowObserver;
  private final RowMerger rowMerger;

  // The number of times we've retried after a timeout
  private int timeoutRetryCount = 0;
  private final CallToStreamObserverAdapter adapter;
  private StreamObserver<ReadRowsResponse> resultObserver;
  private int totalRowsProcessed = 0;
  private volatile ReadRowsRequest nextRequest;

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
    this.rowMerger = new RowMerger(rowObserver);
    this.adapter = new CallToStreamObserverAdapter();
    this.requestManager = new ReadRowsRequestManager(request);
    this.nextRequest = request;
  }

  // This observer will be notified after ReadRowsResponse have been fully processed.
  public void setResultObserver(StreamObserver<ReadRowsResponse> resultObserver) {
    this.resultObserver = resultObserver;
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
    try {
      // restart the clock.
      super.run();
      // pre-fetch one more result, for performance reasons.
      adapter.request(1);
      if (rowObserver instanceof ClientResponseObserver) {
        ((ClientResponseObserver<ReadRowsRequest, FlatRow>) rowObserver).beforeStart(adapter);
      }
    } catch (Exception e) {
      setException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onMessage(ReadRowsResponse message) {
    try {
      resetStatusBasedBackoff();
      // We've had at least one successful RPC, reset the backoff and retry counter
      timeoutRetryCount = 0;

      ByteString previouslyProcessedKey = rowMerger.getLastCompletedRowKey();

      operationSpan.addAnnotation("Got a response");
      // This may take some time. This must not block so that gRPC worker threads don't leak.
      rowMerger.onNext(message);

      // Add an annotation for the number of rows that were returned in the previous response.
      int rowCountInLastMessage = rowMerger.getRowCountInLastMessage();
      operationSpan.addAnnotation("Processed Response", ImmutableMap.of("rowCount",
          AttributeValue.longAttributeValue(rowCountInLastMessage)));

      totalRowsProcessed += rowCountInLastMessage;
      requestManager.incrementRowCount(rowCountInLastMessage);

      ByteString lastProcessedKey = rowMerger.getLastCompletedRowKey();
      if (previouslyProcessedKey != lastProcessedKey) {
        // There was a full row found in the response.
        updateLastFoundKey(lastProcessedKey);
      } else {
        // The service indicates that it processed rows that did not match the filter, and will not
        // need to be reprocessed.
        updateLastFoundKey(message.getLastScannedRowKey());
      }

      if (adapter.autoFlowControlEnabled) {
        adapter.request(1);
      }

      if (resultObserver != null) {
        resultObserver.onNext(message);
      }
    } catch (Exception e) {
      setException(e);
    }
  }

  @Override
  protected void finalizeStats(Status status) {
    super.finalizeStats(status);
    // Add an annotation for the total number of rows that were returned across all responses.
    operationSpan.addAnnotation("Total Rows Processed",
      ImmutableMap.of("rowCount", AttributeValue.longAttributeValue(totalRowsProcessed)));
  }

  private void updateLastFoundKey(ByteString lastProcessedKey) {
    if (lastProcessedKey != null && !lastProcessedKey.isEmpty()) {
      requestManager.updateLastFoundKey(lastProcessedKey);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onClose(Status status, Metadata trailers) {
    if (status.getCause() instanceof StreamWaitTimeoutException
        && ((StreamWaitTimeoutException)status.getCause()).getState() == State.WAITING) {
      handleTimeoutError(status);
      return;
    }

    super.onClose(status, trailers);
  }

  /** {@inheritDoc} */
  @Override
  public void setException(Exception exception) {
    rowMerger.onError(exception);
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
  }

  /**
   * Special retry handling for watchdog timeouts, which uses its own fail counter.
   *
   * @return true if a retry has been scheduled
   */
  private void handleTimeoutError(Status status) {
    Preconditions.checkArgument(status.getCause() instanceof StreamWaitTimeoutException,
        "status is not caused by a StreamWaitTimeoutException");
    StreamWaitTimeoutException e = ((StreamWaitTimeoutException) status.getCause());

    // Cancel the existing rpc.
    rpcTimerContext.close();
    failedCount++;

    // Can this request be retried
    int maxRetries = retryOptions.getMaxScanTimeoutRetries();
    if (retryOptions.enableRetries() && ++timeoutRetryCount <= maxRetries) {
      LOG.warn("The client could not get a response in %d ms. Retrying the scan.",
          e.getWaitTimeMs());

      resetStatusBasedBackoff();
      performRetry(0);
    } else {
      LOG.warn("The client could not get a response after %d tries, giving up.",
          timeoutRetryCount);
      rpc.getRpcMetrics().markFailure();
      finalizeStats(status);
      setException(getExhaustedRetriesException(status));
    }
  }

  @Override
  protected void performRetry(long nextBackOff) {
    buildUpdatedRequest();
    super.performRetry(nextBackOff);
  }

  @VisibleForTesting
  ReadRowsRequest buildUpdatedRequest() {
    this.rowMerger.clearRowInProgress();
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
