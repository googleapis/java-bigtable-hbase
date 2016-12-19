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

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.AbstractRetryingRpcListener;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.protobuf.ByteString;

import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;

/**
 * An extension of {@link AbstractRetryingRpcListener} that manages retries for the readRows
 * streaming RPC. This class will keep track of the last returned row key via
 * {@link ReadRowsRequestManager} and automatically retry from the last row key .
 *
 * @author sduskis
 */
public class ScannerRetryListener
    extends AbstractRetryingRpcListener<ReadRowsRequest, ReadRowsResponse, Void> {

  private final ReadRowsRequestManager requestManager;
  private final StreamObserver<ReadRowsResponse> responseObserver;

  public ScannerRetryListener(
      StreamObserver<ReadRowsResponse> responseObserver,
      RetryOptions retryOptions,
      ReadRowsRequest request,
      BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> retryableRpc,
      CallOptions callOptions,
      ScheduledExecutorService retryExecutorService,
      Metadata originalMetadata) {
    super(retryOptions, request, retryableRpc, callOptions, retryExecutorService, originalMetadata);
    this.responseObserver = responseObserver;
    this.requestManager = new ReadRowsRequestManager(request);
  }

  /**
   * The stream observer handles responses.  Return null here, since a Future is not needed.
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

  /** {@inheritDoc} */
  @Override
  public void onMessage(ReadRowsResponse message) {
    // Get the next request
    call.request(1);
    resetBackoff();
    ByteString lastFoundKey = getLasFoundKey(message);
    updateLastFoundKey(lastFoundKey);
    responseObserver.onNext(message);
  }

  public void updateLastFoundKey(ByteString lastFoundKey) {
    if (!lastFoundKey.isEmpty()) {
      // Some messages may not contain a last found key, if they contain a partial
      requestManager.updateLastFoundKey(lastFoundKey);
    }
  }

  /**
   * Gets either the last rowkey from the message chunks, or alternatively gets the {@
   * @param message
   * @return
   */
  private ByteString getLasFoundKey(ReadRowsResponse message) {
    boolean commitFound = false;
    for (int i = message.getChunksCount() - 1; i >= 0; i--) {
      CellChunk chunk = message.getChunks(i);
      if (chunk.getCommitRow()) {
        commitFound = true;
      }
      if (commitFound && !chunk.getRowKey().isEmpty()) {
        return chunk.getRowKey();
      }
    }
    return message.getLastScannedRowKey();
  }

  @Override
  public void setException(Exception exception) {
    responseObserver.onError(exception);
  }

  @Override
  protected boolean isRequestRetryable() {
    return true;
  }

  @Override
  protected void onOK() {
    responseObserver.onCompleted();
  }

  void resetBackoff() {
    this.currentBackoff = null;
  }
}
