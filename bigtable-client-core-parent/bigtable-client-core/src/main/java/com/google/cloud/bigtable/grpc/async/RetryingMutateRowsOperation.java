/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.google.api.client.util.BackOff;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.async.MutateRowsRequestManager.ProcessingStatus;
import com.google.common.collect.ImmutableMap;

import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.opencensus.trace.AttributeValue;

/**
 * Performs retries for {@link BigtableDataClient#mutateRows(MutateRowsRequest)} operations.
 */
public class RetryingMutateRowsOperation extends
    AbstractRetryingOperation<MutateRowsRequest, MutateRowsResponse, List<MutateRowsResponse>> {
  private final static io.grpc.Status INVALID_RESPONSE =
      io.grpc.Status.INTERNAL.withDescription("The server returned an invalid response");

  private final MutateRowsRequestManager requestManager;

  public RetryingMutateRowsOperation(RetryOptions retryOptions, MutateRowsRequest originalRquest,
      BigtableAsyncRpc<MutateRowsRequest, MutateRowsResponse> retryableRpc, CallOptions callOptions,
      ScheduledExecutorService retryExecutorService, Metadata originalMetadata) {
    super(retryOptions, originalRquest, retryableRpc, callOptions, retryExecutorService, originalMetadata);
    requestManager = new MutateRowsRequestManager(retryOptions, originalRquest);
    operationSpan.addAnnotation("MutationCount", ImmutableMap.of("count",
      AttributeValue.longAttributeValue(originalRquest.getEntriesCount())));
  }

  /**
   * Adds the content of the message to the {@link #results}.
   */
  @Override
  public void onMessage(MutateRowsResponse message) {
    requestManager.onMessage(message);
  }

  @Override
  protected MutateRowsRequest getRetryRequest() {
    return requestManager.getRetryRequest();
  }

  @Override
  protected boolean onOK(Metadata trailers) {
    ProcessingStatus status = requestManager.onOK();

    if (status == ProcessingStatus.INVALID) {
      // Set an exception.
      onError(INVALID_RESPONSE, trailers);
      return true;
    }

    // There was a problem in the data found in onMessage(), so fail the RPC.
    if (status == ProcessingStatus.SUCCESS || status == ProcessingStatus.NOT_RETRYABLE) {
      // Set the response, with either success, or non-retryable responses.
      completionFuture.set(Arrays.asList(requestManager.buildResponse()));
      return true;
    }

    // Perform a partial retry, if the backoff policy allows it.
    long nextBackOff = getNextBackoff();
    if (nextBackOff == BackOff.STOP) {
      // Return the response as is, and don't retry;
      rpc.getRpcMetrics().markRetriesExhasted();
      completionFuture.set(Arrays.asList(requestManager.buildResponse()));
      operationSpan.addAnnotation("MutationCount", ImmutableMap.of("failureCount",
        AttributeValue.longAttributeValue(requestManager.getRetryRequest().getEntriesCount())));
      return true;
    }

    performRetry(nextBackOff);
    operationSpan.addAnnotation("MutationCount", ImmutableMap.of("retryCount",
      AttributeValue.longAttributeValue(requestManager.getRetryRequest().getEntriesCount())));
    return false;
  }

}
