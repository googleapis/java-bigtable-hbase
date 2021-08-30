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

import com.google.api.core.ApiClock;
import com.google.api.core.InternalApi;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.DeadlineGenerator;
import com.google.cloud.bigtable.grpc.async.MutateRowsRequestManager.ProcessingStatus;
import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import io.opencensus.trace.AttributeValue;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Performs retries for {@link BigtableDataClient#mutateRows(MutateRowsRequest)} operations.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class RetryingMutateRowsOperation
    extends AbstractRetryingOperation<
        MutateRowsRequest, MutateRowsResponse, List<MutateRowsResponse>> {
  protected static final Logger LOG = new Logger(RetryingMutateRowsOperation.class);

  private static final io.grpc.Status INVALID_RESPONSE =
      io.grpc.Status.INTERNAL.withDescription("The server returned an invalid response");

  private final MutateRowsRequestManager requestManager;

  public RetryingMutateRowsOperation(
      RetryOptions retryOptions,
      MutateRowsRequest originalRequest,
      BigtableAsyncRpc<MutateRowsRequest, MutateRowsResponse> retryableRpc,
      DeadlineGenerator deadlineGenerator,
      ScheduledExecutorService retryExecutorService,
      Metadata originalMetadata,
      ApiClock clock) {
    super(
        retryOptions,
        originalRequest,
        retryableRpc,
        deadlineGenerator,
        retryExecutorService,
        originalMetadata,
        clock);
    requestManager = new MutateRowsRequestManager(retryOptions, originalRequest);
    operationSpan.addAnnotation(
        "MutationCount",
        ImmutableMap.of(
            "count", AttributeValue.longAttributeValue(originalRequest.getEntriesCount())));
  }

  @Override
  public void onMessage(MutateRowsResponse message) {
    try {
      requestManager.onMessage(message);
    } catch (Exception e) {
      setException(e);
    }
  }

  @Override
  protected MutateRowsRequest getRetryRequest() {
    return requestManager.getRetryRequest();
  }

  @Override
  protected boolean onOK(Metadata trailers) {
    ProcessingStatus status = requestManager.onOK();

    if (status == ProcessingStatus.INVALID) {
      LOG.error("BulkMutateRows was invalid, final state: " + requestManager.getResultString());

      // Set an exception.
      onError(INVALID_RESPONSE, trailers);
      return true;
    }

    // There was a problem in the data found in onMessage(), so fail the RPC.
    if (status == ProcessingStatus.SUCCESS || status == ProcessingStatus.NOT_RETRYABLE) {
      // Set the response, with either success, or non-retryable responses.
      completionFuture.set(Arrays.asList(requestManager.buildResponse()));

      if (status != ProcessingStatus.SUCCESS) {
        LOG.error(
            "BulkMutateRows partially failed with nonretryable errors, final state: "
                + requestManager.getResultString());
      }
      return true;
    }

    // Perform a partial retry, if the backoff policy allows it.
    Long nextBackOff = getNextBackoff();
    if (nextBackOff == null) {
      // Return the response as is, and don't retry;
      rpc.getRpcMetrics().markRetriesExhasted();
      completionFuture.set(Arrays.asList(requestManager.buildResponse()));
      operationSpan.addAnnotation(
          "MutationCount",
          ImmutableMap.of(
              "failureCount",
              AttributeValue.longAttributeValue(
                  requestManager.getRetryRequest().getEntriesCount())));

      LOG.error(
          "BulkMutateRows exhausted retries, final state: " + requestManager.getResultString());
      return true;
    }

    performRetry(nextBackOff);
    operationSpan.addAnnotation(
        "MutationCount",
        ImmutableMap.of(
            "retryCount",
            AttributeValue.longAttributeValue(requestManager.getRetryRequest().getEntriesCount())));
    return false;
  }
}
