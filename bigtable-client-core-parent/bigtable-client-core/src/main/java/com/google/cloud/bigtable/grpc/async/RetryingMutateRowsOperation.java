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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.google.api.client.util.BackOff;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.MutateRowsResponse.Entry;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.collect.ImmutableMap;
import com.google.rpc.Status;

import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.Status.Code;
import io.opencensus.trace.AttributeValue;

/**
 * Performs retries for {@link BigtableDataClient#mutateRows(MutateRowsRequest)} operations.
 */
public class RetryingMutateRowsOperation extends
    AbstractRetryingOperation<MutateRowsRequest, MutateRowsResponse, List<MutateRowsResponse>> {
  private final static io.grpc.Status INVALID_RESPONSE =
      io.grpc.Status.INTERNAL.withDescription("The server returned an invalid response");
  private final static Status STATUS_INTERNAL =
      Status.newBuilder().setCode(io.grpc.Status.Code.INTERNAL.value()).build();

  private static Code getGrpcCode(Status status) {
    return status == null ? null : io.grpc.Status.fromCodeValue(status.getCode()).getCode();
  }

  private static Entry createEntry(int i, Status status) {
    return MutateRowsResponse.Entry.newBuilder().setIndex(i).setStatus(status).build();
  }


  /**
   * The current request to send. This starts as the original request. If retries occur, this
   * request will contain the subset of Mutations that need to be retried.
   */
  private MutateRowsRequest currentRequest;

  /**
   * When doing retries, the retry sends a partial set of the original mutations that failed with a
   * retryable status. This array contains a mapping of indices from the {@link #getRetryRequest()}
   * to {@link #getOriginalRequest()}.
   */
  private int[] mapToOriginalIndex;

  /**
   * This array tracks the cumulative set of results across all RPC requests.
   */
  private final Status[] results;
  private boolean messageIsInvalid;

  public RetryingMutateRowsOperation(RetryOptions retryOptions, MutateRowsRequest originalRquest,
      BigtableAsyncRpc<MutateRowsRequest, MutateRowsResponse> retryableRpc, CallOptions callOptions,
      ScheduledExecutorService retryExecutorService, Metadata originalMetadata) {
    super(retryOptions, originalRquest, retryableRpc, callOptions, retryExecutorService, originalMetadata);
    this.currentRequest = originalRquest;
    results = new Status[originalRquest.getEntriesCount()];

    // This map should is a map between currentRequest and originalRquest. For now, currentRequest
    // == originalRquest, but they could diverge if a retry occurs.
    mapToOriginalIndex = new int[originalRquest.getEntriesCount()];
    for (int i = 0; i < mapToOriginalIndex.length; i++) {
      mapToOriginalIndex[i] = i;
    }
    operationSpan.addAnnotation("MutationCount", ImmutableMap.of("count",
      AttributeValue.longAttributeValue(originalRquest.getEntriesCount())));
  }

  /**
   * Adds the content of the message to the {@link #results}.
   */
  @Override
  public void onMessage(MutateRowsResponse message) {
    for (Entry entry : message.getEntriesList()) {
      int index = (int) entry.getIndex();

      // Sanity check to make sure that the index returned from the server is valid.
      if (index >= mapToOriginalIndex.length || index < 0) {
        messageIsInvalid = true;
        break;
      }

      // Set the result.
      results[mapToOriginalIndex[index]] = entry.getStatus();
    }
  }

  @Override
  protected MutateRowsRequest getRetryRequest() {
    return currentRequest;
  }

  @Override
  protected boolean onOK(Metadata trailers) {
    // Sanity check to make sure that every mutation received a response.
    for (int i = 0; i < results.length; i++) {
      if (results[i] == null) {
        messageIsInvalid = true;
        break;
      }
    }

    // There was a problem in the data found in onMessage(), so fail the RPC.
    if (messageIsInvalid) {
      onError(INVALID_RESPONSE, trailers);
      return false;
    }

    boolean fail = false;
    List<Integer> toRetry = new ArrayList<>();

    // Check the current state to determine the state of the results.
    // There are three states: OK, Fail, or Partial Retry.
    for (int i = 0; i < results.length; i++) {
      Status status = results[i];
      if (status.getCode() == io.grpc.Status.Code.OK.value()) {
        continue;
      }

      if (retryOptions.isRetryable(getGrpcCode(status))) {
        // An individual mutation failed with a retryable code, usually DEADLINE_EXCEEDED.
        toRetry.add(i);
      } else {
        // Don't retry if even a single response is not retryable.
        fail = true;
      }
    }

    if (fail) {
      rpc.getRpcMetrics().markFailure();
    }

    if (!toRetry.isEmpty()) {
      operationSpan.addAnnotation("MutationCount", ImmutableMap.of("retryCount",
        AttributeValue.longAttributeValue(toRetry.size())));
    }

    // OK or Fail should set the future, and end the operation.
    // The caller should handle the processing of the failed mutations.
    if (toRetry.isEmpty() || fail) {
      completionFuture.set(Arrays.asList(buildResponse()));
      return true;
    }

    // Perform a partial retry, if the backoff policy allows it.
    long nextBackOff = getNextBackoff();
    if (nextBackOff == BackOff.STOP) {
      rpc.getRpcMetrics().markRetriesExhasted();
      completionFuture.set(Arrays.asList(buildResponse()));
      return true;
    } else {
      currentRequest = createRetryRequest(toRetry);
      mapToOriginalIndex = toIntArray(toRetry);
      performRetry(nextBackOff);
      return false;
    }
  }

  /**
   * Creates a new {@link MutateRowsRequest} that's a subset of the original request that
   * corresponds to a set of indices.
   *
   * @param indiciesToRetry
   * @return the new {@link MutateRowsRequest}.
   */
  private MutateRowsRequest createRetryRequest(List<Integer> indiciesToRetry) {
    MutateRowsRequest originalRequest = getOriginalRequest();
    MutateRowsRequest.Builder updatedRequest = MutateRowsRequest.newBuilder()
        .setTableName(originalRequest.getTableName());

    for (int i = 0; i < indiciesToRetry.size(); i++) {
      updatedRequest.addEntries(originalRequest.getEntries(indiciesToRetry.get(i)));
    }

    return updatedRequest.build();
  }

  /**
   * Converts the List<Integer> to int[].
   */
  private int[] toIntArray(List<Integer> list) {
    int[] array = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      array[i] = list.get(i);
    }
    return array;
  }

  private MutateRowsResponse buildResponse() {
    List<MutateRowsResponse.Entry> entries = new ArrayList<>();
    for (int i = 0; i < results.length; i++) {
      Status status = (results[i] == null) ? STATUS_INTERNAL : results[i];
      entries.add(createEntry(i, status));
    }
    return MutateRowsResponse.newBuilder().addAllEntries(entries).build();
  }
}
