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

import com.google.api.core.InternalApi;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.MutateRowsResponse.Entry;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.base.MoreObjects;
import com.google.rpc.Status;
import io.grpc.Status.Code;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Performs retries for {@link BigtableDataClient#mutateRows(MutateRowsRequest)} operations.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class MutateRowsRequestManager {
  private static final Status STATUS_INTERNAL =
      Status.newBuilder()
          .setCode(io.grpc.Status.Code.INTERNAL.value())
          .setMessage("Response was not returned for this index.")
          .build();

  private static Code getGrpcCode(Status status) {
    return status == null ? null : io.grpc.Status.fromCodeValue(status.getCode()).getCode();
  }

  private static Entry createEntry(int i, Status status) {
    return MutateRowsResponse.Entry.newBuilder().setIndex(i).setStatus(status).build();
  }

  public enum ProcessingStatus {
    /* All responses produced OK */
    SUCCESS,
    /* All responses produced OK or a retryable code */
    RETRYABLE,
    /* Some responses a non-retryable code */
    NOT_RETRYABLE,
    /* The response was invalid - missing indexes and etc. */
    INVALID;
  }

  /**
   * The current request to send. This starts as the original request. If retries occur, this
   * request will contain the subset of Mutations that need to be retried.
   */
  private volatile MutateRowsRequest currentRequest;

  /**
   * When doing retries, the retry sends a partial set of the original mutations that failed with a
   * retryable status. This array contains a mapping of indices from the {@link #currentRequest} to
   * {@link #originalRequest}.
   */
  private int[] mapToOriginalIndex;

  /** This array tracks the cumulative set of results across all RPC requests. */
  private final Status[] results;

  private final RetryOptions retryOptions;
  private final MutateRowsRequest originalRequest;

  private boolean messageIsInvalid = false;

  public MutateRowsRequestManager(RetryOptions retryOptions, MutateRowsRequest originalRequest) {
    this.currentRequest = originalRequest;
    this.originalRequest = originalRequest;
    this.retryOptions = retryOptions;
    results = new Status[originalRequest.getEntriesCount()];

    // This map should is a map between currentRequest and originalRequest. For now, currentRequest
    // == originalRequest, but they could diverge if a retry occurs.
    mapToOriginalIndex = new int[originalRequest.getEntriesCount()];
    for (int i = 0; i < mapToOriginalIndex.length; i++) {
      mapToOriginalIndex[i] = i;
    }
  }

  /** Adds the content of the message to the {@link #results}. */
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

  /**
   * This is called when all calls to {@link #onMessage(MutateRowsResponse)} are complete.
   *
   * @return {@link ProcessingStatus} of the accumulated responses - success, invalid, retrable,
   *     non-retryable.
   */
  public ProcessingStatus onOK() {
    // Sanity check to make sure that every mutation received a response.
    if (!messageIsInvalid) {
      for (int i = 0; i < results.length; i++) {
        if (results[i] == null) {
          messageIsInvalid = true;
          break;
        }
      }
    }

    // There was a problem in the data found in onMessage(), so fail the RPC.
    if (messageIsInvalid) {
      return ProcessingStatus.INVALID;
    }

    List<Integer> toRetry = new ArrayList<>();
    ProcessingStatus processingStatus = ProcessingStatus.SUCCESS;

    // Check the current state to determine the state of the results.
    // There are three states: OK, Fail, or Partial Retry.
    for (int i = 0; i < results.length; i++) {
      Status status = results[i];
      if (status.getCode() == io.grpc.Status.Code.OK.value()) {
        continue;
      } else if (retryOptions.isRetryable(getGrpcCode(status))) {
        // An individual mutation failed with a retryable code, usually DEADLINE_EXCEEDED.
        toRetry.add(i);
        if (processingStatus == ProcessingStatus.SUCCESS) {
          processingStatus = ProcessingStatus.RETRYABLE;
        }
      } else {
        // Don't retry if even a single response is not retryable.
        processingStatus = ProcessingStatus.NOT_RETRYABLE;
        break;
      }
    }

    if (!toRetry.isEmpty()) {
      currentRequest = createRetryRequest(toRetry);
    }

    return processingStatus;
  }

  public MutateRowsRequest getRetryRequest() {
    return currentRequest;
  }

  /**
   * Creates a new {@link MutateRowsRequest} that's a subset of the original request that
   * corresponds to a set of indices.
   *
   * @param indicesToRetry
   * @return the new {@link MutateRowsRequest}.
   */
  private MutateRowsRequest createRetryRequest(List<Integer> indicesToRetry) {
    MutateRowsRequest.Builder updatedRequest =
        MutateRowsRequest.newBuilder().setTableName(originalRequest.getTableName());
    mapToOriginalIndex = new int[indicesToRetry.size()];
    for (int i = 0; i < indicesToRetry.size(); i++) {
      mapToOriginalIndex[i] = indicesToRetry.get(i);
      updatedRequest.addEntries(originalRequest.getEntries(indicesToRetry.get(i)));
    }
    return updatedRequest.build();
  }

  /**
   * @return a {@link MutateRowsResponse} built from the accumulation of all calls to
   *     onMessage/onOK.
   */
  public MutateRowsResponse buildResponse() {
    List<MutateRowsResponse.Entry> entries = new ArrayList<>();
    for (int i = 0; i < results.length; i++) {
      Status status = (results[i] == null) ? STATUS_INTERNAL : results[i];
      entries.add(createEntry(i, status));
    }
    return MutateRowsResponse.newBuilder().addAllEntries(entries).build();
  }

  public String getResultString() {
    // 2D map: code -> msg -> count
    Map<Integer, Map<String, Long>> resultCounts = new TreeMap<>();

    for (Status result : results) {
      if (result == null) {
        result = STATUS_INTERNAL;
      }
      Map<String, Long> msgCounts = resultCounts.get(result.getCode());
      if (msgCounts == null) {
        msgCounts = new TreeMap<>();
        resultCounts.put(result.getCode(), msgCounts);
      }

      String msg = MoreObjects.firstNonNull(result.getMessage(), "");
      Long count = MoreObjects.firstNonNull(msgCounts.get(msg), 0L);
      msgCounts.put(msg, count + 1);
    }

    // Format string as: code: msg(count), msg2(count); code2: msg(count);
    StringBuilder buffer = new StringBuilder();
    boolean isFirstCode = true;
    for (Map.Entry<Integer, Map<String, Long>> codeEntry : resultCounts.entrySet()) {
      if (!isFirstCode) {
        buffer.append("; ");
      } else {
        isFirstCode = false;
      }

      buffer.append(io.grpc.Status.fromCodeValue(codeEntry.getKey()).getCode());
      buffer.append(": ");

      boolean isFirstMsg = true;
      for (Map.Entry<String, Long> msgEntry : codeEntry.getValue().entrySet()) {
        if (!isFirstMsg) {
          buffer.append(", ");
        } else {
          isFirstMsg = false;
        }
        buffer.append(msgEntry.getKey());
        buffer.append("(" + msgEntry.getValue() + ")");
      }
    }

    return buffer.toString();
  }
}
