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

import com.google.common.annotations.VisibleForTesting;
import com.google.api.client.util.BackOff;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.MutateRowsRequest;
import com.google.bigtable.v1.MutateRowsRequest.Entry;
import com.google.bigtable.v1.MutateRowsResponse;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.rpc.Status;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
 import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class combines a collection of {@link MutateRowRequest}s into a single
 * {@link MutateRowsRequest}. This class is not thread safe, and requires calling classes to make it
 * thread safe.
 */
public class BulkMutation {

  private final static StatusRuntimeException MISSING_ENTRY_EXCEPTION =
      io.grpc.Status.UNKNOWN
          .withDescription("Mutation does not have a status")
          .asRuntimeException();
  protected final static Logger LOG = new Logger(BulkMutation.class);

  private static StatusRuntimeException toException(Status status) {
    io.grpc.Status grpcStatus = io.grpc.Status
        .fromCodeValue(status.getCode())
        .withDescription(status.getMessage());
    for (Any detail : status.getDetailsList()) {
      grpcStatus.augmentDescription(detail.toString());
    }
    return grpcStatus.asRuntimeException();
  }

  @VisibleForTesting
  static Entry convert(MutateRowRequest request) {
    return MutateRowsRequest.Entry.newBuilder()
            .setRowKey(request.getRowKey())
            .addAllMutations(request.getMutationsList())
            .build();
  }

  @VisibleForTesting
  static class RequestManager {
    private final List<SettableFuture<Empty>> futures = new ArrayList<>();
    private final MutateRowsRequest.Builder builder;
    private MutateRowsRequest request;
    private long approximateByteSize = 0l;

    RequestManager(String tableName) {
      this.builder = MutateRowsRequest.newBuilder().setTableName(tableName);
      this.approximateByteSize = tableName.length() + 2;
    }

    void add(SettableFuture<Empty> future, MutateRowsRequest.Entry entry) {
      futures.add(future);
      builder.addEntries(entry);
      approximateByteSize += entry.getSerializedSize();
    }

    MutateRowsRequest build() {
      request = builder.build();
      return request;
    }
  }

  @VisibleForTesting
  static class Batch implements Runnable {
    private final AsyncExecutor asyncExecutor;
    private final RetryOptions retryOptions;
    private final ScheduledExecutorService retryExecutorService;
    private final int maxRowKeyCount;
    private final long maxRequestSize;

    private RequestManager currentRequestManager;
    private Long retryId;
    private BackOff currentBackoff;
    private int failedCount;

    Batch(
        String tableName,
        AsyncExecutor asyncExecutor,
        RetryOptions retryOptions,
        ScheduledExecutorService retryExecutorService,
        int maxRowKeyCount,
        long maxRequestSize) {
      this.currentRequestManager = new RequestManager(tableName);
      this.asyncExecutor = asyncExecutor;
      this.retryOptions = retryOptions;
      this.retryExecutorService = retryExecutorService;
      this.maxRowKeyCount = maxRowKeyCount;
      this.maxRequestSize = maxRequestSize;
    }

    /**
     * Adds a {@link MutateRowRequest} to the
     * {@link com.google.bigtable.v1.MutateRowsRequest.Builder}. NOTE: Users have to make sure that
     * this gets called in a thread safe way.
     * @param request The {@link MutateRowRequest} to add
     * @return a {@link SettableFuture} that will be populated when the {@link MutateRowsResponse}
     *         returns from the server. See {@link #addCallback(ListenableFuture)} for
     *         more information about how the SettableFuture is set.
     */
    ListenableFuture<Empty> add(MutateRowRequest request) {
      SettableFuture<Empty> future = SettableFuture.create();
      currentRequestManager.add(future, convert(request));
      return future;
    }

    boolean isFull() {
      return getRequestCount() >= maxRowKeyCount
          || currentRequestManager.approximateByteSize >= maxRequestSize;
    }

    /**
     * Adds a {@link FutureCallback} that will update all of the SettableFutures created by
     * {@link BulkMutation#add(MutateRowRequest)} when the provided {@link ListenableFuture} for the
     * {@link MutateRowsResponse} is complete.
     */
    void addCallback(ListenableFuture<MutateRowsResponse> bulkFuture) {
      Futures.addCallback(
          bulkFuture,
          new FutureCallback<MutateRowsResponse>() {
            @Override
            public void onSuccess(MutateRowsResponse result) {
              handleResult(result);
            }

            @Override
            public void onFailure(Throwable t) {
              perfromFullRetry(new AtomicReference<Long>(), t);
            }
          });
    }


    synchronized void handleResult(MutateRowsResponse result) {
      AtomicReference<Long> backoffTime = new AtomicReference<>();
      if (this.currentRequestManager == null) {
        LOG.warn("Got duplicate responses for bulk mutation.");
        return;
      }

      if (result == null
          || result.getStatusesList() == null
          || result.getStatusesList().isEmpty()) {
        perfromFullRetry(backoffTime, new IllegalStateException("empty result or statuses"));
        return;
      }

      try {
        Iterator<Status> statuses = result.getStatusesList().iterator();
        Iterator<SettableFuture<Empty>> futures = currentRequestManager.futures.iterator();

        String tableName = currentRequestManager.request.getTableName();
        RequestManager retryRequestManager = new RequestManager(tableName);

        int processedCount = handleResponses(backoffTime, statuses, futures, retryRequestManager);
        handleExtraFutures(backoffTime, futures, processedCount, retryRequestManager);
        handleExtraStatuses(statuses);
        completeOrRetry(backoffTime, retryRequestManager);
      } catch (RuntimeException e) {
        LOG.error(
          "Unexpected Exception occurred. Treating this issue as a temporary issue and retrying.",
          e);
        perfromFullRetry(backoffTime, e);
      }
    }

    private void perfromFullRetry(AtomicReference<Long> backOffTime, Throwable t) {
      getCurrentBackoff(backOffTime);
      if (backOffTime.get() == BackOff.STOP) {
        setFailure(new BigtableRetriesExhaustedException("Exhausted retries.", t));
      } else {
        LOG.info("Retrying failed call. Failure #%d, got: %s", t, failedCount++,
          io.grpc.Status.fromThrowable(t));
        retryExecutorService.schedule(this, backOffTime.get(), TimeUnit.MILLISECONDS);
      }
    }

    private Long getCurrentBackoff(AtomicReference<Long> backOffTime) {
      if (backOffTime.get() == null) {
        try {
          if(this.currentBackoff == null) {
            this.currentBackoff = retryOptions.createBackoff();
          }
          backOffTime.set(this.currentBackoff.nextBackOffMillis());
        } catch (IOException e) {
          // Something unusually bad happened in getting the nextBackOff. The Exponential backoff
          // doesn't throw an exception. A different backoff was used that does I/O. Just stop in this
          // case.
          LOG.warn("Could not get the next backoff.", e);
          backOffTime.set(BackOff.STOP);
        }
      }
      return backOffTime.get();
    }

    private int handleResponses(AtomicReference<Long> backoffTime, Iterator<Status> statuses,
        Iterator<SettableFuture<Empty>> futures, RequestManager retryRequestManager) {
      int count = 0;
      while (futures.hasNext() && statuses.hasNext()) {
        SettableFuture<Empty> future = futures.next();
        Status status = statuses.next();
        if (status.getCode() == io.grpc.Status.Code.OK.value()) {
          future.set(Empty.getDefaultInstance());
        } else if (!isRetryable(status) || getCurrentBackoff(backoffTime) == BackOff.STOP) {
          future.setException(toException(status));
        } else {
          retryRequestManager.add(future, this.currentRequestManager.request.getEntries(count));
        }
        count++;
      }
      return count;
    }

    private void handleExtraFutures(AtomicReference<Long> backoffTime, Iterator<SettableFuture<Empty>> futures,
        int processedCount, RequestManager retryRequestManager) {
      if (!futures.hasNext()) {
        return;
      }
      long missingEntriesCount = 0;
      getCurrentBackoff(backoffTime);
      while (futures.hasNext()) {
        if (backoffTime.get() == BackOff.STOP) {
          futures.next().setException(MISSING_ENTRY_EXCEPTION);
        } else {
          retryRequestManager.add(futures.next(),
            this.currentRequestManager.request.getEntries(processedCount));
        }
        processedCount++;
        missingEntriesCount++;
      }
      String handling =
          backoffTime.get() == BackOff.STOP ? "Setting exceptions on the futures" : "Retrying";
      LOG.error("Missing %d responses for bulkWrite. %s.", missingEntriesCount, handling);
    }

    private void handleExtraStatuses(Iterator<Status> statuses) {
      if (!statuses.hasNext()) {
        return;
      }
      int extraStatusCount = 0;
      while(statuses.hasNext()) {
        extraStatusCount++;
        LOG.error("Got %extra status: %s", statuses.next());
      }
      LOG.error("Got %d extra statuses", extraStatusCount);
    }

    private void completeOrRetry(AtomicReference<Long> backoffTime, RequestManager retryRequestManager) {
      if (retryRequestManager == null || retryRequestManager.futures.isEmpty()) {
        this.currentRequestManager = null;
        setRetryComplete();
      } else {
        this.currentRequestManager = retryRequestManager;
        LOG.info(
          "Retrying failed call. Failure #%d, got #%d failures",
          failedCount++,
          currentRequestManager.futures.size());
        retryExecutorService.schedule(this, getCurrentBackoff(backoffTime), TimeUnit.MILLISECONDS);
      }
    }

    private boolean isRetryable(Status status) {
      int codeId = status.getCode();
      Code code = io.grpc.Status.fromCodeValue(codeId).getCode();
      return retryOptions.isRetryable(code);
    }

    @Override
    public synchronized void run() {
      ListenableFuture<MutateRowsResponse> future = null;
      try {
        if (retryId == null) {
          retryId = Long.valueOf(this.asyncExecutor.getRpcThrottler().registerRetry());
        }
        future = asyncExecutor.mutateRowsAsync(currentRequestManager.build());
      } catch (InterruptedException e) {
        future = Futures.<MutateRowsResponse> immediateFailedFuture(e);
      } finally {
        addCallback(future);
      }
    }

    /**
     *       // This would have happened after all retries are exhausted on the MutateRowsRequest.
      // Don't retry individual mutations.
     * @param t
     */
    private void setFailure(Throwable t) {
      try {
        for (SettableFuture<Empty> future : currentRequestManager.futures) {
          future.setException(t);
        }
      } finally {
        setRetryComplete();
      }
    }

    private void setRetryComplete() {
      this.asyncExecutor.getRpcThrottler().onRetryCompletion(retryId);
    }

    @VisibleForTesting
   int getRequestCount() {
      return currentRequestManager == null ? 0 : currentRequestManager.futures.size();
    }
  }

  @VisibleForTesting
  Batch currentBatch = null;

  private final String tableName;
  private final AsyncExecutor asyncExecutor;
  private final RetryOptions retryOptions;
  private final ScheduledExecutorService retryExecutorService;
  private final int maxRowKeyCount;
  private final long maxRequestSize;

  public BulkMutation(
      String tableName,
      AsyncExecutor asyncExecutor,
      RetryOptions retryOptions,
      ScheduledExecutorService retryExecutorService,
      int maxRowKeyCount,
      long maxRequestSize) {
    this.tableName = tableName;
    this.asyncExecutor = asyncExecutor;
    this.retryOptions = retryOptions;
    this.retryExecutorService = retryExecutorService;
    this.maxRowKeyCount = maxRowKeyCount;
    this.maxRequestSize = maxRequestSize;
  }

  /**
   * Adds a {@link MutateRowRequest} to the
   * {@link com.google.bigtable.v1.MutateRowsRequest.Builder}. NOTE: Users have to make sure that
   * this gets called in a thread safe way.
   * @param request The {@link MutateRowRequest} to add
   * @return a {@link SettableFuture} that will be populated when the {@link MutateRowsResponse}
   *         returns from the server. See {@link Batch#addCallback(ListenableFuture)} for
   *         more information about how the SettableFuture is set.
   */
  public synchronized ListenableFuture<Empty> add(MutateRowRequest request) {
    if (currentBatch == null) {
      currentBatch =
          new Batch(
              tableName,
              asyncExecutor,
              retryOptions,
              retryExecutorService,
              maxRowKeyCount,
              maxRequestSize);
    }

    ListenableFuture<Empty> future = currentBatch.add(request);
    if (currentBatch.isFull()) {
      currentBatch.run();
      currentBatch = null;
    }
    return future;
  }

  public synchronized void flush() {
    if (currentBatch != null) {
      currentBatch.run();
      currentBatch = null;
    }
  }

  public synchronized boolean isFlushed() {
    return currentBatch == null;
  }
}