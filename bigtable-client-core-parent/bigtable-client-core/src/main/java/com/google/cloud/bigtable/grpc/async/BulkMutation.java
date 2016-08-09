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

import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.common.annotations.VisibleForTesting;
import com.google.api.client.util.BackOff;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.MutateRowsResponse.Entry;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Any;
import com.google.rpc.Status;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class combines a collection of {@link com.google.bigtable.v2.MutateRowRequest}s into a single
 * {@link com.google.bigtable.v2.MutateRowsRequest}. This class is not thread safe, and requires calling classes to make it
 * thread safe.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BulkMutation {

  private final static StatusRuntimeException MISSING_ENTRY_EXCEPTION =
      io.grpc.Status.UNKNOWN
          .withDescription("Mutation does not have a status")
          .asRuntimeException();
  /** Constant <code>LOG</code> */
  protected final static Logger LOG = new Logger(BulkMutation.class);

  private static StatusRuntimeException toException(Status status) {
    io.grpc.Status grpcStatus = io.grpc.Status
        .fromCodeValue(status.getCode())
        .withDescription(status.getMessage());
    for (Any detail : status.getDetailsList()) {
      grpcStatus = grpcStatus.augmentDescription(detail.toString());
    }
    return grpcStatus.asRuntimeException();
  }

  @VisibleForTesting
  static MutateRowsRequest.Entry convert(MutateRowRequest request) {
    return MutateRowsRequest.Entry.newBuilder()
            .setRowKey(request.getRowKey())
            .addAllMutations(request.getMutationsList())
            .build();
  }

  @VisibleForTesting
  static class RequestManager {
    private final List<SettableFuture<MutateRowResponse>> futures = new ArrayList<>();
    private final MutateRowsRequest.Builder builder;
    private MutateRowsRequest request;
    private long approximateByteSize = 0l;
    private Meter addMeter;

    RequestManager(String tableName, Meter addMeter) {
      this.builder = MutateRowsRequest.newBuilder().setTableName(tableName);
      this.approximateByteSize = tableName.length() + 2;
      this.addMeter = addMeter;
    }

    void add(SettableFuture<MutateRowResponse> future, MutateRowsRequest.Entry entry) {
      addMeter.mark();
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

    private final Meter mutationMeter =
        BigtableClientMetrics.meter(MetricLevel.Info, "BulkMutations.mutation.added.meter");
    private final Meter mutationRetryMeter =
        BigtableClientMetrics.meter(MetricLevel.Info, "BulkMutations.mutation.retry.meter");

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
      this.currentRequestManager = new RequestManager(tableName, mutationMeter);
      this.asyncExecutor = asyncExecutor;
      this.retryOptions = retryOptions;
      this.retryExecutorService = retryExecutorService;
      this.maxRowKeyCount = maxRowKeyCount;
      this.maxRequestSize = maxRequestSize;
    }

    /**
     * Adds a {@link MutateRowRequest} to the
     * {@link com.google.bigtable.v2.MutateRowsRequest.Builder}. NOTE: Users have to make sure that
     * this gets called in a thread safe way.
     * @param request The {@link MutateRowRequest} to add
     * @return a {@link SettableFuture} that will be populated when the {@link MutateRowsResponse}
     *         returns from the server. See {@link #addCallback(ListenableFuture)} for
     *         more information about how the SettableFuture is set.
     */
    @VisibleForTesting
    ListenableFuture<MutateRowResponse> add(MutateRowRequest request) {
      SettableFuture<MutateRowResponse> future = SettableFuture.create();
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
    void addCallback(ListenableFuture<List<MutateRowsResponse>> bulkFuture) {
      Futures.addCallback(
          bulkFuture,
          new FutureCallback<List<MutateRowsResponse>>() {
            @Override
            public void onSuccess(List<MutateRowsResponse> result) {
              handleResult(result);
            }

            @Override
            public void onFailure(Throwable t) {
              performFullRetry(new AtomicReference<Long>(), t);
            }
          });
    }

    synchronized void handleResult(List<MutateRowsResponse> results) {
      if (this.currentRequestManager == null) {
        LOG.warn("Got duplicate responses for bulk mutation.");
        return;
      }

      if (results == null || results.isEmpty()) {
        performFullRetry(new IllegalStateException("No MutateRowResponses were found."));
        return;
      }

      List<MutateRowsResponse.Entry> entries = new ArrayList<>();
      for (MutateRowsResponse response : results) {
        entries.addAll(response.getEntriesList());
      }

      if (entries.isEmpty()) {
        performFullRetry(new IllegalStateException("No MutateRowsResponses entries were found."));
        return;
      }

      AtomicReference<Long> backoffTime = new AtomicReference<>();

      try {
        String tableName = currentRequestManager.request.getTableName();
        RequestManager retryRequestManager = new RequestManager(tableName, mutationRetryMeter);

        handleResponses(backoffTime, entries, retryRequestManager);
        handleExtraFutures(backoffTime, retryRequestManager, entries);
        completeOrRetry(backoffTime, retryRequestManager);
      } catch (RuntimeException e) {
        LOG.error(
          "Unexpected Exception occurred. Treating this issue as a temporary issue and retrying.",
          e);
        performFullRetry(backoffTime, e);
      }
    }

    private void performFullRetry(Throwable t) {
      performFullRetry(new AtomicReference<Long>(), t);
    }

    private void performFullRetry(AtomicReference<Long> backoff, Throwable t) {
      Long backoffMs = getCurrentBackoff(backoff);
      if (backoffMs == BackOff.STOP) {
        setFailure(new BigtableRetriesExhaustedException("Exhausted retries.", t));
      } else {
        LOG.info("Retrying failed call. Failure #%d, got: %s", t, failedCount++,
          io.grpc.Status.fromThrowable(t));
        mutationRetryMeter.mark(currentRequestManager.futures.size());
        retryExecutorService.schedule(this, backoffMs, TimeUnit.MILLISECONDS);
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

    private void handleResponses(AtomicReference<Long> backoffTime,
        Iterable<MutateRowsResponse.Entry> entries, RequestManager retryRequestManager) {
      for (MutateRowsResponse.Entry entry : entries) {
        int index = (int) entry.getIndex();
        if (index >= currentRequestManager.futures.size()) {
          LOG.error("Got extra status: %s", entry);
          break;
        }

        SettableFuture<MutateRowResponse> future = currentRequestManager.futures.get(index);

        Status status = entry.getStatus();
        int statusCode = status.getCode();
        if (statusCode == io.grpc.Status.Code.OK.value()) {
          future.set(MutateRowResponse.getDefaultInstance());
        } else if (!isRetryable(statusCode) || getCurrentBackoff(backoffTime) == BackOff.STOP) {
          future.setException(toException(status));
        } else {
          retryRequestManager.add(future, currentRequestManager.request.getEntries(index));
        }
      }
    }

    private void handleExtraFutures(AtomicReference<Long> backoffTime, RequestManager retryRequestManager,
        List<Entry> entries) {
      Set<Integer> indexes = new HashSet<>();
      indexes.addAll(getIndexes(entries));
      long missingEntriesCount = 0;
      getCurrentBackoff(backoffTime);
      for (int i = 0; i < currentRequestManager.futures.size(); i++) {
        // If the indexes do not contain this future, then there's a problem.
        if (!indexes.remove(i)) {
          missingEntriesCount++;
          if (backoffTime.get() == BackOff.STOP) {
            currentRequestManager.futures.get(i).setException(MISSING_ENTRY_EXCEPTION);
          } else {
            retryRequestManager.add(currentRequestManager.futures.get(i),
              this.currentRequestManager.request.getEntries(i));
          }
        }
      }
      if (missingEntriesCount > 0) {
        String handling =
            backoffTime.get() == BackOff.STOP ? "Setting exceptions on the futures" : "Retrying";
        LOG.error("Missing %d responses for bulkWrite. %s.", missingEntriesCount, handling);
      }
    }

    private List<Integer> getIndexes(List<Entry> entries) {
      List<Integer> indexes = new ArrayList<>(entries.size());
      for (Entry entry : entries) {
        indexes.add((int) entry.getIndex());
      }
      return indexes;
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

    protected boolean isRetryable(int codeId) {
      Code code = io.grpc.Status.fromCodeValue(codeId).getCode();
      return retryOptions.isRetryable(code);
    }

    @Override
    public synchronized void run() {
      ListenableFuture<List<MutateRowsResponse>> future = null;
      try {
        if (retryId == null) {
          retryId = Long.valueOf(this.asyncExecutor.getRpcThrottler().registerRetry());
        }
        MutateRowsRequest request = currentRequestManager.build();
        future = asyncExecutor.mutateRowsAsync(request);
      } catch (InterruptedException e) {
        future = Futures.<List<MutateRowsResponse>> immediateFailedFuture(e);
      } finally {
        addCallback(future);
      }
    }

    /**
     * This would have happened after all retries are exhausted on the MutateRowsRequest. Don't
     * retry individual mutations.
     */
    private void setFailure(Throwable t) {
      try {
        for (SettableFuture<MutateRowResponse> future : currentRequestManager.futures) {
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
  private final Meter batchMeter =
      BigtableClientMetrics.meter(MetricLevel.Info, "BulkMutations.batch.meter");

  /**
   * <p>Constructor for BulkMutation.</p>
   *
   * @param tableName a {@link com.google.cloud.bigtable.grpc.BigtableTableName} object.
   * @param asyncExecutor a {@link com.google.cloud.bigtable.grpc.async.AsyncExecutor} object.
   * @param retryOptions a {@link com.google.cloud.bigtable.config.RetryOptions} object.
   * @param retryExecutorService a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @param maxRowKeyCount a int.
   * @param maxRequestSize a long.
   */
  public BulkMutation(
      BigtableTableName tableName,
      AsyncExecutor asyncExecutor,
      RetryOptions retryOptions,
      ScheduledExecutorService retryExecutorService,
      int maxRowKeyCount,
      long maxRequestSize) {
    this.tableName = tableName.toString();
    this.asyncExecutor = asyncExecutor;
    this.retryOptions = retryOptions;
    this.retryExecutorService = retryExecutorService;
    this.maxRowKeyCount = maxRowKeyCount;
    this.maxRequestSize = maxRequestSize;
  }

  /**
   * Adds a {@link com.google.bigtable.v2.MutateRowRequest} to the
   * {@link com.google.bigtable.v2.MutateRowsRequest.Builder}. NOTE: Users have to make sure that
   * this gets called in a thread safe way.
   *
   * @param request The {@link com.google.bigtable.v2.MutateRowRequest} to add
   * @return a {@link com.google.common.util.concurrent.SettableFuture} that will be populated when the {@link com.google.bigtable.v2.MutateRowsResponse}
   *         returns from the server. See {@link com.google.cloud.bigtable.grpc.async.BulkMutation.Batch#addCallback(ListenableFuture)} for
   *         more information about how the SettableFuture is set.
   */
  public synchronized ListenableFuture<MutateRowResponse> add(MutateRowRequest request) {
    if (currentBatch == null) {
      batchMeter.mark();
      currentBatch =
          new Batch(
              tableName,
              asyncExecutor,
              retryOptions,
              retryExecutorService,
              maxRowKeyCount,
              maxRequestSize);
    }

    ListenableFuture<MutateRowResponse> future = currentBatch.add(request);
    if (currentBatch.isFull()) {
      currentBatch.run();
      currentBatch = null;
    }
    return future;
  }

  /**
   * <p>flush.</p>
   */
  public synchronized void flush() {
    if (currentBatch != null) {
      currentBatch.run();
      currentBatch = null;
    }
  }

  /**
   * <p>isFlushed.</p>
   *
   * @return a boolean.
   */
  public synchronized boolean isFlushed() {
    return currentBatch == null;
  }
}
