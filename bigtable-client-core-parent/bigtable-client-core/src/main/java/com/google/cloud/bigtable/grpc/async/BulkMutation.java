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
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Clock;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.MutateRowsResponse.Entry;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.OperationAccountant.ComplexOperationStalenessHandler;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class combines a collection of {@link MutateRowRequest}s into a single {@link
 * MutateRowsRequest}. This class is not thread safe, and requires calling classes to make it thread
 * safe.
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

  public static final long MAX_RPC_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);

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
    if (request == null) {
      return null;
    } else {
      return MutateRowsRequest.Entry.newBuilder().setRowKey(request.getRowKey())
          .addAllMutations(request.getMutationsList()).build();
    }
  }

  @VisibleForTesting
  static Clock clock = Clock.SYSTEM;

  @VisibleForTesting
  static class RequestManager {
    private final List<SettableFuture<MutateRowResponse>> futures = new ArrayList<>();
    private final MutateRowsRequest.Builder builder;
    private final Meter addMeter;

    private MutateRowsRequest request;
    private long approximateByteSize = 0l;

    @VisibleForTesting
    Long lastRpcSentTime;

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

    public boolean isEmpty() {
      return futures.isEmpty();
    }

    public int getRequestCount() {
      return futures.size();
    }

    public boolean isStale() {
      return lastRpcSentTime != null
          && lastRpcSentTime < (clock.currentTimeMillis() - MAX_RPC_WAIT_TIME);
    }

    public boolean wasSent() {
      return lastRpcSentTime != null;
    }
  }

  @VisibleForTesting
  class Batch implements Runnable {
    private final Meter mutationMeter =
        BigtableClientMetrics.meter(MetricLevel.Info, "bulk-mutator.mutations.added");
    private final Meter mutationRetryMeter =
        BigtableClientMetrics.meter(MetricLevel.Info, "bulk-mutator.mutations.retried");

    @VisibleForTesting
    Long retryId;
    private RequestManager currentRequestManager;
    private BackOff currentBackoff;
    private int failedCount;
    private ListenableFuture<List<MutateRowsResponse>> mutateRowsFuture;

    private Batch() {
      this.currentRequestManager = new RequestManager(tableName, mutationMeter);
    }

    /**
     * Adds a {@link com.google.bigtable.v2.MutateRowsRequest.Entry} to the {@link
     * com.google.bigtable.v2.MutateRowsRequest.Builder}. NOTE: Users have to make sure that this
     * gets called in a thread safe way.
     *
     * @param entry The {@link com.google.bigtable.v2.MutateRowsRequest.Entry} to add
     * @return a {@link SettableFuture} that will be populated when the {@link MutateRowsResponse}
     *     returns from the server. See {@link #addCallback(ListenableFuture)} for more information
     *     about how the SettableFuture is set.
     */
    private ListenableFuture<MutateRowResponse> add(MutateRowsRequest.Entry entry) {
      Preconditions.checkNotNull(entry);
      SettableFuture<MutateRowResponse> future = SettableFuture.create();
      currentRequestManager.add(future, entry);
      return future;
    }

    private boolean isFull() {
      Preconditions.checkNotNull(currentRequestManager);
      return getRequestCount() >= maxRowKeyCount
          || (currentRequestManager.approximateByteSize >= maxRequestSize);
    }

    /**
     * Adds a {@link FutureCallback} that will update all of the SettableFutures created by
     * {@link BulkMutation#add(MutateRowRequest)} when the provided {@link ListenableFuture} for the
     * {@link MutateRowsResponse} is complete.
     */
    @VisibleForTesting
    void addCallback(ListenableFuture<List<MutateRowsResponse>> bulkFuture) {
      Futures.addCallback(
          bulkFuture,
          new FutureCallback<List<MutateRowsResponse>>() {
            @Override
            public void onSuccess(List<MutateRowsResponse> result) {
              mutateRowsFuture = null;
              handleResult(result);
            }

            @Override
            public void onFailure(Throwable t) {
              mutateRowsFuture = null;
              performFullRetry(new AtomicReference<Long>(), t);
            }
          });
    }

    private synchronized void handleResult(List<MutateRowsResponse> results) {

      AtomicReference<Long> backoffTime = new AtomicReference<>();
      try {
        if (operationsAreComplete()) {
          LOG.warn("Got duplicate responses for bulk mutation.");
          setRetryComplete();
          return;
        }
        if (results == null || results.isEmpty()) {
          performFullRetry(backoffTime,
            new IllegalStateException("No MutateRowResponses were found."));
          return;
        }

        List<MutateRowsResponse.Entry> entries = new ArrayList<>();
        for (MutateRowsResponse response : results) {
          entries.addAll(response.getEntriesList());
        }

        if (entries.isEmpty()) {
          performFullRetry(backoffTime,
            new IllegalStateException("No MutateRowsResponses entries were found."));
          return;
        }

        String tableName = currentRequestManager.request.getTableName();
        RequestManager retryRequestManager = new RequestManager(tableName, mutationRetryMeter);

        handleResponses(backoffTime, entries, retryRequestManager);
        handleExtraFutures(backoffTime, retryRequestManager, entries);
        completeOrRetry(backoffTime, retryRequestManager);
      } catch (Throwable e) {
        LOG.error(
          "Unexpected Exception occurred. Treating this issue as a temporary issue and retrying.",
          e);
        performFullRetry(backoffTime, e);
      }
    }

    private void performFullRetry(AtomicReference<Long> backoff, Throwable t) {
      if (currentRequestManager == null) {
        setRetryComplete();
        return;
      }
      Long backoffMs = getCurrentBackoff(backoff);
      failedCount++;
      if (backoffMs == BackOff.STOP) {
        setFailure(new BigtableRetriesExhaustedException("Batch #" + retryId + " Exhausted retries.", t));
      } else {
        LOG.info("Retrying failed call for batch #%d. Failure #%d, got: %s", t, retryId, failedCount,
          io.grpc.Status.fromThrowable(t));
        mutationRetryMeter.mark(getRequestCount());
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
        if (index >= getRequestCount()) {
          LOG.error("Got extra status: %s", entry);
          break;
        }

        SettableFuture<MutateRowResponse> future = currentRequestManager.futures.get(index);

        if (future == null) {
          LOG.warn("Could not find a future for index %d.", index);
          return;
        }

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
      Set<Integer> indexes = getIndexes(entries);
      long missingEntriesCount = 0;
      getCurrentBackoff(backoffTime);
      for (int i = 0; i < getRequestCount(); i++) {
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

    private Set<Integer> getIndexes(List<Entry> entries) {
      Set<Integer> indexes = new HashSet<>(entries.size());
      for (Entry entry : entries) {
        indexes.add((int) entry.getIndex());
      }
      return indexes;
    }

    private void completeOrRetry(
        AtomicReference<Long> backoffTime, RequestManager retryRequestManager) {
      if (retryRequestManager == null || retryRequestManager.isEmpty()) {
        setRetryComplete();
      } else {
        this.currentRequestManager = retryRequestManager;
        failedCount++;
        mutationRetryMeter.mark(getRequestCount());
        LOG.info(
            "Retrying failed call. Failure #%d, got #%d failures",
            failedCount, getRequestCount());
        retryExecutorService.schedule(this, getCurrentBackoff(backoffTime), TimeUnit.MILLISECONDS);
      }
    }

    private boolean isRetryable(int codeId) {
      Code code = io.grpc.Status.fromCodeValue(codeId).getCode();
      return retryOptions.isRetryable(code);
    }

    @Override
    public synchronized void run() {
      if (operationsAreComplete()) {
        setRetryComplete();
        return;
      }
      try {
        if (retryId == null) {
          retryId = Long.valueOf(
            asyncExecutor.getOperationAccountant().registerComplexOperation(createRetryHandler()));
        }
        mutateRowsFuture = asyncExecutor.mutateRowsAsync(currentRequestManager.build());
        currentRequestManager.lastRpcSentTime = clock.currentTimeMillis();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        mutateRowsFuture = Futures.<List<MutateRowsResponse>> immediateFailedFuture(e);
      } catch (Throwable e) {
        mutateRowsFuture = Futures.<List<MutateRowsResponse>> immediateFailedFuture(e);
      } finally {
        addCallback(mutateRowsFuture);
      }
    }

    /**
     * A {@link ComplexOperationStalenessHandler} is a method of last resort in cases where somehow the various
     * listeners failed to get a response, or had some kind of strange issue.
     */
    private ComplexOperationStalenessHandler createRetryHandler() {
      return new ComplexOperationStalenessHandler() {
        @Override
        public void performRetryIfStale() {
          // If the retryId is null, it means that the operation somehow fails partially,
          // cleanup the retry.
          if (retryId == null || operationsAreComplete() || currentRequestManager.isStale()) {
            setRetryComplete();
          }
        }
      };
    }

    /**
     * This would have happened after all retries are exhausted on the MutateRowsRequest. Don't
     * retry individual mutations.
     */
    private void setFailure(Throwable t) {
      try {
        if (currentRequestManager != null) {
          for (SettableFuture<MutateRowResponse> future : currentRequestManager.futures) {
            future.setException(t);
          }
        }
      } finally {
        setRetryComplete();
      }
    }

    private synchronized void setRetryComplete() {
      if (retryId != null) {
        if (mutateRowsFuture != null) {
          mutateRowsFuture.cancel(true);
        }
        mutateRowsFuture = null;
        asyncExecutor.getOperationAccountant().onComplexOperationCompletion(retryId);
        if (failedCount > 0) {
          LOG.info("Batch #%d recovered from the failure and completed.", retryId);
        }
        retryId = null;
      }
      currentRequestManager = null;
    }

    @VisibleForTesting
    int getRequestCount() {
      return currentRequestManager == null ? 0 : currentRequestManager.getRequestCount();
    }

    boolean operationsAreComplete() {
      return currentRequestManager == null || currentRequestManager.isEmpty();
    }
  }

  @VisibleForTesting
  Batch currentBatch = null;

  private ScheduledFuture<?> scheduledFlush = null;

  private final String tableName;
  private final AsyncExecutor asyncExecutor;
  private final RetryOptions retryOptions;
  private final ScheduledExecutorService retryExecutorService;
  private final int maxRowKeyCount;
  private final long maxRequestSize;
  private final long autoflushMs;
  private final Meter batchMeter =
      BigtableClientMetrics.meter(MetricLevel.Info, "bulk-mutator.batch.meter");


  /**
   * Constructor for BulkMutation.
   *
   * @param tableName a {@link BigtableTableName} object for the table to which all {@link
   *     MutateRowRequest}s will be sent.
   * @param asyncExecutor a {@link AsyncExecutor} object that asynchronously sends {@link
   *     MutateRowsRequest}.
   * @param retryOptions a {@link RetryOptions} object that describes how to perform retries.
   * @param retryExecutorService a {@link ScheduledExecutorService} object on which to schedule
   *     retries.
   * @param maxRowKeyCount describes the maximum number of {@link MutateRowRequest}s to send in a
   *     single {@link MutateRowsRequest}.
   * @param maxRequestSize describes the maximum cumulative size of a {@link MutateRowsRequest}.
   */
  public BulkMutation(
      BigtableTableName tableName,
      AsyncExecutor asyncExecutor,
      RetryOptions retryOptions,
      ScheduledExecutorService retryExecutorService,
      int maxRowKeyCount,
      long maxRequestSize) {
    this(tableName, asyncExecutor, retryOptions, retryExecutorService, maxRowKeyCount, maxRequestSize, 0);
  }

  /**
   * Constructor for BulkMutation.
   *
   * @param tableName a {@link BigtableTableName} object for the table to which all {@link
   *     MutateRowRequest}s will be sent.
   * @param asyncExecutor a {@link AsyncExecutor} object that asynchronously sends {@link
   *     MutateRowsRequest}.
   * @param retryOptions a {@link RetryOptions} object that describes how to perform retries.
   * @param retryExecutorService a {@link ScheduledExecutorService} object on which to schedule
   *     retries.
   * @param maxRowKeyCount describes the maximum number of {@link MutateRowRequest}s to send in a
   *     single {@link MutateRowsRequest}.
   * @param maxRequestSize describes the maximum cumulative size of a {@link MutateRowsRequest}.
   * @param autoflushMs the maximum number of milliseconds that items can linger before being flush. 0 to disable.
   */
  public BulkMutation(
      BigtableTableName tableName,
      AsyncExecutor asyncExecutor,
      RetryOptions retryOptions,
      ScheduledExecutorService retryExecutorService,
      int maxRowKeyCount,
      long maxRequestSize,
      long autoflushMs) {
    this.tableName = tableName.toString();
    this.asyncExecutor = asyncExecutor;
    this.retryOptions = retryOptions;
    this.retryExecutorService = retryExecutorService;
    this.maxRowKeyCount = maxRowKeyCount;
    this.maxRequestSize = maxRequestSize;
    this.autoflushMs = autoflushMs;
  }
  public ListenableFuture<MutateRowResponse> add(MutateRowRequest request) {
    return add(convert(request));
  }

  /**
   * Adds a {@link MutateRowsRequest.Entry} to the {@link MutateRowsRequest.Builder}.
   * @param entry The {@link MutateRowsRequest.Entry} to add
   * @return a {@link com.google.common.util.concurrent.SettableFuture} that will be populated when
   *         the {@link MutateRowsResponse} returns from the server. See
   *         {@link BulkMutation.Batch#addCallback(ListenableFuture)} for more information about how
   *         the SettableFuture is set.
   */
  public synchronized ListenableFuture<MutateRowResponse> add(MutateRowsRequest.Entry entry) {
    Preconditions.checkNotNull(entry, "Request null");
    Preconditions.checkArgument(!entry.getRowKey().isEmpty(), "Request has an empty rowkey");
    if (currentBatch == null) {
      batchMeter.mark();
      currentBatch = new Batch();
    }

    ListenableFuture<MutateRowResponse> future = currentBatch.add(entry);
    if (currentBatch.isFull()) {
      flush();
    }

    // If autoflushing is enabled and there is pending data then schedule a flush if one hasn't been scheduled
    // NOTE: this is optimized for adding minimal overhead to per item adds, at the expense of periodic partial batches
    if (this.autoflushMs > 0 && currentBatch != null && scheduledFlush == null) {
      scheduledFlush = retryExecutorService.schedule(new Runnable() {
        @Override
        public void run() {
          scheduledFlush = null;
          flush();
        }
      }, autoflushMs, TimeUnit.MILLISECONDS);
    }

    return future;
  }

  /**
   * Send any outstanding {@link MutateRowRequest}s.
   */
  public synchronized void flush() {
    if (currentBatch != null) {
      currentBatch.run();
      currentBatch = null;
    }
  }

  /**
   * @return false if there are any outstanding {@link MutateRowRequest} that still need to be sent.
   */
  public boolean isFlushed() {
    return currentBatch == null;
  }
}
