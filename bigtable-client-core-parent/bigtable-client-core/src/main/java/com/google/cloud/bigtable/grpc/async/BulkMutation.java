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
import com.google.api.client.util.NanoClock;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.MutateRowsResponse.Entry;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
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

import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
      io.grpc.Status.INTERNAL
          .withDescription("Mutation does not have a status")
          .asRuntimeException();
  /** Constant <code>LOG</code> */
  @VisibleForTesting
  static Logger LOG = new Logger(BulkMutation.class);

  public static final long MAX_RPC_WAIT_TIME_NANOS = TimeUnit.MINUTES.toNanos(7);

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
  static class RequestManager {
    private final List<SettableFuture<MutateRowResponse>> futures = new ArrayList<>();
    private final MutateRowsRequest.Builder builder;
    private final Meter addMeter;

    private MutateRowsRequest request;
    private long approximateByteSize = 0l;

    @VisibleForTesting
    Long lastRpcSentTimeNanos;
    private NanoClock clock;

    RequestManager(String tableName, Meter addMeter, NanoClock clock) {
      this.builder = MutateRowsRequest.newBuilder().setTableName(tableName);
      this.approximateByteSize = tableName.length() + 2;
      this.addMeter = addMeter;
      this.clock = clock;
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
      return lastRpcSentTimeNanos != null && calculateTimeUntilStaleNanos() <= 0;
    }

    public long calculateTimeUntilStaleNanos() {
      return lastRpcSentTimeNanos + MAX_RPC_WAIT_TIME_NANOS - clock.nanoTime();
    }

    public boolean wasSent() {
      return lastRpcSentTimeNanos != null;
    }
  }

  @VisibleForTesting
  class Batch implements Runnable {
    private final Meter mutationMeter =
        BigtableClientMetrics.meter(MetricLevel.Info, "bulk-mutator.mutations.added");

    private final SettableFuture<String> completionFuture = SettableFuture.create();
    private RequestManager currentRequestManager;
    private ListenableFuture<List<MutateRowsResponse>> mutateRowsFuture;
    private ScheduledFuture<?> stalenessFuture;

    private Batch() {
      this.currentRequestManager = new RequestManager(tableName, mutationMeter, clock);
    }

    /**
     * Adds a {@link com.google.bigtable.v2.MutateRowsRequest.Entry} to the {@link
     * com.google.bigtable.v2.MutateRowsRequest.Builder}. NOTE: Users have to make sure that this
     * gets called in a thread safe way.
     *
     * @param entry The {@link com.google.bigtable.v2.MutateRowsRequest.Entry} to add
     * @return a {@link SettableFuture} that will be populated when the {@link MutateRowsResponse}
     *     returns from the server. See {@link #addCallback(ListenableFuture, Long)} for more 
     *     information about how the SettableFuture is set.
     */
    private ListenableFuture<MutateRowResponse> add(MutateRowsRequest.Entry entry) {
      Preconditions.checkNotNull(entry);
      SettableFuture<MutateRowResponse> future = SettableFuture.create();
      currentRequestManager.add(future, entry);
      return future;
    }

    private boolean isFull() {
      Preconditions.checkNotNull(operationsAreComplete());
      return getRequestCount() >= maxRowKeyCount
          || (currentRequestManager.approximateByteSize >= maxRequestSize);
    }

    /**
     * Adds a {@link FutureCallback} that will update all of the SettableFutures created by
     * {@link BulkMutation#add(MutateRowRequest)} when the provided {@link ListenableFuture} for the
     * {@link MutateRowsResponse} is complete.
     */
    private void addCallback(ListenableFuture<List<MutateRowsResponse>> bulkFuture,
        final Long rpcId) {
      Futures.addCallback(bulkFuture, new FutureCallback<List<MutateRowsResponse>>() {
        @Override
        public void onSuccess(List<MutateRowsResponse> result) {
          markCompletion();
          handleResult(result);
        }

        @Override
        public void onFailure(Throwable t) {
          markCompletion();
          setFailure(t);
        }

        private void markCompletion() {
          if (rpcId != null) {
            resourceLimiter.markCanBeCompleted(rpcId);
          }
        }
      });
    }

    @VisibleForTesting
    synchronized void handleResult(List<MutateRowsResponse> results) {
      mutateRowsFuture = null;
      if (results.size() != 1) {
        setFailure(io.grpc.Status.INTERNAL.withDescription("Expected only a single response.")
            .asRuntimeException());
        return;
      }
      List<MutateRowsResponse.Entry> entries = new ArrayList<>();
      for (MutateRowsResponse response : results) {
        entries.addAll(response.getEntriesList());
      }

      if (entries.isEmpty()) {
        setFailure(io.grpc.Status.INTERNAL
            .withDescription("No MutateRowsResponses entries were found.").asRuntimeException());
        return;
      }
      try {
        if (operationsAreComplete()) {
          LOG.warn("Got duplicate responses for bulk mutation.");
          setComplete();
          return;
        }
        if (entries.isEmpty()) {
          setFailure(io.grpc.Status.INTERNAL.withDescription("No MutateRowResponses were found.")
              .asRuntimeException());
          return;
        }

        handleResponses(entries);
        handleExtraFutures(entries);
        setComplete();
      } catch (Throwable e) {
        setFailure(e);
      }
    }

    private void handleResponses(Iterable<MutateRowsResponse.Entry> entries) {
      for (MutateRowsResponse.Entry entry : entries) {
        int index = (int) entry.getIndex();
        if (index >= getRequestCount()) {
          LOG.error("Got extra status: %s", entry);
          continue;
        }

        SettableFuture<MutateRowResponse> future = currentRequestManager.futures.get(index);

        if (future == null) {
          LOG.warn("Could not find a future for index %d.", index);
          continue;
        }

        Status status = entry.getStatus();
        int statusCode = status.getCode();
        if (statusCode == io.grpc.Status.Code.OK.value()) {
          future.set(MutateRowResponse.getDefaultInstance());
        } else {
          future.setException(toException(status));
        }
      }
    }

    private void handleExtraFutures(List<Entry> entries) {
      Set<Integer> indexes = getIndexes(entries);
      long missingEntriesCount = 0;
      for (int i = 0; i < getRequestCount(); i++) {
        // If the indexes do not contain this future, then there's a problem.
        if (!indexes.remove(i)) {
          missingEntriesCount++;
          currentRequestManager.futures.get(i).setException(MISSING_ENTRY_EXCEPTION);
        }
      }
      if (missingEntriesCount > 0) {
        LOG.error("Missing %d responses for bulkWrite. Setting exceptions on the futures.",
          missingEntriesCount);
      }
    }

    private Set<Integer> getIndexes(List<Entry> entries) {
      Set<Integer> indexes = new HashSet<>(entries.size());
      for (Entry entry : entries) {
        indexes.add((int) entry.getIndex());
      }
      return indexes;
    }

    @Override
    public synchronized void run() {
      if (operationsAreComplete()) {
        setComplete();
        return;
      }
      Preconditions.checkState(!completionFuture.isDone());
      Long operationId = null;
      try {
        MutateRowsRequest request = currentRequestManager.build();
        operationId = resourceLimiter
            .registerOperationWithHeapSize(request.getSerializedSize());
        mutateRowsFuture = client.mutateRowsAsync(request);
        currentRequestManager.lastRpcSentTimeNanos = clock.nanoTime();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        mutateRowsFuture = Futures.<List<MutateRowsResponse>> immediateFailedFuture(e);
      } catch (Throwable e) {
        mutateRowsFuture = Futures.<List<MutateRowsResponse>> immediateFailedFuture(e);
      } finally {
        addCallback(mutateRowsFuture, operationId);
      }
      setupStalenessChecker();
    }

    protected void setupStalenessChecker() {
      if (operationsAreComplete()){
        setComplete();
        return;
      }
      Runnable runnable = new Runnable() {
        @Override
        public void run() {
          synchronized (Batch.this) {
            if (operationsAreComplete()) {
              setComplete();
            } else if (currentRequestManager.isStale()) {
              setFailure(
                io.grpc.Status.INTERNAL.withDescription("Stale requests.").asRuntimeException());
            } else {
              setupStalenessChecker();
            }
          }
        }
      };
      if (currentRequestManager.lastRpcSentTimeNanos != null) {
        long delay = currentRequestManager.calculateTimeUntilStaleNanos();
        stalenessFuture = retryExecutorService.schedule(runnable, delay, TimeUnit.NANOSECONDS);
      }
    }

    /**
     * This would have happened after all retries are exhausted on the MutateRowsRequest. Don't
     * retry individual mutations.
     */
    @VisibleForTesting
    void setFailure(Throwable t) {
      try {
        if (currentRequestManager != null) {
          for (SettableFuture<MutateRowResponse> future : currentRequestManager.futures) {
            future.setException(t);
          }
        }
      } finally {
        setComplete();
      }
    }
 
    private synchronized void setComplete() {
      if (mutateRowsFuture != null && !mutateRowsFuture.isDone()) {
        mutateRowsFuture.cancel(true);
      }
      cancelIfNotDone(mutateRowsFuture);
      cancelIfNotDone(stalenessFuture);
      if (!completionFuture.isDone()) {
        completionFuture.set("");
      }
      currentRequestManager = null;
      mutateRowsFuture = null;
    }

    private void cancelIfNotDone(Future<?> future) {
      if (future != null && !future.isDone()) {
        future.cancel(true);
      }
    }

    @VisibleForTesting
    int getRequestCount() {
      return operationsAreComplete() ? 0 : currentRequestManager.getRequestCount();
    }

    boolean operationsAreComplete() {
      return currentRequestManager == null || currentRequestManager.isEmpty();
    }
  }

  @VisibleForTesting
  Batch currentBatch = null;

  private ScheduledFuture<?> scheduledFlush = null;

  private final String tableName;
  private final BigtableDataClient client;
  private final ResourceLimiter resourceLimiter;
  private final OperationAccountant operationAccountant;
  private final ScheduledExecutorService retryExecutorService;
  private final int maxRowKeyCount;
  private final long maxRequestSize;
  private final long autoflushMs;
  private final Meter batchMeter =
      BigtableClientMetrics.meter(MetricLevel.Info, "bulk-mutator.batch.meter");

  @VisibleForTesting
  NanoClock clock = NanoClock.SYSTEM;

  /**
   * Constructor for BulkMutation.
   * @param tableName a {@link BigtableTableName} object for the table to which all
   *          {@link MutateRowRequest}s will be sent.
   * @param client a {@link BigtableDataClient} object on which to perform RPCs.
   * @param resourceLimiter a {@link ResourceLimiter} object that curbs the amount of RPCs to
   *          something sustainable for the entire JVM.
   * @param operationAccountant a {@link OperationAccountant} object that keeps track of outstanding
   *          RPCs that this object performed.
   * @param retryOptions a {@link RetryOptions} object that describes how to perform retries.
   * @param retryExecutorService a {@link ScheduledExecutorService} object on which to schedule
   *          retries.
   * @param bulkOptions a {@link BulkOptions} with the user specified options for the behavior of
   *          this instance.
   */
  public BulkMutation(
      BigtableTableName tableName,
      BigtableDataClient client,
      ResourceLimiter resourceLimiter,
      OperationAccountant operationAccountant,
      ScheduledExecutorService retryExecutorService,
      BulkOptions bulkOptions) {
    this.tableName = tableName.toString();
    this.client = client;
    this.resourceLimiter = resourceLimiter;
    this.retryExecutorService = retryExecutorService;
    this.operationAccountant = operationAccountant;
    this.maxRowKeyCount = bulkOptions.getBulkMaxRowKeyCount();
    this.maxRequestSize = bulkOptions.getBulkMaxRequestSize();
    this.autoflushMs = bulkOptions.getAutoflushMs();
  }

  public ListenableFuture<MutateRowResponse> add(MutateRowRequest request) {
    return add(convert(request));
  }

  /**
   * Adds a {@link com.google.bigtable.v2.MutateRowsRequest.Entry} to the {@link
   * com.google.bigtable.v2.MutateRowsRequest.Builder}.
   *
   * @param entry The {@link com.google.bigtable.v2.MutateRowsRequest.Entry} to add
   * @return a {@link com.google.common.util.concurrent.SettableFuture} that will be populated when
   *     the {@link MutateRowsResponse} returns from the server. See {@link
   *     BulkMutation.Batch#addCallback(ListenableFuture, Long)} for more information about how the
   *     SettableFuture is set.
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
      try {
        currentBatch.run();
      } finally {
        operationAccountant.registerOperation(currentBatch.completionFuture);
      }
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
