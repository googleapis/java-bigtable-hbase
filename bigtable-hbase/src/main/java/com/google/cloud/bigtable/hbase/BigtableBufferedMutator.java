/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableClient;
import com.google.cloud.bigtable.hbase.adapters.AppendAdapter;
import com.google.cloud.bigtable.hbase.adapters.DeleteAdapter;
import com.google.cloud.bigtable.hbase.adapters.GetAdapter;
import com.google.cloud.bigtable.hbase.adapters.IncrementAdapter;
import com.google.cloud.bigtable.hbase.adapters.MutationAdapter;
import com.google.cloud.bigtable.hbase.adapters.PutAdapter;
import com.google.cloud.bigtable.hbase.adapters.RowAdapter;
import com.google.cloud.bigtable.hbase.adapters.RowMutationsAdapter;
import com.google.cloud.bigtable.hbase.adapters.ScanAdapter;
import com.google.cloud.bigtable.hbase.adapters.TableMetadataSetter;
import com.google.cloud.bigtable.hbase.adapters.UnsupportedOperationAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.GeneratedMessage;

/**
 * Bigtable's {@link BufferedMutator} implementation.
 */
// TODO: Cleanup the interface so that @VisibleForTesting can be reduced.
public class BigtableBufferedMutator implements BufferedMutator {

  protected static final Logger LOG = new Logger(BigtableBufferedMutator.class);

  // Flush is not properly synchronized with respect to waiting. It will never exit
  // improperly, but it might wait more than it has to. Setting this to a low value ensures
  // that improper waiting is minimal.
  private static final long WAIT_MILLIS = 250;

  // In flush, wait up to this number of milliseconds without any operations completing. If
  // this amount of time goes by without any updates, flush will log a warning. Flush()
  // will still wait to complete.
  private static final long COMPLETION_GRACE_PERIOD_MILLIS = 60000;

  protected final ExecutorService heapSizeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder()
        .setNameFormat("heapSize-async-%s")
        .setDaemon(true)
        .build());

  /**
   * This class ensures that operations meet heap size and max RPC counts. A wait will occur if RPCs
   * are requested after heap and RPC count thresholds are exceeded.
   */
  @VisibleForTesting
  static class HeapSizeManager {
    private final long maxHeapSize;
    private final int maxInFlightRpcs;
    private long currentWriteBufferSize = 0;
    private long operationSequenceGenerator = 0;

    @VisibleForTesting
    final Map<Long, RequestAccountant> activeRequests = new HashMap<>();
    private long lastOperationChange = System.currentTimeMillis();
    private long nextLogOrSweep = -1L;

    public HeapSizeManager(long maxHeapSize, int maxInflightRpcs) {
      this.maxHeapSize = maxHeapSize;
      this.maxInFlightRpcs = maxInflightRpcs;
    }

    public long getMaxHeapSize() {
      return maxHeapSize;
    }

    public synchronized void waitUntilAllOperationsAreDone() throws InterruptedException {
      cleanupCompletedCallbacks();
      while (!activeRequests.isEmpty()) {
        waitForCompletions();
      }
      LOG.trace("flush() completed");
      nextLogOrSweep = -1L;
    }

    /**
     * Initialize an AccountingFutureCallback once heap size and max in flight rpcs restrictions are
     * satisfied. This method will block the current thread if heap size or max in flight limits are
     * violated.
     */
    public synchronized void initializeOperation(RequestAccountant callback)
        throws InterruptedException {
      while (currentWriteBufferSize >= maxHeapSize || activeRequests.size() >= maxInFlightRpcs) {
        waitForCompletions();
      }

      callback.operationSequenceId = ++operationSequenceGenerator;
      currentWriteBufferSize += callback.heapSize;
      callback.sizeWasAdded = true;
      lastOperationChange = System.currentTimeMillis();
    }

    /**
     * Add the callback once it's underlying operation actually started successfully.
     */
    public synchronized void registerInvokedCallback(RequestAccountant callback) {
      // There could have been a backlog of RPCs, and the RPC completed by the time the synchronized
      // registerInvokedCallback method was finally called. In that case, simply clean up the heap
      // size. Otherwise, keep track of the callback in the activeRequests Map.
      if (callback.isRequestDone()) {
        operationComplete(callback);
      } else {
        this.activeRequests.put(callback.operationSequenceId, callback);
      }
    }

    public synchronized void operationComplete(RequestAccountant callback) {
      if (removeAndDecrement(callback)){
        lastOperationChange = System.currentTimeMillis();
        notifyAll();
      }
    }

    public synchronized boolean hasInflightRequests() {
      return !activeRequests.isEmpty();
    }

    private void waitForCompletions() throws InterruptedException {
      long now = System.currentTimeMillis();
      long lastUpdated = now - lastOperationChange;

      if (now >= nextLogOrSweep && COMPLETION_GRACE_PERIOD_MILLIS < lastUpdated) {
        if (cleanupCompletedCallbacks()) {
          return;
        } else {
          LOG.warn("No operations completed within the last %d seconds."
              + "There are still %d operations in progress with buffer size %d ", lastUpdated,
            activeRequests.size(), currentWriteBufferSize);
          nextLogOrSweep = System.currentTimeMillis() + COMPLETION_GRACE_PERIOD_MILLIS;
        }
      }
      wait(WAIT_MILLIS);
    }

    /**
     * gRPC may have finished operations without calling the callback. Check to make sure that all
     * registered callbacks that are actually complete are removed from the heap size manager.
     */
    private boolean cleanupCompletedCallbacks() {
      boolean updated = false;
      for (RequestAccountant callback : new ArrayList<>(activeRequests.values())) {
        // Remove this callback if somehow the operation completed without the listener being
        // invoked or if something went wrong during the startup process.
        if (callback.isRequestDone()) {
          updated |= removeAndDecrement(callback);
        }
      }
      if (updated) {
        lastOperationChange = System.currentTimeMillis();
        notifyAll();
      }
      return updated;
    }

    /**
     * If the callback was not removed before, then remove it from the callbacks Map and
     * decrement the heap size from the cumulative currentWriteBufferSize value.
     *
     * @return true if either heap size or in flight rpc count changed.
     */
    private boolean removeAndDecrement(RequestAccountant callback) {
      boolean accountingUpdated = false;

      // Update the currentWriteBufferSize.
      if (callback.sizeWasAdded && !callback.sizeWasRemoved) {
        currentWriteBufferSize -= callback.getHeapSize();
        callback.sizeWasRemoved = true;
        accountingUpdated = true;
      }

      // Remove the callback if it was in the aggregated Map.
      if (activeRequests.remove(callback.operationSequenceId) != null) {
        accountingUpdated = true;
      }

      // If true, either the currentWriteBufferSize or in flight rpc count were updated
      return accountingUpdated;
    }

    @VisibleForTesting
    synchronized Object getHeapSize() {
      return currentWriteBufferSize;
    }
  }

  @VisibleForTesting
  static class MutationException {
    private final Row mutation;
    private final Throwable throwable;

    MutationException(Row mutation, Throwable throwable) {
      this.mutation = mutation;
      this.throwable = throwable;
    }
  }

  private final Configuration configuration;
  private final TableName tableName;

  @VisibleForTesting
  final HeapSizeManager sizeManager;
  private boolean closed = false;

  /**
   * Makes sure that mutations and flushes are safe to proceed. Ensures that while the mutator is
   * closing, there will be no additional writes.
   */
  private final ReentrantReadWriteLock mutationLock = new ReentrantReadWriteLock();
  private final BatchExecutor batchExecutor;
  private final ExceptionListener exceptionListener;

  @VisibleForTesting
  final AtomicBoolean hasExceptions = new AtomicBoolean(false);

  @VisibleForTesting
  final List<MutationException> globalExceptions = new ArrayList<MutationException>();

  private final String host;

  public BigtableBufferedMutator(Configuration configuration, TableName tableName,
      int maxInflightRpcs, long maxHeapSize, BigtableClient client, BigtableOptions options,
      ExecutorService executorService, BufferedMutator.ExceptionListener listener) {
    this.sizeManager = new HeapSizeManager(maxHeapSize, maxInflightRpcs);
    this.configuration = configuration;
    this.tableName = tableName;
    this.exceptionListener = listener;
    InetAddress host = options.getDataHost();
    this.host = host == null ? null : host.toString();

    DeleteAdapter deleteAdapter = new DeleteAdapter();
    PutAdapter putAdapter = new PutAdapter(configuration);
    ScanAdapter scanAdapter = new ScanAdapter(FilterAdapter.buildAdapter());
    GetAdapter getAdapter = new GetAdapter(scanAdapter);

    RowMutationsAdapter rowMutationsAdapter =
        new RowMutationsAdapter(new MutationAdapter(deleteAdapter, putAdapter,
            new UnsupportedOperationAdapter<Increment>("increment"),
            new UnsupportedOperationAdapter<Append>("append")));

    ListeningExecutorService listeningExecutorService =
        MoreExecutors.listeningDecorator(executorService);

    batchExecutor =
        new BatchExecutor(client, options, TableMetadataSetter.from(tableName, options),
            listeningExecutorService, getAdapter, putAdapter, deleteAdapter, rowMutationsAdapter,
            new AppendAdapter(), new IncrementAdapter(), new RowAdapter());
  }

  @VisibleForTesting
  public BigtableBufferedMutator(BatchExecutor batchExecutor, long maxHeapSize,
      ExceptionListener exceptionListener, String host, int maxInflightRpcs, TableName tableName) {
    this.batchExecutor = batchExecutor;
    this.configuration = null;
    this.exceptionListener = exceptionListener;
    this.host = host;
    this.tableName = tableName;
    this.sizeManager = new HeapSizeManager(maxHeapSize, maxInflightRpcs);
  }

  @Override
  public void close() throws IOException {
    WriteLock lock = mutationLock.writeLock();
    lock.lock();
    try {
      if (!closed) {
        doFlush();
        heapSizeExecutor.shutdown();
        closed = true;
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void flush() throws IOException {
    WriteLock lock = mutationLock.writeLock();
    lock.lock();
    try {
      doFlush();
    } finally {
      lock.unlock();
    }
  }

  private void doFlush() throws IOException {
    LOG.trace("Flushing");
    try {
      sizeManager.waitUntilAllOperationsAreDone();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    LOG.trace("Done flushing");
    handleExceptions();
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public long getWriteBufferSize() {
    return this.sizeManager.getMaxHeapSize();
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws IOException {
    // Ensure that close() or flush() aren't current being called.
    ReadLock lock = mutationLock.readLock();
    lock.lock();
    try {
      if (closed) {
        throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
      }
      handleExceptions();
      for (Mutation mutation : mutations) {
        doMutation(mutation);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * <p>
   * This will write a single mutation. This method will block if either of the following are true:
   * </p>
   * <ol>
   * <li>There are more than {@code maxInflightRpcs} RPCs in flight
   * <li>There are more than {@link #getWriteBufferSize()} bytes pending
   * </ol>
   */
  @Override
  public void mutate(final Mutation mutation) throws IOException {
    ReadLock lock = mutationLock.readLock();
    lock.lock();
    try {
      if (closed) {
        throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
      }
      handleExceptions();
      doMutation(mutation);
    } finally {
      lock.unlock();
    }
  }

  private void doMutation(final Mutation mutation) throws IOException {
    RequestAccountant accountant = new RequestAccountant(mutation);
    try {
      // initializeOperation() waits until heapSize and rpc counts are not at their maximum
      // values. It then adds the mutation's heapSize to the accumulated value.
      sizeManager.initializeOperation(accountant);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

      // Cleanup the callback. We can remove the heapSize from the accumulated heapSize value.
      accountant.onFailure(e);
      throw new IOException("doMutation was interruptted", e);
    }
    try {
      accountant.request = batchExecutor.issueRequest(mutation);
    } catch (RuntimeException e) {
      // The issueRequest might throw a validation exception or something like that. Make sure that
      // the callback's clean up method gets invoked so that the HeapSizeManager doesn't block
      // future requests.
      accountant.onFailure(e);
      throw e;
    }
    Futures.addCallback(accountant.request, accountant, heapSizeExecutor);
    sizeManager.registerInvokedCallback(accountant);
  }

  /**
   * Create a {@link RetriesExhaustedWithDetailsException} if there were any async exceptions and
   * send it to the {@link org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener}.
   */
  @VisibleForTesting
  void handleExceptions() throws RetriesExhaustedWithDetailsException {
    if (hasExceptions.get()) {
      ArrayList<MutationException> mutationExceptions = null;
      synchronized (globalExceptions) {
        if (!hasExceptions.get()) {
          return;
        }
        hasExceptions.set(false);

        if (globalExceptions.isEmpty()) {
          return;
        } else {
          mutationExceptions = new ArrayList<>(globalExceptions);
          globalExceptions.clear();
        }
      }

      List<Throwable> problems = new ArrayList<>(mutationExceptions.size());
      ArrayList<String> hostnames = new ArrayList<>(mutationExceptions.size());
      List<Row> failedMutations = new ArrayList<>(mutationExceptions.size());

      for (MutationException mutationException : mutationExceptions) {
        problems.add(mutationException.throwable);
        failedMutations.add(mutationException.mutation);
        hostnames.add(host);
      }

      RetriesExhaustedWithDetailsException exception =
          new RetriesExhaustedWithDetailsException(problems, failedMutations, hostnames);
      exceptionListener.onException(exception, this);
    }
  }

  class RequestAccountant implements FutureCallback<GeneratedMessage> {
    private final Row row;
    private final long heapSize;

    private Long operationSequenceId;
    private ListenableFuture<? extends GeneratedMessage> request;
    private boolean sizeWasAdded = false;
    private boolean sizeWasRemoved = false;

    public RequestAccountant(Mutation mutation) {
      this.row = mutation;
      this.heapSize = mutation.heapSize();
    }

    public long getHeapSize() {
      return heapSize;
    }

    public boolean isRequestDone() {
      return request != null && request.isDone();
    }

    @Override
    public void onFailure(final Throwable t) {
      sizeManager.operationComplete(this);
      synchronized (globalExceptions) {
        globalExceptions.add(new MutationException(row, t));
        hasExceptions.set(true);
      }
    }

    @Override
    public void onSuccess(GeneratedMessage ignored) {
      sizeManager.operationComplete(this);
    }

    public boolean sizeWasAdded() {
      return sizeWasAdded;
    }

    public boolean sizeWasRemoved() {
      return sizeWasRemoved;
    }
  }

  public boolean hasInflightRequests() {
    return sizeManager.hasInflightRequests();
  }
}
