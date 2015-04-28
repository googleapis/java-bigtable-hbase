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
import com.google.cloud.bigtable.grpc.BigtableClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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

  // In flush, wait up to this number of milliseconds without any operations completing.  If
  // this amount of time goes by without any updates, flush will log a warning.  Flush()
  // will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING = 300000;

  /**
   * This class ensures that operations meet heap size and max RPC counts.  A wait will occur
   * if RPCs are requested after heap and RPC count thresholds are exceeded.
   */
  @VisibleForTesting
  static class HeapSizeManager {
    private final long maxHeapSize;
    private final int maxInFlightRpcs;
    private long currentWriteBufferSize = 0;
    private long operationSequenceGenerator = 0;

    @VisibleForTesting
    final Map<Long, Long> pendingOperationsWithSize = new HashMap<>();
    private long lastOperationChange = System.currentTimeMillis();

    public HeapSizeManager(long maxHeapSize, int maxInflightRpcs) {
      this.maxHeapSize = maxHeapSize;
      this.maxInFlightRpcs = maxInflightRpcs;
    }

    public long getMaxHeapSize() {
      return maxHeapSize;
    }

    public synchronized void waitUntilAllOperationsAreDone() throws InterruptedException {
      boolean performedWarning = false;
      while(!pendingOperationsWithSize.isEmpty()) {
        if (!performedWarning
            && lastOperationChange + INTERVAL_NO_SUCCESS_WARNING < System.currentTimeMillis()) {
          long lastUpdated = (System.currentTimeMillis() - lastOperationChange) / 1000;
          LOG.warn("No operations completed within the last %d seconds."
              + "There are still %d operations in progress.", lastUpdated,
            pendingOperationsWithSize.size());
          performedWarning = true;
        }
        wait(WAIT_MILLIS);
      }
      if (performedWarning) {
        LOG.info("flush() completed");
      }
    }

    public synchronized long registerOperationWithHeapSize(long heapSize)
        throws InterruptedException {
      long operationId = ++operationSequenceGenerator;
      while (currentWriteBufferSize >= maxHeapSize
          || pendingOperationsWithSize.size() >= maxInFlightRpcs) {
        wait(WAIT_MILLIS);
      }

      lastOperationChange = System.currentTimeMillis();
      pendingOperationsWithSize.put(operationId, heapSize);
      currentWriteBufferSize += heapSize;
      return operationId;
    }

    public synchronized void operationComplete(long operationSequenceId) {
      lastOperationChange = System.currentTimeMillis();
      Long heapSize = pendingOperationsWithSize.remove(operationSequenceId);
      if (heapSize != null) {
        currentWriteBufferSize -= heapSize;
        notifyAll();
      } else {
        LOG.warn("An operation completion was recieved multiple times. Your operations completed."
            + " Please notify Google that this occurred.");
      }
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
   * Makes sure that mutations and flushes are safe to proceed.  Ensures that while the mutator
   * is closing, there will be no additional writes.
   */
  private final ReentrantReadWriteLock mutationLock = new ReentrantReadWriteLock();
  private final BatchExecutor batchExecutor;
  private final ExceptionListener exceptionListener;

  @VisibleForTesting
  final AtomicBoolean hasExceptions = new AtomicBoolean(false);

  @VisibleForTesting
  final List<MutationException> globalExceptions = new ArrayList<MutationException>();

  private final String host;

  public BigtableBufferedMutator(
      Configuration configuration,
      TableName tableName,
      int maxInflightRpcs,
      long maxHeapSize,
      BigtableClient client,
      BigtableOptions options,
      ExecutorService executorService,
      BufferedMutator.ExceptionListener listener) {
    this.sizeManager = new HeapSizeManager(maxHeapSize, maxInflightRpcs);
    this.configuration = configuration;
    this.tableName = tableName;
    this.exceptionListener = listener;
    InetAddress host = null;

    try {
      host = options.getTransportOptions().getHost();
    } catch (IOException e) {
      LOG.warn("Could not get the host used for writes", options);
    }
    this.host = host == null ? null : host.toString();

    DeleteAdapter deleteAdapter = new DeleteAdapter();
    PutAdapter putAdapter = new PutAdapter(configuration);
    ScanAdapter scanAdapter = new ScanAdapter(FilterAdapter.buildAdapter());
    GetAdapter getAdapter = new GetAdapter(scanAdapter);

    RowMutationsAdapter rowMutationsAdapter =
        new RowMutationsAdapter(
            new MutationAdapter(
                deleteAdapter,
                putAdapter,
                new UnsupportedOperationAdapter<Increment>("increment"),
                new UnsupportedOperationAdapter<Append>("append")));

    ListeningExecutorService listeningExecutorService =
        MoreExecutors.listeningDecorator(executorService);

    batchExecutor = new BatchExecutor(
        client,
        options,
        TableMetadataSetter.from(tableName, options),
        listeningExecutorService,
        getAdapter,
        putAdapter,
        deleteAdapter,
        rowMutationsAdapter,
        new AppendAdapter(),
        new IncrementAdapter(),
        new RowAdapter());
  }

  @VisibleForTesting
  public BigtableBufferedMutator(
      BatchExecutor batchExecutor,
      long maxHeapSize,
      ExceptionListener exceptionListener,
      String host,
      int maxInflightRpcs,
      TableName tableName) {
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
   * Being a Mutation. This method will block if either of the following are true:
   * 1) There are more than {@code maxInflightRpcs} RPCs in flight
   * 2) There are more than {@link #getWriteBufferSize()} bytes pending
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

  private void doMutation(final Mutation mutation) throws RetriesExhaustedWithDetailsException {
    Long sequenceId = null;
    try {
      // registerOperationWithHeapSize() waits until both the memory and rpc count maximum
      // requirements are achieved.
      sequenceId = sizeManager.registerOperationWithHeapSize(mutation.heapSize());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    AccountingFutureCallback callback = new AccountingFutureCallback(mutation, sequenceId);

    // TODO: Consider adding the callback in another executor for the blocking call. There are some
    // concerns running this callback on the rpc threadpool. This callback requires locks on
    // resources. If they cannot be acquired, the callback would block future completions.
    Futures.addCallback(batchExecutor.issueRequest(mutation), callback);
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
        mutationExceptions = new ArrayList<>(globalExceptions);
        globalExceptions.clear();
        hasExceptions.set(false);
      }

      if (mutationExceptions.isEmpty()) {
        // This should never happen.
        return;
      }

      List<Throwable> problems = new ArrayList<>(mutationExceptions.size());
      ArrayList<String> hostnames = new ArrayList<>(mutationExceptions.size());
      List<Row> failedMutations = new ArrayList<>(mutationExceptions.size());

      for (MutationException mutationException : mutationExceptions) {
        problems.add(mutationException.throwable);
        failedMutations.add(mutationException.mutation);
        hostnames.add(host);
      }

      RetriesExhaustedWithDetailsException exception = new RetriesExhaustedWithDetailsException(
          problems, failedMutations, hostnames);
      exceptionListener.onException(exception, this);
    }
  }

  private class AccountingFutureCallback implements FutureCallback<GeneratedMessage> {
    private final long operationSequenceId;
    private final Row mutation;

    public AccountingFutureCallback(Row mutation, long operationSequenceId) {
      this.mutation = mutation;
      this.operationSequenceId = operationSequenceId;
    }

    @Override
    public void onFailure(Throwable t) {
      synchronized (globalExceptions) {
        globalExceptions.add(new MutationException(mutation, t));
      }
      hasExceptions.set(true);
      sizeManager.operationComplete(operationSequenceId);
    }

    @Override
    public void onSuccess(GeneratedMessage ignored) {
      sizeManager.operationComplete(operationSequenceId);
    }
  }
}
