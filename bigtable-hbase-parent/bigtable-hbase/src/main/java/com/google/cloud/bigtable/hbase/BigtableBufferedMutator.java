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
package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Bigtable's {@link org.apache.hadoop.hbase.client.BufferedMutator} implementation.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableBufferedMutator implements BufferedMutator {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(BigtableBufferedMutator.class);

  /**
   * The time to wait for an async operation to poll before shutting down a thread. Default is 1
   * second.
   */
  protected static final long ASYNC_POLL_WAIT_MS = TimeUnit.SECONDS.toMillis(1);

  /**
   * The time to wait for a an async operation to offer a new mutation before throwing an exception.
   * Default is 1 minute.
   */
  protected static final long ASYNC_OFFER_WAIT_MS = TimeUnit.MINUTES.toMillis(1);

  private static class MutationException {
    private final Row mutation;
    private final Throwable throwable;

    MutationException(Row mutation, Throwable throwable) {
      this.mutation = mutation;
      this.throwable = throwable;
    }
  }

  private AtomicBoolean isShutdown = new AtomicBoolean(false);

  private class MutationOperation implements Runnable {
    final Mutation mutation;
    final long operationId;

    public MutationOperation(Mutation mutation, long operationId) {
      this.mutation = mutation;
      this.operationId = operationId;
    }

    @Override
    public void run() {
      addExceptionCallback(issueRequestDetails(mutation, operationId), mutation);
    }
  }

  private class BulkMutationOperation implements Runnable {
    final Mutation mutation;

    public BulkMutationOperation(Mutation mutation) {
      this.mutation = mutation;
    }

    @Override
    public void run() {
      addExceptionCallback(bulkMutation.add(adapt(mutation)), mutation);
    }
  }

  private final Configuration configuration;

  /**
   * Makes sure that mutations and flushes are safe to proceed.  Ensures that while the mutator
   * is closing, there will be no additional writes.
   */
  private final ReentrantReadWriteLock isClosedLock = new ReentrantReadWriteLock();
  private final ReadLock closedReadLock = isClosedLock.readLock();
  private final WriteLock closedWriteLock = isClosedLock.writeLock();

  private boolean closed = false;

  private final HBaseRequestAdapter adapter;
  private final ExceptionListener exceptionListener;

  private final AtomicBoolean hasExceptions = new AtomicBoolean(false);
  private final List<MutationException> globalExceptions = new ArrayList<MutationException>();

  private final String host;

  private final AsyncExecutor asyncExecutor;
  private final ExecutorService executorService;
  private final BulkOptions bulkOptions;

  private final LinkedBlockingQueue<Runnable> asyncOperationsQueue;
  private final AtomicBoolean operationsComplete = new AtomicBoolean(true);

  /**
   * A counter for the number of {@link #mutationWorker}s are
   * active.
   */
  private final AtomicInteger activeMutationWorkers = new AtomicInteger();

  private BulkMutation bulkMutation = null;

  /**
   * This {@link Runnable} pulls a mutation from {@link #asyncOperationsQueue}, and calls
   * {@link MutationOperation#run()} or {@link BulkMutationOperation#run()}.
   */
  private final Runnable mutationWorker = new Runnable() {
    @Override
    public void run() {
      activeMutationWorkers.incrementAndGet();
      try {
        while (!executorService.isShutdown() && !isShutdown.get()) {
          try {
            Runnable operation =
                asyncOperationsQueue.poll(ASYNC_POLL_WAIT_MS, TimeUnit.MILLISECONDS);
            // The operation can be null if a timeout occurs.
            if (operation == null) {
              break;
            }
            operation.run();
          } catch (InterruptedException e) {
            LOG.info("Interrupted. Shutting down the mutation worker.");
            break;
          } catch (Throwable e) {
            LOG.error("Exception in buffered mutator.", e);
          } finally {
            if (asyncOperationsQueue.isEmpty()) {
              synchronized(operationsComplete) {
                if (!operationsComplete.get() && asyncOperationsQueue.isEmpty()) {
                  operationsComplete.set(true);
                  operationsComplete.notifyAll();
                }
              }
            }
          }
        }
      } finally {
        activeMutationWorkers.decrementAndGet();
      }
    }
  };

  /**
   * <p>
   * Constructor for BigtableBufferedMutator.
   * </p>
   * @param adapter Converts HBase objects to Bigtable protos
   * @param configuration For Additional configuration. TODO: move this to options
   * @param listener Handles exceptions. By default, it just throws the exception.
   * @param session a {@link com.google.cloud.bigtable.grpc.BigtableSession} to get
   *          {@link com.google.cloud.bigtable.config.BigtableOptions},
   *          {@link com.google.cloud.bigtable.grpc.async.AsyncExecutor} and
   *          {@link com.google.cloud.bigtable.grpc.async.BulkMutation} objects from
   * @param asyncRpcExecutorService Optional performance improvement for adapting hbase objects and
   *          starting the async operations on the BigtableDataClient.
   */
  public BigtableBufferedMutator(
      HBaseRequestAdapter adapter,
      Configuration configuration,
      BigtableSession session,
      BufferedMutator.ExceptionListener listener,
      ExecutorService asyncRpcExecutorService) {
    this.adapter = adapter;
    this.configuration = configuration;
    this.exceptionListener = listener;
    BigtableOptions options = session.getOptions();
    this.host = options.getDataHost().toString();
    this.asyncExecutor = session.createAsyncExecutor();
    this.bulkOptions = options.getBulkOptions();
    this.executorService = asyncRpcExecutorService;
    int asyncMutatorCount = options.getBulkOptions().getAsyncMutatorCount();

    // The queue should be limited in size so that way there isn't too large of a queue waiting to
    // be processed. An unlimited queue can eat up a lot of memory, and is an indication of problems
    // in mutationWorker.  2 * the number of workers seems reasonable.
    this.asyncOperationsQueue =
        asyncMutatorCount == 0 ? null : new LinkedBlockingQueue<Runnable>(asyncMutatorCount * 2);
    if (bulkOptions.useBulkApi()) {
      BigtableTableName tableName = this.adapter.getBigtableTableName();
      this.bulkMutation = session.createBulkMutation(tableName, asyncExecutor);
    }
  }

  private void initializeAsyncMutators() {
    if (areNewWorkersRequired()) {
      synchronized (activeMutationWorkers) {
        if (areNewWorkersRequired()) {
          for (int i = activeMutationWorkers.get(); i < bulkOptions.getAsyncMutatorCount(); i++) {
            executorService.submit(mutationWorker);
          }
        }
      }
    }
  }

  protected boolean areNewWorkersRequired() {
    return executorService != null
        && (activeMutationWorkers.get() < bulkOptions.getAsyncMutatorCount());
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    closedWriteLock.lock();
    try {
      flush();
      isShutdown.set(true);
      asyncExecutor.flush();
      closed = true;
    } finally {
      closedWriteLock.unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    while (!operationsComplete.get()) {
      // Make sure that the async mutator workers are running.
      if (asyncOperationsQueue != null && !asyncOperationsQueue.isEmpty()) {
        initializeAsyncMutators();
      }
      synchronized(operationsComplete) {
        try {
          if (!operationsComplete.get()) {
            operationsComplete.wait(250);
          }
        } catch (InterruptedException e) {
          throw new IOException("BigtableBufferedMutator.flush() was interrupted.");
        }
      }
    }

    // If there is a bulk mutation in progress, then send it.
    if (bulkMutation != null) {
      bulkMutation.flush();
    }
    asyncExecutor.flush();
    handleExceptions();
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  /** {@inheritDoc} */
  @Override
  public TableName getName() {
    return this.adapter.getTableName();
  }

  /** {@inheritDoc} */
  @Override
  public long getWriteBufferSize() {
    return this.asyncExecutor.getMaxHeapSize();
  }

  /** {@inheritDoc} */
  @Override
  public void mutate(List<? extends Mutation> mutations) throws IOException {
    closedReadLock.lock();
    try {
      if (closed) {
        throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
      }
      handleExceptions();
      for (Mutation mutation : mutations) {
        offer(mutation);
      }
    } finally {
      closedReadLock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   *
   * Being a Mutation. This method will block if either of the following are true:
   * 1) There are more than {@code maxInflightRpcs} RPCs in flight
   * 2) There are more than {@link #getWriteBufferSize()} bytes pending
   */
  @Override
  public void mutate(final Mutation mutation) throws IOException {
    closedReadLock.lock();
    try {
      if (closed) {
        throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
      }
      handleExceptions();
      offer(mutation);
    } finally {
      closedReadLock.unlock();
    }
  }

  /**
   * Send the operations to the async executor asynchronously.  The conversion from hbase
   * object to cloud bigtable proto and the async call both take time (microseconds worth) that
   * could be parallelized, or at least removed from the user's thread.
   */
  private void offer(Mutation mutation) throws IOException {
    try {
      Runnable runnable;
      if (bulkOptions.useBulkApi() && (mutation instanceof Put || mutation instanceof Delete)) {
        runnable = new BulkMutationOperation(mutation);
      } else {
        long operationId =
            asyncExecutor.getRpcThrottler().registerOperationWithHeapSize(mutation.heapSize());
        runnable = new MutationOperation(mutation, operationId);
      }
      final boolean offerAccepted = offer(runnable, ASYNC_OFFER_WAIT_MS);
      if (!offerAccepted) {
        addGlobalException(mutation, new IllegalStateException(
            "The Bigtable Buffered Mutator asynchronous operations failed."));
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted in buffered mutator while mutating row : '"
          + Bytes.toString(mutation.getRow()), e);
    }
  }

  private boolean offer(Runnable operation, long waitTimeMs) throws InterruptedException {
    if (executorService != null && bulkOptions.getAsyncMutatorCount() > 0) {
      final long totalWaitTimes = waitTimeMs/ASYNC_POLL_WAIT_MS;
      for (int i = 0; i < totalWaitTimes; i++) {
        initializeAsyncMutators();
        boolean offerAccepted =
            asyncOperationsQueue.offer(operation, waitTimeMs, TimeUnit.MILLISECONDS);
        operationsComplete.set(false);
        if (offerAccepted) {
          return true;
        }
      }
    } else {
      operation.run();
      return true;
    }
    return false;
  }

  /**
   * <p>addExceptionCallback.</p>
   *
   * @param future a {@link com.google.common.util.concurrent.ListenableFuture} object.
   * @param mutation a {@link org.apache.hadoop.hbase.client.Mutation} object.
   */
  @SuppressWarnings("unchecked")
  protected void addExceptionCallback(ListenableFuture<?> future,
      Mutation mutation) {
    Futures.addCallback(future, new ExceptionCallback(mutation));
  }

  /**
   * <p>adapt.</p>
   *
   * @param mutation a {@link org.apache.hadoop.hbase.client.Mutation} object.
   * @return a {@link com.google.bigtable.v2.MutateRowRequest} object.
   */
  protected MutateRowRequest adapt(Mutation mutation) {
    if (mutation instanceof Put) {
      return adapter.adapt((Put) mutation);
    } else if (mutation instanceof Delete) {
      return adapter.adapt((Delete) mutation);
    } else {
      throw new IllegalArgumentException(
          "Encountered unknown mutation type: " + mutation.getClass());
    }
  }

  private ListenableFuture<?> issueRequestDetails(Mutation mutation, long operationId) {
    try {
      if (mutation == null) {
        return Futures.immediateFailedFuture(
          new IllegalArgumentException("Cannot perform a mutation on a null object."));
      }
      if (mutation instanceof Put) {
        return asyncExecutor.mutateRowAsync(adapter.adapt((Put) mutation), operationId);
      } else if (mutation instanceof Delete) {
        return asyncExecutor.mutateRowAsync(adapter.adapt((Delete) mutation), operationId);
      } else if (mutation instanceof Increment) {
        return asyncExecutor.readModifyWriteRowAsync(adapter.adapt((Increment) mutation),
          operationId);
      } else if (mutation instanceof Append) {
        return asyncExecutor.readModifyWriteRowAsync(adapter.adapt((Append) mutation), operationId);
      }
      return Futures.immediateFailedFuture(
        new IllegalArgumentException("Encountered unknown mutation type: " + mutation.getClass()));
    } catch (Throwable e) {
      // issueRequest(mutation) could throw an Exception for validation issues. Remove the heapsize
      // and inflight rpc count.
      return Futures.immediateFailedFuture(e);
    }
  }

  private void addGlobalException(Row mutation, Throwable t) {
    synchronized (globalExceptions) {
      globalExceptions.add(new MutationException(mutation, t));
      hasExceptions.set(true);
    }
  }

  /**
   * Create a {@link RetriesExhaustedWithDetailsException} if there were any async exceptions and
   * send it to the {@link org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener}.
   */
  private void handleExceptions() throws RetriesExhaustedWithDetailsException {
    if (hasExceptions.get()) {
      ArrayList<MutationException> mutationExceptions = null;
      synchronized (globalExceptions) {
        hasExceptions.set(false);
        if (globalExceptions.isEmpty()) {
          return;
        }

        mutationExceptions = new ArrayList<>(globalExceptions);
        globalExceptions.clear();
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

  @SuppressWarnings("rawtypes")
  private class ExceptionCallback implements FutureCallback {
    private final Row mutation;

    public ExceptionCallback(Row mutation) {
      this.mutation = mutation;
    }

    @Override
    public void onFailure(Throwable t) {
      addGlobalException(mutation, t);
    }

    @Override
    public void onSuccess(Object ignored) {
    }
  }

  /**
   * <p>hasInflightRequests.</p>
   *
   * @return a boolean.
   */
  public boolean hasInflightRequests() {
    return this.asyncExecutor.hasInflightRequests()
        || (bulkMutation != null && !bulkMutation.isFlushed());
  }
}
