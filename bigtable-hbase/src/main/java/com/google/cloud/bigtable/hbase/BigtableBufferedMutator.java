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
import java.util.concurrent.Executors;
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

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.HeapSizeManager;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.GeneratedMessage;

/**
 * Bigtable's {@link BufferedMutator} implementation.
 */
public class BigtableBufferedMutator implements BufferedMutator {

  protected static final Logger LOG = new Logger(BigtableBufferedMutator.class);

  protected static final long MUTATION_TO_BE_SENT_WAIT_MS = 1000;

  private static class MutationException {
    private final Row mutation;
    private final Throwable throwable;

    MutationException(Row mutation, Throwable throwable) {
      this.mutation = mutation;
      this.throwable = throwable;
    }
  }

  private static class MutationOperation {
    final Mutation mutation;
    final long operationId;
    final boolean isCloseMarker;

    public MutationOperation(Mutation mutation, long operationId, boolean isClosedMarker) {
      this.mutation = mutation;
      this.operationId = operationId;
      this.isCloseMarker = isClosedMarker;
    }
  }

  private static ExecutorService createDefaultExecutorService() {
    return Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("BigtableBufferedMutator-worker-%s")
                .setDaemon(true)
                .build());
  }

  private final static MutationOperation CLOSE_MARKER = new MutationOperation(null, -1, true);
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

  private final HeapSizeManager heapSizeManager;
  private final AsyncExecutor asyncExecutor;
  private final ExecutorService executorService;
  private final BigtableOptions options;

  private final LinkedBlockingQueue<MutationOperation> mutationsToBeSent =
      new LinkedBlockingQueue<>();

  /**
   * A counter for the number of {@link #mutationWorker}s are
   * active.
   */
  private final AtomicInteger activeMutationWorkers = new AtomicInteger();

  /**
   * This {@link Runnable} pulls a mutation from {@link #mutationsToBeSent}, and calls {{@link
   * #issueRequest(Mutation, long)}.
   */
  private final Runnable mutationWorker = new Runnable() {
    @Override
    public void run() {
      activeMutationWorkers.incrementAndGet();
      try {
        while (!executorService.isShutdown()) {
          Mutation mutation = null;
          try {
            MutationOperation operation =
                mutationsToBeSent.poll(MUTATION_TO_BE_SENT_WAIT_MS, TimeUnit.MILLISECONDS);
            // The operation can be null if a timeout occurs.
            if (operation == null || operation.isCloseMarker) {
              break;
            }
            mutation = operation.mutation;
            ListenableFuture<? extends GeneratedMessage> request =
                issueRequest(operation.mutation, operation.operationId);
            ExceptionCallback callback = new ExceptionCallback(operation.mutation);
            Futures.addCallback(request, callback);
          } catch (InterruptedException e) {
            Thread.interrupted();
            LOG.info("Interrupted. Shutting down mutationRunnable.");
            break;
          } catch (Exception e) {
            if (mutation != null) {
              LOG.error("Exception in buffered mutator. Could not process row: %s", e,
                Bytes.toString(mutation.getRow()));
            } else {
              LOG.error("Exception in buffered mutator.", e);
            }
          }
        }
      } finally {
        activeMutationWorkers.decrementAndGet();
      }
    }
  };

  public BigtableBufferedMutator(
      BigtableDataClient client,
      HBaseRequestAdapter adapter,
      Configuration configuration,
      BigtableOptions options,
      BufferedMutator.ExceptionListener listener,
      HeapSizeManager heapSizeManager) {
    this(client, adapter, configuration, options, listener, heapSizeManager,
        createDefaultExecutorService());
  }

  @VisibleForTesting
  BigtableBufferedMutator(
      BigtableDataClient client,
      HBaseRequestAdapter adapter,
      Configuration configuration,
      BigtableOptions options,
      BufferedMutator.ExceptionListener listener,
      HeapSizeManager heapSizeManager,
      ExecutorService executorService) {
    this.adapter = adapter;
    this.configuration = configuration;
    this.exceptionListener = listener;
    this.host = options.getDataHost().toString();
    this.asyncExecutor = new AsyncExecutor(client, heapSizeManager);
    LOG.info(
        "Initializing BigtableBufferd Mutator with %d channels, %,d byte heap size"
        + " and %d concurrent requests.",
        options.getChannelCount(),
        heapSizeManager.getMaxHeapSize(),
        heapSizeManager.getMaxInFlightRpcs());
    this.options = options;
    this.heapSizeManager = heapSizeManager;
    this.executorService = executorService;
  }

  private void initializeAsyncMutators() {
    if (activeMutationWorkers.get() < options.getAsyncMutatorCount()) {
      synchronized (activeMutationWorkers) {
        for (int i = activeMutationWorkers.get(); i < options.getAsyncMutatorCount(); i++) {
          executorService.submit(mutationWorker);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    closedWriteLock.lock();
    try {
      flush();
      executorService.shutdown();
      int activeWorkerCount = activeMutationWorkers.get();
      for (int i = 0; i < activeWorkerCount; i++) {
        mutationsToBeSent.add(CLOSE_MARKER);
      }
      closed = true;
    } finally {
      closedWriteLock.unlock();
    }
  }

  @Override
  public void flush() throws IOException {
    // Make sure that the async mutator workers are running.
    if (!mutationsToBeSent.isEmpty()) {
      initializeAsyncMutators();
    }
    asyncExecutor.flush();
    handleExceptions();
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public TableName getName() {
    return this.adapter.getTableName();
  }

  @Override
  public long getWriteBufferSize() {
    return this.asyncExecutor.getMaxHeapSize();
  }

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
      initializeAsyncMutators();
      long operationId = heapSizeManager.registerOperationWithHeapSize(mutation.heapSize());
      mutationsToBeSent.add(new MutationOperation(mutation, operationId, false));
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new IOException("Interrupted in buffered mutator while mutating row : '"
          + Bytes.toString(mutation.getRow()), e);
    }
  }

  private ListenableFuture<? extends GeneratedMessage> issueRequest(Mutation mutation,
      long operationId) {
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
    } catch (Exception e) {
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

  private class ExceptionCallback implements FutureCallback<GeneratedMessage> {
    private final Row mutation;

    public ExceptionCallback(Row mutation) {
      this.mutation = mutation;
    }

    @Override
    public void onFailure(Throwable t) {
      addGlobalException(mutation, t);
    }

    @Override
    public void onSuccess(GeneratedMessage ignored) {
    }
  }

  public boolean hasInflightRequests() {
    return this.asyncExecutor.hasInflightRequests();
  }
}
