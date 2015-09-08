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
import java.util.concurrent.atomic.AtomicBoolean;
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

import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.async.HeapSizeManager;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.GeneratedMessage;

/**
 * Bigtable's {@link BufferedMutator} implementation.
 */
// TODO: Cleanup the interface so that @VisibleForTesting can be reduced.
public class BigtableBufferedMutator implements BufferedMutator {

  protected static final Logger LOG = new Logger(BigtableBufferedMutator.class);

  private static class MutationException {
    private final Row mutation;
    private final Throwable throwable;

    MutationException(Row mutation, Throwable throwable) {
      this.mutation = mutation;
      this.throwable = throwable;
    }
  }

  private final Configuration configuration;

  private final HeapSizeManager sizeManager;
  private boolean closed = false;

  /**
   * Makes sure that mutations and flushes are safe to proceed.  Ensures that while the mutator
   * is closing, there will be no additional writes.
   */
  private final ReentrantReadWriteLock mutationLock = new ReentrantReadWriteLock();
  private final BigtableDataClient client;
  private final HBaseRequestAdapter adapter;
  private final ExceptionListener exceptionListener;

  private final AtomicBoolean hasExceptions = new AtomicBoolean(false);
  private final List<MutationException> globalExceptions = new ArrayList<MutationException>();

  private final String host;

  public BigtableBufferedMutator(
      BigtableDataClient client,
      HBaseRequestAdapter adapter,
      Configuration configuration,
      int maxInflightRpcs,
      long maxHeapSize,
      String dataHost,
      BufferedMutator.ExceptionListener listener,
      ExecutorService heapSizeExecutor) {
    this.client = client;
    this.adapter = adapter;
    this.sizeManager = new HeapSizeManager(maxHeapSize, maxInflightRpcs, heapSizeExecutor);
    this.configuration = configuration;
    this.exceptionListener = listener;
    this.host = dataHost;
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
    return this.adapter.getTableName();
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
      synchronized (globalExceptions) {
        // Add the exception to the list of global exceptions and handle the
        // RetriesExhaustedWithDetailsException.
        addGlobalException(mutation, e);
        handleExceptions();
      }
      // The handleExceptions() or may not throw an exception.  Don't continue processing.
      Thread.currentThread().interrupt();
      return;
    }

    ListenableFuture<? extends GeneratedMessage> future = issueRequest(mutation);
    Futures.addCallback(future, new ExceptionCallback(mutation));
    this.sizeManager.addCallback(future, sequenceId);
  }

  private ListenableFuture<? extends GeneratedMessage> issueRequest(final Mutation mutation) {
    try {
      if (mutation == null) {
        return Futures.immediateFailedFuture(new IllegalArgumentException(
            "Cannot perform a mutation on a null object."));
      }
      if (mutation instanceof Put) {
        return client.mutateRowAsync(adapter.adapt((Put) mutation));
      } else if (mutation instanceof Delete) {
        return client.mutateRowAsync(adapter.adapt((Delete) mutation));
      } else if (mutation instanceof Increment) {
        return client.readModifyWriteRowAsync(adapter.adapt((Increment) mutation));
      } else if (mutation instanceof Append) {
        return client.readModifyWriteRowAsync(adapter.adapt((Append) mutation));
      } 
      return Futures.immediateFailedFuture(new IllegalArgumentException(
          "Encountered unknown mutation type: " + mutation.getClass()));
    } catch (RuntimeException e) {
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

  @VisibleForTesting
  public boolean hasInflightRequests() {
    return sizeManager.hasInflightRequests();
  }
}
