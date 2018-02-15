/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A helper for Bigtable's {@link org.apache.hadoop.hbase.client.BufferedMutator} implementations.
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableBufferedMutatorHelper {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(BigtableBufferedMutatorHelper.class);

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
  private final AsyncExecutor asyncExecutor;

  private BulkMutation bulkMutation = null;

  private BigtableOptions options;

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
   *          {@link com.google.cloud.bigtable.grpc.async.BulkMutation} objects from starting the
   *          async operations on the BigtableDataClient.
   */
  public BigtableBufferedMutatorHelper(
      HBaseRequestAdapter adapter,
      Configuration configuration,
      BigtableSession session) {
    this.adapter = adapter;
    this.configuration = configuration;
    this.options = session.getOptions();
    this.asyncExecutor = session.createAsyncExecutor();
    BigtableTableName tableName = this.adapter.getBigtableTableName();
    this.bulkMutation = session.createBulkMutation(tableName);
  }

  public void close() throws IOException {
    closedWriteLock.lock();
    try {
      flush();
      asyncExecutor.flush();
      closed = true;
    } finally {
      closedWriteLock.unlock();
    }
  }

  public void flush() throws IOException {
    // If there is a bulk mutation in progress, then send it.
    if (bulkMutation != null) {
      try {
        bulkMutation.flush();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("flush() was interrupted", e);
      }
    }
    asyncExecutor.flush();
  }

  public void sendUnsent() {
    if (bulkMutation != null) {
      bulkMutation.sendUnsent();
    }
  }

  public Configuration getConfiguration() {
    return this.configuration;
  }

  public TableName getName() {
    return this.adapter.getTableName();
  }

  public long getWriteBufferSize() {
    return this.options.getBulkOptions().getMaxMemory();
  }

  public List<ListenableFuture<?>> mutate(List<? extends Mutation> mutations) {
    closedReadLock.lock();
    try {
      List<ListenableFuture<?>> futures = new ArrayList<>(mutations.size());
      for (Mutation mutation : mutations) {
        futures.add(offer(mutation));
      }
      return futures;
    } finally {
      closedReadLock.unlock();
    }
  }

  /**
   * Being a Mutation. This method will block if either of the following are true:
   * 1) There are more than {@code maxInflightRpcs} RPCs in flight
   * 2) There are more than {@link #getWriteBufferSize()} bytes pending
   * @return 
   */
  public ListenableFuture<?> mutate(final Mutation mutation) {
    closedReadLock.lock();
    try {
      if (closed) {
        throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
      }
      return offer(mutation);
    } finally {
      closedReadLock.unlock();
    }
  }

  /**
   * Send the operations to the async executor asynchronously.  The conversion from hbase
   * object to cloud bigtable proto and the async call both take time (microseconds worth) that
   * could be parallelized, or at least removed from the user's thread.
   */
  private ListenableFuture<?> offer(Mutation mutation) {
    if (closed) {
      Futures.immediateFailedFuture(
        new IllegalStateException("Cannot mutate when the BufferedMutator is closed."));
    }
    ListenableFuture<?> future = null;
    try {
      if (mutation == null) {
        future = Futures.immediateFailedFuture(
          new IllegalArgumentException("Cannot perform a mutation on a null object."));
      } else if (mutation instanceof Put) {
        future = bulkMutation.add(adapter.adaptEntry((Put) mutation));
      } else if (mutation instanceof Delete) {
        future = bulkMutation.add(adapter.adaptEntry((Delete) mutation));
      } else if (mutation instanceof Increment) {
        future = asyncExecutor.readModifyWriteRowAsync(adapter.adapt((Increment) mutation));
      } else if (mutation instanceof Append) {
        future = asyncExecutor.readModifyWriteRowAsync(adapter.adapt((Append) mutation));
      } else {
        future = Futures.immediateFailedFuture(new IllegalArgumentException(
            "Encountered unknown mutation type: " + mutation.getClass()));
      }
    } catch (Exception e) {
      // issueRequest(mutation) could throw an Exception for validation issues. Remove the heapsize
      // and inflight rpc count.
      future = Futures.immediateFailedFuture(e);
    }
    return future;
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
