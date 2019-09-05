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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.OperationAccountant;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.util.ApiFutureUtil;
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
import org.apache.hadoop.hbase.client.RowMutations;

/**
 * A helper for Bigtable's {@link org.apache.hadoop.hbase.client.BufferedMutator} implementations.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableBufferedMutatorHelper {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(BigtableBufferedMutatorHelper.class);

  private final Configuration configuration;

  /**
   * Makes sure that mutations and flushes are safe to proceed. Ensures that while the mutator is
   * closing, there will be no additional writes.
   */
  private final ReentrantReadWriteLock isClosedLock = new ReentrantReadWriteLock();

  private final ReadLock closedReadLock = isClosedLock.readLock();
  private final WriteLock closedWriteLock = isClosedLock.writeLock();

  private boolean closed = false;

  private final HBaseRequestAdapter adapter;
  private final IBulkMutation bulkMutation;
  private final IBigtableDataClient dataClient;
  private final BigtableOptions options;
  private final OperationAccountant operationAccountant;

  /**
   * Constructor for BigtableBufferedMutator.
   *
   * @param adapter Converts HBase objects to Bigtable protos
   * @param configuration For Additional configuration. TODO: move this to options
   * @param session a {@link BigtableSession} object.
   */
  public BigtableBufferedMutatorHelper(
      HBaseRequestAdapter adapter, Configuration configuration, BigtableSession session) {
    this.adapter = adapter;
    this.configuration = configuration;
    this.options = session.getOptions();
    BigtableTableName tableName = this.adapter.getBigtableTableName();
    this.bulkMutation = session.createBulkMutationWrapper(tableName);
    this.dataClient = session.getDataClientWrapper();
    this.operationAccountant = new OperationAccountant();
  }

  public void close() throws IOException {
    closedWriteLock.lock();
    try {
      flush();
      bulkMutation.close();
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
        operationAccountant.awaitCompletion();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("flush() was interrupted", e);
      }
    }
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

  public List<ApiFuture<?>> mutate(List<? extends Mutation> mutations) {
    closedReadLock.lock();
    try {
      if (closed) {
        throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
      }
      List<ApiFuture<?>> futures = new ArrayList<>(mutations.size());
      for (Mutation mutation : mutations) {
        futures.add(offer(mutation));
      }
      return futures;
    } finally {
      closedReadLock.unlock();
    }
  }

  /**
   * Being a Mutation. This method will block if either of the following are true: 1) There are more
   * than {@code maxInflightRpcs} RPCs in flight 2) There are more than {@link
   * #getWriteBufferSize()} bytes pending
   *
   * @param mutation a {@link Mutation} object.
   * @return a {@link ApiFuture} object.
   */
  public ApiFuture<?> mutate(final Mutation mutation) {
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
   * @param mutation a {@link RowMutations} object.
   * @return a {@link ApiFuture} object.
   */
  public ApiFuture<?> mutate(final RowMutations mutation) {
    closedReadLock.lock();
    try {
      if (closed) {
        throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
      }
      if (mutation == null) {
        return ApiFutures.immediateFailedFuture(
            new IllegalArgumentException("Cannot perform a mutation on a null object."));
      } else {
        return bulkMutation.add(adapter.adaptEntry(mutation));
      }
    } finally {
      closedReadLock.unlock();
    }
  }

  /**
   * Send the operations to the async executor asynchronously. The conversion from hbase object to
   * cloud bigtable proto and the async call both take time (microseconds worth) that could be
   * parallelized, or at least removed from the user's thread.
   */
  private ApiFuture<?> offer(Mutation mutation) {
    ApiFuture<?> future = null;
    try {
      if (mutation == null) {
        future =
            ApiFutures.immediateFailedFuture(
                new IllegalArgumentException("Cannot perform a mutation on a null object."));
      } else if (mutation instanceof Put) {
        future = bulkMutation.add(adapter.adaptEntry((Put) mutation));
      } else if (mutation instanceof Delete) {
        future = bulkMutation.add(adapter.adaptEntry((Delete) mutation));
      } else if (mutation instanceof Increment) {
        future = dataClient.readModifyWriteRowAsync(adapter.adapt((Increment) mutation));
        operationAccountant.registerOperation(ApiFutureUtil.adapt(future));
      } else if (mutation instanceof Append) {
        future = dataClient.readModifyWriteRowAsync(adapter.adapt((Append) mutation));
        operationAccountant.registerOperation(ApiFutureUtil.adapt(future));
      } else {
        future =
            ApiFutures.immediateFailedFuture(
                new IllegalArgumentException(
                    "Encountered unknown mutation type: " + mutation.getClass()));
      }
    } catch (Exception e) {
      // issueRequest(mutation) could throw an Exception for validation issues. Remove the heapsize
      // and inflight rpc count.
      future = ApiFutures.immediateFailedFuture(e);
    }
    return future;
  }

  /**
   * hasInflightRequests.
   *
   * @return a boolean.
   */
  public boolean hasInflightRequests() {
    return bulkMutation != null
        && !bulkMutation.isFlushed()
        && operationAccountant.hasInflightOperations();
  }
}
