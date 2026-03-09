/*
 * Copyright 2018 Google LLC
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
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.util.OperationAccountant;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import java.io.IOException;
import java.time.Duration;
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
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableBufferedMutatorHelper {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(BigtableBufferedMutatorHelper.class);

  /**
   * Makes sure that mutations and flushes are safe to proceed. Ensures that while the mutator is
   * closing, there will be no additional writes.
   */
  private final ReentrantReadWriteLock isClosedLock = new ReentrantReadWriteLock();

  private final ReadLock closedReadLock = isClosedLock.readLock();
  private final WriteLock closedWriteLock = isClosedLock.writeLock();

  private boolean closed = false;

  private final HBaseRequestAdapter adapter;
  private final BulkMutationWrapper bulkMutation;
  private final DataClientWrapper dataClient;
  private final BigtableHBaseSettings settings;
  private final OperationAccountant operationAccountant;

  /**
   * Constructor for BigtableBufferedMutatorHelper.
   *
   * @param bigtableApi a {@link BigtableApi} object to access bigtable data client.
   * @param settings a {@link BigtableHBaseSettings} object for bigtable settings
   * @param adapter a {@link HBaseRequestAdapter} object to convert HBase objects to Bigtable protos
   */
  public BigtableBufferedMutatorHelper(
      BigtableApi bigtableApi, BigtableHBaseSettings settings, HBaseRequestAdapter adapter) {
    this.adapter = adapter;
    this.settings = settings;
    this.dataClient = bigtableApi.getDataClient();
    this.bulkMutation =
        dataClient.createBulkMutation(
            this.adapter.getTableId(), settings.getBulkMutationCloseTimeoutMilliseconds());
    this.operationAccountant = new OperationAccountant();
  }

  public void close() throws IOException {
    closedWriteLock.lock();
    try {
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
    return this.settings.getConfiguration();
  }

  public TableName getName() {
    return this.adapter.getTableName();
  }

  public long getWriteBufferSize() {
    return this.settings.getBatchingMaxRequestSize();
  }

  public int getMaxRowKeyCount() {
    return this.settings.getBulkMaxRowCount();
  }

  public Duration getAutoFlushInterval() {
    return this.settings.getAutoFlushInterval();
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
        operationAccountant.registerOperation(future);
      } else if (mutation instanceof Append) {
        future = dataClient.readModifyWriteRowAsync(adapter.adapt((Append) mutation));
        operationAccountant.registerOperation(future);
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
}
