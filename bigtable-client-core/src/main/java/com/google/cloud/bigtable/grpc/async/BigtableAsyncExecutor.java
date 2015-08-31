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


import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.CheckAndMutateRowResponse;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessage;

/**
 * Performs asynchronous mutations and reads w
 */
// TODO: Cleanup the interface so that @VisibleForTesting can be reduced.
public class BigtableAsyncExecutor implements Closeable {

  protected static final Logger LOG = new Logger(BigtableAsyncExecutor.class);

  // Flush is not properly synchronized with respect to waiting. It will never exit
  // improperly, but it might wait more than it has to. Setting this to a low value ensures
  // that improper waiting is minimal.
  private static final long WAIT_MILLIS = 250;

  // In flush, wait up to this number of milliseconds without any operations completing.  If
  // this amount of time goes by without any updates, flush will log a warning.  Flush()
  // will still wait to complete.
  private static final long INTERVAL_NO_SUCCESS_WARNING = 300000;

  public interface Callback<ReqT, RespT> {
    void onSuccess(ReqT request, RespT response);
    void onFailure(ReqT request, Throwable t);
  }

  protected final ExecutorService heapSizeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder()
        .setNameFormat("heapSize-async-%s")
        .setDaemon(true)
        .build());

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

    HeapSizeManager(long maxHeapSize, int maxInflightRpcs) {
      this.maxHeapSize = maxHeapSize;
      this.maxInFlightRpcs = maxInflightRpcs;
    }

    private long getMaxHeapSize() {
      return maxHeapSize;
    }

    private synchronized void waitUntilAllOperationsAreDone() throws InterruptedException {
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

    private synchronized long registerOperationWithHeapSize(long heapSize)
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

    @VisibleForTesting
    synchronized void operationComplete(long operationSequenceId) {
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

    private synchronized boolean hasInflightRequests() {
      return !pendingOperationsWithSize.isEmpty();
    }

    @VisibleForTesting
    synchronized long getHeapSize() {
      return currentWriteBufferSize;
    }
  }

  @VisibleForTesting
  final HeapSizeManager sizeManager;
  private boolean closed = false;

  /**
   * Makes sure that mutations and flushes are safe to proceed.  Ensures that while the mutator
   * is closing, there will be no additional writes.
   */
  private final ReentrantReadWriteLock rpcLock = new ReentrantReadWriteLock();
  private final BigtableDataClient client;
  private final Executor executor;

  public BigtableAsyncExecutor(int maxInflightRpcs, long maxHeapSize, BigtableDataClient client,
      Executor executor) {
    this.sizeManager = new HeapSizeManager(maxHeapSize, maxInflightRpcs);
    this.client = client;
    this.executor = executor;
  }

  @Override
  public void close() throws IOException {
    WriteLock lock = rpcLock.writeLock();
    lock.lock();
    try {
      if (!closed) {
        closed = true;
        doFlush();
        heapSizeExecutor.shutdown();
      }
    } finally {
      lock.unlock();
    }
  }

  public void flush() throws IOException {
    WriteLock lock = rpcLock.writeLock();
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
  }

  public long getWriteBufferSize() {
    return this.sizeManager.getMaxHeapSize();
  }

  /**
   * Being a Mutation. This method will block if either of the following are true:
   * 1) There are more than {@code maxInflightRpcs} RPCs in flight
   * 2) There are more than {@link #getWriteBufferSize()} bytes pending
   */
  public ListenableFuture<Empty> mutateRowAsync(MutateRowRequest request) {
    FutureCallback<Empty> callback = register(request);
    ListenableFuture<Empty> future = client.mutateRowAsync(request);
    Futures.addCallback(future, callback, executor);
    return future;
  }

  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) {
    FutureCallback<CheckAndMutateRowResponse> callback = register(request);
    ListenableFuture<CheckAndMutateRowResponse> future = client.checkAndMutateRowAsync(request);
    Futures.addCallback(future, callback, executor);
    return future;
  }

  public ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRowRequest request) {
    FutureCallback<Row> callback = register(request);
    ListenableFuture<Row> future = client.readModifyWriteRowAsync(request);
    Futures.addCallback(future, callback, executor);
    return future;
  }

  public ListenableFuture<List<com.google.bigtable.v1.Row>> readRowsAsync(ReadRowsRequest request) {
    FutureCallback<List<Row>> callback = register(request);
    ListenableFuture<List<Row>> future = client.readRowsAsync(request);
    Futures.addCallback(future, callback, executor);
    return future;
  }

  private <T> AccountingFutureCallback<T> register(GeneratedMessage request) {
    long id;
    ReadLock readLock = rpcLock.readLock();
    readLock.lock();
    try {
      // registerOperationWithHeapSize() waits until both the memory and rpc count maximum
      // requirements are achieved.
      id = sizeManager.registerOperationWithHeapSize(request.getSerializedSize());
    } catch (InterruptedException e) {
      // The handleExceptions() or may not throw an exception. Don't continue processing.
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while trying to perform a batch operation", e);
    } finally {
      readLock.unlock();
    }
    return new AccountingFutureCallback<T>(id);
  }

  private class AccountingFutureCallback<T> implements FutureCallback<T> {
    private final long operationSequenceId;

    public AccountingFutureCallback(long operationSequenceId) {
      this.operationSequenceId = operationSequenceId;
    }

    @Override
    public void onSuccess(T result) {
      sizeManager.operationComplete(operationSequenceId);
    }

    @Override
    public void onFailure(Throwable t) {
      sizeManager.operationComplete(operationSequenceId);
    }
  }

  public boolean hasInflightRequests() {
    return sizeManager.hasInflightRequests();
  }
}
