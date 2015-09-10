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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.CheckAndMutateRowResponse;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessage;

/**
 * This class provides management of asynchronous Bigtable RPCs.  It ensures that there aren't too
 * many concurrent, in flight asynchronous RPCs and also makes sure that the memory used by the
 * requests doesn't exceed a threshold.
 */
public class AsyncMutator {

  // Default rpc count per channel.
  public static final int MAX_INFLIGHT_RPCS_DEFAULT = 50;

  // This is the maximum accumulated size of uncompleted requests that we allow before throttling.
  // Default to 32MB.
  public static final long ASYNC_MUTATOR_MAX_MEMORY_DEFAULT = 16 * 2097152;

  protected static final Logger LOG = new Logger(AsyncMutator.class);

  protected interface AsyncCall<RequestT, ResponseT> {
    ListenableFuture<ResponseT> call(BigtableDataClient client, RequestT request);
  }

  protected static AsyncCall<MutateRowRequest, Empty> MUTATE_ASYNC =
      new AsyncCall<MutateRowRequest, Empty>() {
        @Override
        public ListenableFuture<Empty> call(BigtableDataClient client, MutateRowRequest request) {
          return client.mutateRowAsync(request);
        }
      };

  protected static AsyncCall<ReadModifyWriteRowRequest, Row> READ_MODIFY_WRITE_ASYNC =
      new AsyncCall<ReadModifyWriteRowRequest, Row>() {
        @Override
        public ListenableFuture<Row> call(BigtableDataClient client,
            ReadModifyWriteRowRequest request) {
          return client.readModifyWriteRowAsync(request);
        }
      };

  protected static AsyncCall<CheckAndMutateRowRequest, CheckAndMutateRowResponse> CHECK_AND_MUTATE_ASYNC =
      new AsyncCall<CheckAndMutateRowRequest, CheckAndMutateRowResponse>() {
        @Override
        public ListenableFuture<CheckAndMutateRowResponse> call(BigtableDataClient client,
            CheckAndMutateRowRequest request) {
          return client.checkAndMutateRowAsync(request);
        }
      };

  /**
   * Makes sure that mutations and flushes are safe to proceed.  Ensures that while the mutator
   * is closing, there will be no additional writes.
   */
  private final ReentrantReadWriteLock mutationLock = new ReentrantReadWriteLock();
  private final BigtableDataClient client;
  private final HeapSizeManager sizeManager;

  public AsyncMutator(
      BigtableDataClient client,
      int maxInflightRpcs,
      long maxHeapSize,
      ExecutorService heapSizeExecutor) {
    this.client = client;
    this.sizeManager = new HeapSizeManager(maxHeapSize, maxInflightRpcs, heapSizeExecutor);
  }

  public ListenableFuture<Empty> mutateRowAsync(MutateRowRequest request)
      throws InterruptedException {
    return call(MUTATE_ASYNC, request);
  }

  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) throws InterruptedException {
    return call(CHECK_AND_MUTATE_ASYNC, request);
  }

  public ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRowRequest request)
      throws InterruptedException {
    return call(READ_MODIFY_WRITE_ASYNC, request);
  }

  private <RequestT extends GeneratedMessage, ResponseT> ListenableFuture<ResponseT> call(
      AsyncCall<RequestT, ResponseT> rpc, RequestT request) throws InterruptedException {
    long id = register(request);
    ListenableFuture<ResponseT> future = getFuture(rpc, request);
    sizeManager.addCallback(future, id);
    return future;
  }


  /**
   * Wait until both the memory and rpc count maximum requirements are achieved.  Returns a unique
   * id used to track this request
   */
  private long register(GeneratedMessage request) throws InterruptedException {
    ReadLock lock = mutationLock.readLock();
    lock.lock();
    try {
      return sizeManager.registerOperationWithHeapSize(request.getSerializedSize());
    } finally {
      lock.unlock();
    }
  }

  private <ResponseT, RequestT extends GeneratedMessage> ListenableFuture<ResponseT> getFuture(
      AsyncCall<RequestT, ResponseT> rpc, RequestT request) {
    try {
      return rpc.call(client, request);
    } catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  public void flush() throws IOException {
    WriteLock lock = mutationLock.writeLock();
    lock.lock();
    try {
      LOG.trace("Flushing");
      try {
        sizeManager.waitUntilAllOperationsAreDone();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      LOG.trace("Done flushing");
    } finally {
      lock.unlock();
    }
  }

  public boolean hasInflightRequests() {
    return sizeManager.hasInflightRequests();
  }

  public long getMaxHeapSize() {
    return sizeManager.getMaxHeapSize();
  }
}
