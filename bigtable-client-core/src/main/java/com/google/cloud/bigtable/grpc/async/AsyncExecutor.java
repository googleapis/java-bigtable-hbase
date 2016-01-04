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
import java.util.List;

import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.CheckAndMutateRowResponse;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
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
public class AsyncExecutor {

  // Default rpc count per channel.
  public static final int MAX_INFLIGHT_RPCS_DEFAULT = 50;

  // This is the maximum accumulated size of uncompleted requests that we allow before throttling.
  // Default to 32MB.
  public static final long ASYNC_MUTATOR_MAX_MEMORY_DEFAULT = 16 * 2097152;

  protected static final Logger LOG = new Logger(AsyncExecutor.class);

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

  protected static AsyncCall<ReadRowsRequest, List<Row>> READ_ROWS_ASYNC =
      new AsyncCall<ReadRowsRequest, List<Row>>() {
        @Override
        public ListenableFuture<List<Row>> call(BigtableDataClient client, ReadRowsRequest request) {
          return client.readRowsAsync(request);
        }
      };

  private final BigtableDataClient client;
  private final HeapSizeManager sizeManager;

  public AsyncExecutor(BigtableDataClient client, HeapSizeManager heapSizeManager) {
    this.client = client;
    this.sizeManager = heapSizeManager;
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

  public ListenableFuture<List<com.google.bigtable.v1.Row>> readRowsAsync(ReadRowsRequest request)
      throws InterruptedException {
    return call(READ_ROWS_ASYNC, request);
  }

  private <RequestT extends GeneratedMessage, ResponseT> ListenableFuture<ResponseT> call(
      AsyncCall<RequestT, ResponseT> rpc, RequestT request) throws InterruptedException {
    // Wait until both the memory and rpc count maximum requirements are achieved before getting a
    // unique id used to track this request.
    long id = sizeManager.registerOperationWithHeapSize(request.getSerializedSize());
    ListenableFuture<ResponseT> future;
    try {
      future = rpc.call(client, request);
    } catch (Exception e) {
      future = Futures.immediateFailedFuture(e);
    }
    sizeManager.addCallback(future, id);
    return future;
  }

  public void flush() throws IOException {
    LOG.trace("Flushing");
    try {
      sizeManager.flush();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    LOG.trace("Done flushing");
  }

  public boolean hasInflightRequests() {
    return sizeManager.hasInflightRequests();
  }

  public long getMaxHeapSize() {
    return sizeManager.getMaxHeapSize();
  }
}
