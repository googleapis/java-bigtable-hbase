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

import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * This class provides management of asynchronous Bigtable RPCs. It ensures that there aren't too
 * many concurrent, in flight asynchronous RPCs and also makes sure that the memory used by the
 * requests doesn't exceed a threshold.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class AsyncExecutor {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(AsyncExecutor.class);

  private final BigtableDataClient client;
  private final OperationAccountant operationsAccountant;

  /**
   * <p>
   * Constructor for AsyncExecutor.
   * </p>
   * @param client a {@link com.google.cloud.bigtable.grpc.BigtableDataClient} object for executing
   *          RPCs.
   */
  public AsyncExecutor(BigtableDataClient client) {
    this(client, new OperationAccountant());
  }

  /**
   * <p>
   * Constructor for AsyncExecutor.
   * </p>
   * @param client a {@link com.google.cloud.bigtable.grpc.BigtableDataClient} object for executing
   *          RPCs.
   * @param operationAccountant a {@link com.google.cloud.bigtable.grpc.async.OperationAccountant}
   *          object for tracking the RPCs initiated by this instance.
   */
  @VisibleForTesting
  AsyncExecutor(BigtableDataClient client, OperationAccountant operationAccountant) {
    this.client = client;
    this.operationsAccountant = operationAccountant;
  }

  /**
   * Performs a {@link com.google.cloud.bigtable.grpc.BigtableDataClient#readModifyWriteRow(ReadModifyWriteRowRequest)} on the
   * {@link com.google.bigtable.v2.ReadModifyWriteRowRequest}. This method may block if
   * {@link OperationAccountant#registerOperation(ListenableFuture)} blocks.
   *
   * @param request The {@link com.google.bigtable.v2.ReadModifyWriteRowRequest} to send.
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} which can be listened to for completion events.
   */
  public ListenableFuture<ReadModifyWriteRowResponse>
      readModifyWriteRowAsync(ReadModifyWriteRowRequest request) {
    // Wait until both the memory and rpc count maximum requirements are achieved before getting a
    // unique id used to track this request.
    ListenableFuture<ReadModifyWriteRowResponse> future;
    try {
      future = client.readModifyWriteRowAsync(request);
    } catch (Throwable e) {
      future = Futures.immediateFailedFuture(e);
    }
    operationsAccountant.registerOperation(future);
    return future;
  }

  /**
   * Waits until all operations managed by the
   * {@link com.google.cloud.bigtable.grpc.async.OperationAccountant} complete. See
   * {@link com.google.cloud.bigtable.grpc.async.OperationAccountant#awaitCompletion()} for more
   * information.
   * @throws java.io.IOException if something goes wrong.
   */
  public void flush() throws IOException {
    LOG.trace("Flushing");
    try {
      operationsAccountant.awaitCompletion();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Batch operations were interrupted.");
    }
    LOG.trace("Done flushing");
  }

  /**
   * <p>
   * hasInflightRequests.
   * </p>
   * @return a boolean.
   */
  public boolean hasInflightRequests() {
    return operationsAccountant.hasInflightOperations();
  }

}
