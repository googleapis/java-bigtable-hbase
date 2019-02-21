/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.core;

import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import com.google.cloud.bigtable.grpc.async.OperationAccountant;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface to support batching multiple RowMutation request into a single grpc request.
 */
public interface IBulkMutation {

  /**
   * Send any outstanding {@link RowMutation} and wait until all requests are complete.
   */
  void flush() throws InterruptedException;

  void sendUnsent();

  /**
   * @return false if there is any outstanding {@link RowMutation} that still needs to be sent.
   */
  boolean isFlushed();

  /**
   * Adds a {@link RowMutation} to the underlying IBulkMutation mechanism.
   *
   * @param rowMutation The {@link RowMutation} to add.
   * @return a {@link ListenableFuture} of type {@link Void} will be set when request is
   *     successful otherwise exception will be thrown.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Void> add(RowMutation rowMutation);

  /**
   * Performs a {@link IBigtableDataClient#readModifyWriteRowAsync(ReadModifyWriteRow)} on the
   * {@link ReadModifyWriteRow}. This method may block if
   * {@link OperationAccountant#registerOperation(ListenableFuture)} blocks.
   *
   * @param request The {@link ReadModifyWriteRow} to send.
   * @return a {@link ListenableFuture} which can be listened to for completion events.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Row> readModifyWrite(ReadModifyWriteRow request);
}
