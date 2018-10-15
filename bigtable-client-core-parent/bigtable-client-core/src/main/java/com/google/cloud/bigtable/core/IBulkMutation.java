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

import com.google.api.core.ApiFuture;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Interface to support batching multiple RowMutation request in to singe grpc request.
 */
public interface IBulkMutation {
  long MAX_RPC_WAIT_TIME_NANOS = TimeUnit.MINUTES.toNanos(12);

  /**
   * Send any outstanding {@link MutateRowRequest}s and wait until all requests are complete.
   */
  void flush() throws InterruptedException, TimeoutException;

  void sendUnsent();

  /**
   * @return false if there are any outstanding {@link MutateRowRequest} that still need to be sent.
   */
  boolean isFlushed();

  /**
   * Adds a {@link com.google.cloud.bigtable.data.v2.models.RowMutation} to the underlying IBulkMutation
   * mechanism.
   *
   * @param rowMutation The {@link com.google.cloud.bigtable.data.v2.models.RowMutation} to add
   * @return a {@link ApiFuture} of type {@link Void} will be set when request is
   *     successful otherwise exception will be thrown.
   */
  ApiFuture<Void> add(RowMutation rowMutation);
}
