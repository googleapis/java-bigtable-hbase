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
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.io.IOException;

/**
 * Interface to support batching multiple {@link RowMutation} request into a single grpc request.
 */
@InternalApi("For internal usage only")
public interface IBulkMutation extends AutoCloseable {

  /**
   * Adds a {@link RowMutation} to the underlying IBulkMutation mechanism.
   *
   * @param rowMutation The {@link RowMutation} to add.
   * @return a {@link ApiFuture} of type {@link Void} will be set when request is successful
   *     otherwise exception will be thrown.
   */
  ApiFuture<Void> add(RowMutation rowMutation);

  /** Sends any outstanding {@link RowMutation}s, present in the current batch. */
  void sendUnsent();

  /** Sends any outstanding {@link RowMutation} and wait until all requests are complete. */
  void flush() throws InterruptedException;

  /** @return false if there is any outstanding {@link RowMutation} that still needs to be sent. */
  boolean isFlushed();

  /** Closes this bulk Mutation and prevents from mutating any more elements */
  @Override
  void close() throws IOException;
}
