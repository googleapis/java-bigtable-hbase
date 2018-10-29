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
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Interface to access Bigtable data service api.
 */
public interface IBigtableDataClient {

  /**
   * Mutate a row atomically.
   *
   * @param rowMutation a {@link RowMutation} model object.
   * @throws ExecutionException if any.
   * @throws InterruptedException if any.
   */
  void mutateRow(RowMutation rowMutation) throws ExecutionException, InterruptedException;

  /**
   * Mutate a row atomically.
   *
   * @param rowMutation a {@link RowMutation} model object.
   * @return a {@link ApiFuture} of type {@link Void} will be set when request is
   *     successful otherwise exception will be thrown.
   * @throws InterruptedException if any.
   */
  ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) throws InterruptedException;

  /**
   * Perform an atomic read-modify-write operation on a row.
   *
   * @param readModifyWriteRow a {@link ReadModifyWriteRow} model object.
   * @return Row a modified row.
   * @throws ExecutionException if any.
   * @throws InterruptedException if any.
   */
  Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow)
          throws ExecutionException, InterruptedException;

  /**
   * Perform an atomic read-modify-write operation on a row.
   *
   * @param readModifyWriteRow a {@link ReadModifyWriteRow} model object.
   * @return a {@link ApiFuture} of type {@link Row} will be set when request is
   *     successful otherwise exception will be thrown.
   * @throws InterruptedException if any.
   */
  ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) throws InterruptedException;

  /**
   * Creates BulMutation batcher.
   */
  IBulkMutation createBulkMutationBatcher();

  /**
   * Mutate a row atomically dependent on a precondition.
   *
   * @param conditionalRowMutation a {@link ConditionalRowMutation} model object.
   * @return a {@link ApiFuture} of type {@link Boolean} will be set when request is
   *     successful otherwise exception will be thrown.
   */
  ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation);

  /**
   * Mutate a row atomically dependent on a precondition.
   *
   * @param conditionalRowMutation a {@link ConditionalRowMutation} model object.
   * @return Boolean returns true if predicate returns any result.
   * @throws ExecutionException if any.
   * @throws InterruptedException if any.
   */
  Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation)
          throws ExecutionException, InterruptedException;

  /**
   * Read a single row. If the row does not exist, the value returned will be null.
   *
   * @param tableId a {@link String} object
   * @param rowKey a {@link ByteString} object
   * @return a {@link Row} object. A single row.
   */
  Row readRow(String tableId, ByteString rowKey);

  /**
   * Read a single row. If the row does not exist, the value returned will be null.
   *
   * @param tableId a {@link String} object
   * @param rowKey a {@link String} object
   * @return a {@link Row} object. A single row.
   */
  Row readRow(String tableId, String rowKey);

  /**
   * Read a single row asynchronously. If the row does not exist, the future's value will be null
   *
   * @param tableId a {@link String} object
   * @param rowKey a {@link ByteString} object
   * @return a {@link ApiFuture} of type {@link Row}
   */
  ApiFuture<Row> readRowAsync(String tableId, ByteString rowKey);

  /**
   * Read a single row asynchronously. If the row does not exist, the future's value will be null
   *
   * @param tableId a {@link String} object
   * @param rowKey a {@link String} object
   * @return a {@link ApiFuture} of type {@link Row}
   */
  ApiFuture<Row> readRowAsync(String tableId, String rowKey);

  /**
   * Convenience method for synchronously streaming the results of a Query.
   *
   * @param query a {@link Query} object.
   * @return a {@link ServerStream} of type {@link Row}
   */
  ServerStream<Row> readRows(Query query);

  /**
   * Convenience method for asynchronously streaming the results of a Query.
   *
   * @param query a {@link Query} object.
   * @param observer a {@link ResponseObserver} of type {@link Row}
   */
  void readRowsAsync(Query query, ResponseObserver<Row> observer);

  /**
   * Convenience method to synchronously return a sample of row keys in the table.
   * The returned row keys will delimit contiguous sections of the table of approximately equal size,
   * which can be used to break up the data for distributed tasks like mapreduces.
   *
   * @param tableId a {@link String} object
   * @return a {@link List} of type {@link KeyOffset}
   */
  List<KeyOffset> sampleRowKeys(String tableId);

  /**
   * Convenience method to synchronously return a sample of row keys in the table.
   * The returned row keys will delimit contiguous sections of the table of approximately equal size,
   * which can be used to break up the data for distributed tasks like mapreduces.
   *
   * @param tableId a {@link String} object
   * @return a {@link ApiFuture} of type {@link List} of type {@link KeyOffset}
   */
  ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId);
}
