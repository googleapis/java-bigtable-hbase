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

import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Interface to wrap {@link com.google.cloud.bigtable.grpc.BigtableDataClient} with
 * Google-Cloud-java's models.
 */
public interface IBigtableDataClient {

  /**
   * Mutate a row atomically.
   *
   * @param rowMutation a {@link RowMutation} model object.
   */
  void mutateRow(RowMutation rowMutation);

  /**
   * Mutate a row atomically.
   *
   * @param rowMutation a {@link RowMutation} model object.
   * @return a {@link ListenableFuture} of type {@link Void} will be set when request is
   *     successful otherwise exception will be thrown.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Void> mutateRowAsync(RowMutation rowMutation);

  /**
   * Perform an atomic read-modify-write operation on a row.
   *
   * @param readModifyWriteRow a {@link ReadModifyWriteRow} model object.
   * @return {@link Row} a modified row.
   */
  Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow);

  /**
   * Perform an atomic read-modify-write operation on a row.
   *
   * @param readModifyWriteRow a {@link ReadModifyWriteRow} model object.
   * @return a {@link ListenableFuture} of type {@link Row} will be set when request is
   *     successful otherwise exception will be thrown.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow);

  /**
   * Creates {@link IBulkMutation} batcher.
   */
  IBulkMutation createBulkMutationBatcher();

  /**
   * Mutate a row atomically dependent on a precondition.
   *
   * @param conditionalRowMutation a {@link ConditionalRowMutation} model object.
   * @return a {@link ListenableFuture} of type {@link Boolean} will be set when request is
   *     successful otherwise exception will be thrown.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation);

  /**
   * Mutate a row atomically dependent on a precondition.
   *
   * @param conditionalRowMutation a {@link ConditionalRowMutation} model object.
   * @return returns true if predicate returns any result.
   */
  Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation);

  /**
   * Sample row keys from a table.
   *
   * @param tableId a String object.
   * @return an immutable {@link List} object.
   */
  List<KeyOffset> sampleRowKeys(String tableId);

  /**
   * Sample row keys from a table, returning a Future that will complete when the sampling has
   * completed.
   *
   * @param tableId a String object.
   * @return a {@link ListenableFuture} object.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture< List<KeyOffset>> sampleRowKeysAsync(String tableId);

  /**
   * Perform a scan over {@link Row}s, in key order.
   *
   * @param request a {@link Query} object.
   * @return a {@link Row} object.
   */
  ResultScanner<Row> readRows(Query request);

  /**
   * Read multiple {@link Row}s into an in-memory list, in key order.
   *
   * @return a {@link ListenableFuture} that will finish when
   * all reads have completed.
   * @param request a {@link Query} object.
   */
  ListenableFuture<List<Row>> readRowsAsync(Query request);

  /**
   * Returns a list of {@link FlatRow}s, in key order.
   *
   * @param request a {@link Query} object.
   * @return a List with {@link FlatRow}s.
   */
  List<FlatRow> readFlatRowsList(Query request);

  /**
   * Perform a scan over {@link FlatRow}s, in key order.
   *
   * @param request a {@link Query} object.
   * @return a {@link ResultScanner} object.
   */
  ResultScanner<FlatRow> readFlatRows(Query request);

  /**
   * Read multiple {@link FlatRow}s into an in-memory list, in key order.
   *
   * @return a {@link ListenableFuture} that will finish when
   * all reads have completed.
   * @param request a {@link Query} object.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<List<FlatRow>> readFlatRowsAsync(Query request);

  /**
   * Read {@link FlatRow} asynchronously, and pass them to a stream observer to be processed.
   *
   * @param request  a {@link Query} object.
   * @param observer a {@link StreamObserver} object.
   */
  void readFlatRowsAsync(Query request, StreamObserver<FlatRow> observer);
}
