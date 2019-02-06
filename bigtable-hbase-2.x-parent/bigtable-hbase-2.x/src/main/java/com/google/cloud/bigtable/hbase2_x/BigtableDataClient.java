/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase2_x;

import static com.google.cloud.bigtable.hbase2_x.FutureUtils.toCompletableFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;

/**
 * Interface to access v2 Bigtable data service methods.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableDataClient {

  private final com.google.cloud.bigtable.grpc.BigtableDataClient dataClient;
  private final IBigtableDataClient clientWrapper;

  public BigtableDataClient(com.google.cloud.bigtable.grpc.BigtableDataClient dataClient,
      IBigtableDataClient clientWrapper) {
    this.dataClient = dataClient;
    this.clientWrapper = clientWrapper;
  }

  /**
   * Mutate a row atomically.
   *
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} that will finish when
   * the mutation has completed.
   * @param request a {@link com.google.bigtable.v2.MutateRowRequest} object.
   */
  public CompletableFuture<Void> mutateRowAsync(RowMutation request) {
    return toCompletableFuture(clientWrapper.mutateRowAsync(request));
  }

  /**
   * Mutate a row atomically dependent on a precondition.
   *
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} that will finish when
   * the mutation has completed.
   * @param request a {@link com.google.bigtable.v2.CheckAndMutateRowRequest} object.
   */
  public CompletableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request){
    return toCompletableFuture(dataClient.checkAndMutateRowAsync(request));
  }

  /**
   * Perform an atomic read-modify-write operation on a row,
   *
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} that will finish when
   * the mutation has completed.
   * @param request a {@link com.google.bigtable.v2.ReadModifyWriteRowRequest} object.
   */
  public CompletableFuture<ReadModifyWriteRowResponse>
      readModifyWriteRowAsync(ReadModifyWriteRowRequest request){
    return toCompletableFuture(dataClient.readModifyWriteRowAsync(request));
  }

  /**
   * Read multiple {@link FlatRow}s into an in-memory list, in key order.
   *
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} that will finish when
   * all reads have completed.
   * @param request a {@link com.google.bigtable.v2.ReadRowsRequest} object.
   */
  public CompletableFuture<List<FlatRow>> readFlatRowsAsync(ReadRowsRequest request) {
    return toCompletableFuture(dataClient.readFlatRowsAsync(request));
  }

  public com.google.cloud.bigtable.grpc.BigtableDataClient getClient() {
    return dataClient;
  }
}
