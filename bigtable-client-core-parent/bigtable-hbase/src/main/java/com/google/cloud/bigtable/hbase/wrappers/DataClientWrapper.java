/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * Common API surface for data operation.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public interface DataClientWrapper extends AutoCloseable {

  /** Creates instance of bulkMutation with specified table ID. */
  BulkMutationWrapper createBulkMutation(String tableId);

  BulkMutationWrapper createBulkMutation(String tableId, long closeTimeoutMilliseconds);

  /**
   * Creates {@link BulkReadWrapper} with specified table ID.
   *
   * <p>The BulkRead instance will be scoped to a single user visible operation. The operation
   * timeout (which is configured in the settings) is started from the time the createBulkRead is
   * invoked.
   *
   * <pre>{@code
   * try (BulkReadWrapper batch = wrapper.createBulkRead(tableId)) {
   *   batch.add(key1, filter1);
   *   batch.add(key2, filter2);
   * }
   * }</pre>
   */
  BulkReadWrapper createBulkRead(String tableId);

  /** Mutate a row atomically. */
  ApiFuture<Void> mutateRowAsync(RowMutation rowMutation);

  /** Perform an atomic read-modify-write operation on a row. */
  ApiFuture<Result> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow);

  /** Mutate a row atomically dependent on a precondition. */
  ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation);

  /**
   * Sample row keys from a table, returning a Future that will complete when the sampling has
   * completed.
   */
  ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId);

  /** Reads a single row based on filter, If row not found then returns an empty {@link Result}. */
  ApiFuture<Result> readRowAsync(
      String tableId, ByteString rowKey, @Nullable Filters.Filter filter);

  /** Perform a scan over {@link Result}s, in key order. */
  ResultScanner readRows(Query request);

  /** Read multiple {@link Result}s into an in-memory list, in key order. */
  ApiFuture<List<Result>> readRowsAsync(Query request);

  /** Read {@link Result} asynchronously, and pass them to a stream observer to be processed. */
  // TODO: once veneer is implemented update this with gax's ResponseObserver.
  void readRowsAsync(Query request, StreamObserver<Result> observer);

  @Override
  void close() throws IOException;

  ResultScanner readRows(Query.QueryPaginator paginator);
}
