/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.veneer.SharedDataClientWrapperFactory.Key;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/** Simple wrapper around a {@link DataClientWrapper} that will release resources on close. */
class SharedDataClientWrapper implements DataClientWrapper {
  private final SharedDataClientWrapperFactory owner;
  private final SharedDataClientWrapperFactory.Key key;
  private final DataClientWrapper delegate;

  public SharedDataClientWrapper(
      SharedDataClientWrapperFactory owner, Key key, DataClientWrapper delegate) {
    this.owner = owner;
    this.key = key;
    this.delegate = delegate;
  }

  @Override
  public BulkMutationWrapper createBulkMutation(String tableId) {
    return delegate.createBulkMutation(tableId);
  }

  @Override
  public BulkMutationWrapper createBulkMutation(String tableId, long closeTimeoutMilliseconds) {
    return delegate.createBulkMutation(tableId, closeTimeoutMilliseconds);
  }

  @Override
  public BulkReadWrapper createBulkRead(String tableId) {
    return delegate.createBulkRead(tableId);
  }

  @Override
  public ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    return delegate.mutateRowAsync(rowMutation);
  }

  @Override
  public ApiFuture<Result> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    return delegate.readModifyWriteRowAsync(readModifyWriteRow);
  }

  @Override
  public ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation) {
    return delegate.checkAndMutateRowAsync(conditionalRowMutation);
  }

  @Override
  public ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId) {
    return delegate.sampleRowKeysAsync(tableId);
  }

  @Override
  public ApiFuture<Result> readRowAsync(
      String tableId, ByteString rowKey, @Nullable Filter filter) {
    return delegate.readRowAsync(tableId, rowKey, filter);
  }

  @Override
  public ResultScanner readRows(Query request) {
    return delegate.readRows(request);
  }

  @Override
  public ApiFuture<List<Result>> readRowsAsync(Query request) {
    return delegate.readRowsAsync(request);
  }

  @Override
  public void readRowsAsync(Query request, StreamObserver<Result> observer) {
    delegate.readRowsAsync(request, observer);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
    owner.release(key);
  }

  @Override
  public ResultScanner readRows(Query.QueryPaginator paginator, int requestedPageSize) {
    return delegate.readRows(paginator, requestedPageSize);
  }
}
