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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.StateCheckingResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class DataClientVeneerApi implements DataClientWrapper {

  private static final RowResultAdapter RESULT_ADAPTER = new RowResultAdapter();

  private final BigtableDataClient delegate;

  DataClientVeneerApi(BigtableDataClient delegate) {
    this.delegate = delegate;
  }

  @Override
  public BulkMutationWrapper createBulkMutation(String tableId) {
    return new BulkMutationVeneerApi(delegate.newBulkMutationBatcher(tableId));
  }

  @Override
  public BulkReadWrapper createBulkRead(String tableId) {
    return new BulkReadVeneerApi(delegate, tableId);
  }

  @Override
  public ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    return delegate.mutateRowAsync(rowMutation);
  }

  @Override
  public ApiFuture<Result> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    return ApiFutures.transform(
        delegate.readModifyWriteRowAsync(readModifyWriteRow),
        new ApiFunction<Row, Result>() {
          @Override
          public Result apply(Row row) {
            return Adapters.ROW_ADAPTER.adaptResponse(row);
          }
        },
        MoreExecutors.directExecutor());
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
      String tableId, ByteString rowKey, @Nullable Filters.Filter filter) {
    return ApiFutures.transform(
        delegate.readRowAsync(tableId, rowKey, filter),
        new ApiFunction<Row, Result>() {
          @Override
          public Result apply(Row row) {
            return Adapters.ROW_ADAPTER.adaptResponse(row);
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public ResultScanner readRows(Query request) {
    return new RowResultScanner(delegate.readRowsCallable(RESULT_ADAPTER).call(request));
  }

  @Override
  public ApiFuture<List<Result>> readRowsAsync(Query request) {
    return delegate.readRowsCallable(RESULT_ADAPTER).all().futureCall(request);
  }

  @Override
  public void readRowsAsync(Query request, StreamObserver<Result> observer) {
    delegate.readRowsCallable(RESULT_ADAPTER).call(request, new StreamObserverAdapter<>(observer));
  }

  @Override
  public void close() {
    delegate.close();
  }

  /** wraps {@link StreamObserver} onto GCJ {@link com.google.api.gax.rpc.ResponseObserver}. */
  private static class StreamObserverAdapter<T> extends StateCheckingResponseObserver<T> {
    private final StreamObserver<T> delegate;

    StreamObserverAdapter(StreamObserver<T> delegate) {
      this.delegate = delegate;
    }

    protected void onStartImpl(StreamController controller) {}

    protected void onResponseImpl(T response) {
      this.delegate.onNext(response);
    }

    protected void onErrorImpl(Throwable t) {
      this.delegate.onError(t);
    }

    protected void onCompleteImpl() {
      this.delegate.onCompleted();
    }
  }

  /** wraps {@link ServerStream} onto HBase {@link ResultScanner}. */
  private static class RowResultScanner extends AbstractClientScanner {

    private final ServerStream<Result> serverStream;
    private final Iterator<Result> iterator;

    RowResultScanner(ServerStream<Result> serverStream) {
      this.serverStream = serverStream;
      this.iterator = serverStream.iterator();
    }

    @Override
    public Result next() {
      if (!iterator.hasNext()) {
        // null signals EOF
        return null;
      }

      return iterator.next();
    }

    @Override
    public void close() {
      if (iterator.hasNext()) {
        serverStream.cancel();
      }
    }

    public boolean renewLease() {
      throw new UnsupportedOperationException("renewLease");
    }
  }
}
