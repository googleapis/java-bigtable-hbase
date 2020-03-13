/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.bigtable.grpc;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.StateCheckingResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.async.BulkMutationGCJClient;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowAdapter;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.RowResultScanner;
import io.grpc.stub.StreamObserver;
import java.util.List;

/**
 * This class implements existing {@link IBigtableDataClient} operations with Google-cloud-java's
 * {@link com.google.cloud.bigtable.data.v2.BigtableDataClient}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableDataGCJClient implements IBigtableDataClient, AutoCloseable {

  private final BigtableDataClient delegate;

  public BigtableDataGCJClient(BigtableDataClient delegate) {
    this.delegate = delegate;
  }

  @Override
  public void mutateRow(RowMutation rowMutation) {
    delegate.mutateRow(rowMutation);
  }

  @Override
  public ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    return delegate.mutateRowAsync(rowMutation);
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow) {
    return delegate.readModifyWriteRow(readModifyWriteRow);
  }

  @Override
  public ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    return delegate.readModifyWriteRowAsync(readModifyWriteRow);
  }

  @Override
  public IBulkMutation createBulkMutationBatcher(String tableId) {
    return new BulkMutationGCJClient(delegate.newBulkMutationBatcher(tableId));
  }

  @Override
  public Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation) {
    return delegate.checkAndMutateRow(conditionalRowMutation);
  }

  @Override
  public ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation) {
    return delegate.checkAndMutateRowAsync(conditionalRowMutation);
  }

  @Override
  public List<KeyOffset> sampleRowKeys(String tableId) {
    return delegate.sampleRowKeys(tableId);
  }

  @Override
  public ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId) {
    return delegate.sampleRowKeysAsync(tableId);
  }

  @Override
  public ResultScanner<Row> readRows(Query request) {
    final ServerStream<Row> response = delegate.readRows(request);
    return new RowResultScanner<>(response, new Row[0]);
  }

  @Override
  public ApiFuture<List<Row>> readRowsAsync(Query request) {
    return delegate.readRowsCallable().all().futureCall(request);
  }

  @Override
  public List<FlatRow> readFlatRowsList(Query request) {
    return delegate.readRowsCallable(new FlatRowAdapter()).all().call(request);
  }

  @Override
  public ResultScanner<FlatRow> readFlatRows(Query request) {
    final ServerStream<FlatRow> stream =
        delegate.readRowsCallable(new FlatRowAdapter()).call(request);
    return new RowResultScanner<>(stream, new FlatRow[0]);
  }

  @Override
  public ApiFuture<List<FlatRow>> readFlatRowsAsync(Query request) {
    return delegate.readRowsCallable(new FlatRowAdapter()).all().futureCall(request);
  }

  @Override
  public void readFlatRowsAsync(Query request, StreamObserver<FlatRow> observer) {
    delegate
        .readRowsCallable(new FlatRowAdapter())
        .call(request, new StreamObserverAdapter<FlatRow>(observer));
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  /**
   * To wrap stream of native CBT client's {@link StreamObserver} onto GCJ {@link
   * com.google.api.gax.rpc.ResponseObserver}.
   */
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
}
