/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.StateCheckingResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.api.gax.rpc.UnaryCallable;
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
import com.google.cloud.bigtable.metrics.OperationMetrics;
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
  private UnaryCallable<RowMutation, Void> mutateRowCallable;
  private UnaryCallable<ReadModifyWriteRow, Row> readModifyWriteRowCallable;
  private UnaryCallable<ConditionalRowMutation, Boolean> checkAndMutateRowCallable;
  private UnaryCallable<String, List<KeyOffset>> sampleRowKeysCallable;
  private ServerStreamingCallable<Query, Row> readRowsCallable;
  private ServerStreamingCallable<Query, FlatRow> readFlatRowsCallable;

  BigtableDataGCJClient(BigtableDataClient delegate) {
    this.delegate = delegate;
    this.mutateRowCallable =
        new InstrumentedUnaryCallable<>(
            delegate.mutateRowCallable(), OperationMetrics.create("MutateRow"));
    this.readModifyWriteRowCallable =
        new InstrumentedUnaryCallable<>(
            delegate.readModifyWriteRowCallable(), OperationMetrics.create("ReadModifyRow"));
    this.checkAndMutateRowCallable =
        new InstrumentedUnaryCallable<>(
            delegate.checkAndMutateRowCallable(), OperationMetrics.create("CheckAndMutate"));
    this.sampleRowKeysCallable =
        new InstrumentedUnaryCallable<>(
            delegate.sampleRowKeysCallable(), OperationMetrics.create("SampleRowKeys"));
    this.readRowsCallable =
        new InstrumentedReadRowsCallable<>(
            delegate.readRowsCallable(), OperationMetrics.create("ReadRows"));
    this.readFlatRowsCallable =
        new InstrumentedReadRowsCallable<>(
            delegate.readRowsCallable(new FlatRowAdapter()), OperationMetrics.create("ReadRows"));
  }

  @Override
  public void mutateRow(RowMutation rowMutation) {
    mutateRowCallable.call(rowMutation);
  }

  @Override
  public ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    return mutateRowCallable.futureCall(rowMutation);
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow) {
    return readModifyWriteRowCallable.call(readModifyWriteRow);
  }

  @Override
  public ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    return readModifyWriteRowCallable.futureCall(readModifyWriteRow);
  }

  @Override
  public IBulkMutation createBulkMutationBatcher(String tableId) {
    return new BulkMutationGCJClient(delegate.newBulkMutationBatcher(tableId));
  }

  @Override
  public Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation) {
    return checkAndMutateRowCallable.call(conditionalRowMutation);
  }

  @Override
  public ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation) {
    return checkAndMutateRowCallable.futureCall(conditionalRowMutation);
  }

  @Override
  public List<KeyOffset> sampleRowKeys(String tableId) {
    return sampleRowKeysCallable.call(tableId);
  }

  @Override
  public ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId) {
    return sampleRowKeysCallable.futureCall(tableId);
  }

  @Override
  public ResultScanner<Row> readRows(Query request) {
    return new RowResultScanner<>(readRowsCallable.call(request), new Row[0]);
  }

  @Override
  public ApiFuture<List<Row>> readRowsAsync(Query request) {
    return readRowsCallable.all().futureCall(request);
  }

  @Override
  public List<FlatRow> readFlatRowsList(Query request) {
    return readFlatRowsCallable.all().call(request);
  }

  @Override
  public ResultScanner<FlatRow> readFlatRows(Query request) {
    return new RowResultScanner<>(readFlatRowsCallable.call(request), new FlatRow[0]);
  }

  @Override
  public ApiFuture<List<FlatRow>> readFlatRowsAsync(Query request) {
    return readFlatRowsCallable.all().futureCall(request);
  }

  @Override
  public void readFlatRowsAsync(Query request, StreamObserver<FlatRow> observer) {
    readFlatRowsCallable.call(request, new StreamObserverAdapter<>(observer));
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
