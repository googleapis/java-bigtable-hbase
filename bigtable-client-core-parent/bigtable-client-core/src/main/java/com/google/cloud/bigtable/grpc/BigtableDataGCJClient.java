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
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowAdapter;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.RowResultScanner;
import com.google.common.collect.ImmutableList;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * This class implements existing {@link com.google.cloud.bigtable.core.IBigtableDataClient} operations with
 * Google-cloud-java's {@link com.google.cloud.bigtable.data.v2.BigtableDataClient}.
 */
public class BigtableDataGCJClient implements IBigtableDataClient, AutoCloseable {

  private final BigtableDataClient delegate;

  public BigtableDataGCJClient(BigtableDataClient delegate){
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
  public IBulkMutation createBulkMutationBatcher() {
    throw new UnsupportedOperationException("Not implemented yet");
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

  private ResultScanner<FlatRow> readFlatRowsV2(Query request){
    List<FlatRow> flatRows = delegate.readRowsCallable(new FlatRowAdapter()).all().call(request);
    final Iterator<FlatRow> flatRowIterator = flatRows.iterator();
    return new ResultScanner<FlatRow>(){

      @Override
      public void close() throws IOException {
      }

      @Override
      public FlatRow next() throws IOException {
        return flatRowIterator.next();
      }

      @Override
      public FlatRow[] next(int count) throws IOException {
        ImmutableList.Builder<FlatRow> builder = ImmutableList.builder();
        for (int i = 0; flatRowIterator.hasNext() && i < count; i++) {
          builder.add(flatRowIterator.next());
        }
        return builder.build().toArray(new FlatRow[0]);
      }

      @Override
      public int available() {
        throw new UnsupportedOperationException("can not support available");
      }
    };
  }

  @Override
  public ApiFuture<List<FlatRow>> readFlatRowsAsync(Query request) {
    return delegate.readRowsCallable(new FlatRowAdapter()).all().futureCall(request);
  }

  @Override
  public void readFlatRowsAsync(Query request, StreamObserver<FlatRow> observer) {
    //TODO: figure out to convert StreamObserver to ResponseObserver.
    //delegate.readRowsCallable(new FlatRowAdapter()).call(request, observer);
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }
}
