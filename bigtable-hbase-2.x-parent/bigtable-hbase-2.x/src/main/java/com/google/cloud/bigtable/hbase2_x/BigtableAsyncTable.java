/*
d * Copyright 2017 Google Inc. All Rights Reserved.
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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.BatchExecutor;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Bigtable implementation of {@link AsyncTable}.
 * 
 * @author spollapally
 */
public class BigtableAsyncTable implements AsyncTable {
  private final Logger LOG = new Logger(getClass());

  private final BigtableAsyncConnection asyncConnection;
  private final BigtableDataClient client;
  private final HBaseRequestAdapter hbaseAdapter;
  private final TableName tableName;
  private final ExecutorService executorService;
  private BatchExecutor batchExecutor;

  public BigtableAsyncTable(BigtableAsyncConnection asyncConnection,
      HBaseRequestAdapter hbaseAdapter, ExecutorService executorService) {
    this.asyncConnection = asyncConnection;
    BigtableSession session = asyncConnection.getSession();
    this.client = session.getDataClient();
    this.hbaseAdapter = hbaseAdapter;
    this.tableName = hbaseAdapter.getTableName();
    this.executorService = executorService;
  }

  protected synchronized BatchExecutor getBatchExecutor() {
    if (batchExecutor == null) {
      batchExecutor = new BatchExecutor(asyncConnection.getSession(), hbaseAdapter);
    }
    return batchExecutor;
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    throw new UnsupportedOperationException("append"); // TODO
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    throw new UnsupportedOperationException("batch"); // TODO
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] rowParam, byte[] familyParam) {
    throw new UnsupportedOperationException("checkAndMutate"); // TODO
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    // figure out how to time this with Opencensus
    MutateRowRequest request = hbaseAdapter.adapt(delete);
    ListenableFuture<MutateRowResponse> future = client.mutateRowAsync(request);
    return FutureUtils.toCompletableFuture(future, executorService)
        .thenApply(r -> null);
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    throw new UnsupportedOperationException("delete list"); // TODO
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    ListenableFuture<List<FlatRow>> future = client.readFlatRowsAsync(hbaseAdapter.adapt(get));
    return FutureUtils.toCompletableFuture(future, (list -> toResult("get", list)),
      executorService);
  }

  private Get addKeyOnlyFilter(Get get) {
    Get existsGet = new Get(get);
    if (get.getFilter() == null) {
      existsGet.setFilter(new KeyOnlyFilter());
    } else {
      existsGet.setFilter(new FilterList(get.getFilter(), new KeyOnlyFilter()));
    }
    return existsGet;
  }

  @Override
  public CompletableFuture<Boolean> exists(Get get) {
    return get(addKeyOnlyFilter(get)).thenApply(r -> !r.isEmpty());
  }

  private static Result toResult(String method, List<FlatRow> list) {
    return Adapters.FLAT_ROW_ADAPTER.adaptResponse(getSingleResult(method, list));
  }

  private static FlatRow getSingleResult(String method, List<FlatRow> list) {
    switch (list.size()) {
    case 0:
      return null;
    case 1:
      return list.get(0);
    default:
      throw new IllegalStateException("Multiple responses found for " + method);
    }
  }

  @Override
  public List<CompletableFuture<Result>> get(List<Get> arg0) {
    throw new UnsupportedOperationException("get"); // TODO
  }

  @Override
  public Configuration getConfiguration() {
    return this.asyncConnection.getConfiguration(); // TODO
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @Override
  public long getOperationTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException("getOperationTimeout"); // TODO
  }

  @Override
  public long getReadRpcTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getReadRpcTimeout"); // TODO
  }

  @Override
  public long getRpcTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getRpcTimeout"); // TODO
  }

  @Override
  public long getScanTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getScanTimeout"); // TODO
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getWriteRpcTimeout"); // TODO
  }

  @Override
  public CompletableFuture<Result> increment(Increment arg0) {
    throw new UnsupportedOperationException("increment"); // TODO
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations rowMutations) {
    MutateRowRequest request = hbaseAdapter.adapt(rowMutations);
    ListenableFuture<MutateRowResponse> future = client.mutateRowAsync(request);
    return FutureUtils.toCompletableFuture(future, executorService)
        .thenApply(r -> null);
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    // figure out how to time this with Opencensus
    MutateRowRequest request = hbaseAdapter.adapt(put);
    ListenableFuture<MutateRowResponse> future = client.mutateRowAsync(request);
    return FutureUtils.toCompletableFuture(future, executorService).thenApply(r -> null);
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> arg0) {
    throw new UnsupportedOperationException("put"); // TODO
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    throw new UnsupportedOperationException("scanAll"); // TODO
  }

  @Override
  public ResultScanner getScanner(Scan scan) {
    throw new UnsupportedOperationException("getScanner"); // TODO
  }

  @Override
  public void scan(Scan scan, ScanResultConsumer consumer) {
    throw new UnsupportedOperationException("scan"); // TODO
  }

}
