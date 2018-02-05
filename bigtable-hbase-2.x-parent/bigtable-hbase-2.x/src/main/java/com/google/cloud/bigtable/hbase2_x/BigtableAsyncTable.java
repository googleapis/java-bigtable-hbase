/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.BatchExecutor;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;

/**
 * Bigtable implementation of {@link AsyncTable}.
 * 
 * @author spollapally
 */
@SuppressWarnings("deprecation")
public class BigtableAsyncTable implements AsyncTable{
  private final Logger LOG = new Logger(getClass());

  private final BigtableAsyncConnection asyncConnection;
  private final BigtableDataClient client;
  private final HBaseRequestAdapter hbaseAdapter;
  private final TableName tableName;
  private BatchExecutor batchExecutor;
  
  public BigtableAsyncTable(BigtableAsyncConnection asyncConnection,
      HBaseRequestAdapter hbaseAdapter) {
    this.asyncConnection = asyncConnection;
    BigtableSession session = asyncConnection.getSession();
    this.client = session.getDataClient();
    this.hbaseAdapter = hbaseAdapter;
    this.tableName = hbaseAdapter.getTableName();
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
    throw new UnsupportedOperationException("delete"); // TODO
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    throw new UnsupportedOperationException("delete list"); // TODO
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    throw new UnsupportedOperationException("get"); // TODO
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
  public CompletableFuture<Void> mutateRow(RowMutations arg0) {
    throw new UnsupportedOperationException("mutateRow"); // TODO
  }

  @Override
  public CompletableFuture<Void> put(Put arg0) {
    throw new UnsupportedOperationException("put"); // TODO
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> arg0) {
    throw new UnsupportedOperationException("put"); // TODO
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan arg0) {
    throw new UnsupportedOperationException("scanAll"); // TODO
  }

  @Override
  public ResultScanner getScanner(Scan arg0) {
    throw new UnsupportedOperationException("getScanner"); // TODO
  }

  @Override
  public void scan(Scan arg0, ScanResultConsumer arg1) {
    throw new UnsupportedOperationException("scan"); // TODO
  }
}
