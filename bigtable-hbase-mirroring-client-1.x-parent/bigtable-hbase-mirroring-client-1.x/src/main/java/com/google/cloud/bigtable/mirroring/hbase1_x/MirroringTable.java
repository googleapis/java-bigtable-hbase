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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncTableWrapper;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Service;

/**
 * Table which mirrors every two mutations to two underlying tables.
 *
 * <p>Objects of this class present themselves as HBase 1.x `Table` objects. Every operation is
 * first performed on primary table and if it succeeded it is replayed on the secondary table
 * asynchronously. Read operations are mirrored to verify that content of both databases matches.
 */
@InternalApi("For internal usage only")
public class MirroringTable implements Table {
  Table primaryTable;
  Table secondaryTable;
  AsyncTableWrapper secondaryAsyncWrapper;
  VerificationContinuationFactory verificationContinuationFactory;

  /**
   * @param executorService ExecutorService is used to perform operations on secondaryTable and
   *     verification tasks.
   * @param mismatchDetector Detects mismatches in results from operations preformed on both
   *     databases.
   */
  public MirroringTable(
      Table primaryTable,
      Table secondaryTable,
      ExecutorService executorService,
      MismatchDetector mismatchDetector) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
    this.verificationContinuationFactory = new VerificationContinuationFactory(mismatchDetector);
    this.secondaryAsyncWrapper =
        new AsyncTableWrapper(
            this.secondaryTable, MoreExecutors.listeningDecorator(executorService));
  }

  @Override
  public TableName getName() {
    return this.primaryTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    boolean result = this.primaryTable.exists(get);
    Futures.addCallback(
        this.secondaryAsyncWrapper.exists(get),
        this.verificationContinuationFactory.exists(get, result),
        MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public boolean[] existsAll(List<Get> list) throws IOException {
    boolean[] result = this.primaryTable.existsAll(list);
    Futures.addCallback(
        this.secondaryAsyncWrapper.existsAll(list),
        this.verificationContinuationFactory.existsAll(list, result),
        MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public void batch(List<? extends Row> list, Object[] objects)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] batch(List<? extends Row> list) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> void batchCallback(List<? extends Row> list, Object[] objects, Callback<R> callback)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> list, Callback<R> callback)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Result get(Get get) throws IOException {
    Result result = this.primaryTable.get(get);
    Futures.addCallback(
        this.secondaryAsyncWrapper.get(get),
        this.verificationContinuationFactory.get(get, result),
        MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public Result[] get(List<Get> list) throws IOException {
    Result[] result = this.primaryTable.get(list);
    Futures.addCallback(
        this.secondaryAsyncWrapper.get(list),
        this.verificationContinuationFactory.get(list, result),
        MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return new MirroringResultScanner(
        scan,
        this.primaryTable.getScanner(scan),
        this.secondaryAsyncWrapper.getScanner(scan),
        this.verificationContinuationFactory);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return getScanner(new Scan().addFamily(family));
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return getScanner(new Scan().addColumn(family, qualifier));
  }

  @Override
  public void close() throws IOException {
    try {
      this.primaryTable.close();
    } finally {
      this.secondaryAsyncWrapper.close();
    }
  }

  @Override
  public void put(Put put) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(List<Put> list) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Put put)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean checkAndPut(
      byte[] bytes, byte[] bytes1, byte[] bytes2, CompareOp compareOp, byte[] bytes3, Put put)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(Delete delete) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(List<Delete> list) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean checkAndDelete(
      byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Delete delete) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean checkAndDelete(
      byte[] bytes, byte[] bytes1, byte[] bytes2, CompareOp compareOp, byte[] bytes3, Delete delete)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mutateRow(RowMutations rowMutations) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Result append(Append append) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long incrementColumnValue(
      byte[] bytes, byte[] bytes1, byte[] bytes2, long l, Durability durability)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(
      Class<T> aClass, byte[] bytes, byte[] bytes1, Call<T, R> call) throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Service, R> void coprocessorService(
      Class<T> aClass, byte[] bytes, byte[] bytes1, Call<T, R> call, Callback<R> callback)
      throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getWriteBufferSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWriteBufferSize(long l) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r)
      throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R extends Message> void batchCoprocessorService(
      MethodDescriptor methodDescriptor,
      Message message,
      byte[] bytes,
      byte[] bytes1,
      R r,
      Callback<R> callback)
      throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean checkAndMutate(
      byte[] bytes,
      byte[] bytes1,
      byte[] bytes2,
      CompareOp compareOp,
      byte[] bytes3,
      RowMutations rowMutations)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOperationTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getOperationTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRpcTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getReadRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setReadRpcTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getWriteRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWriteRpcTimeout(int i) {
    throw new UnsupportedOperationException();
  }
}
