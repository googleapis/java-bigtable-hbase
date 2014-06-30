/*
 * Copyright (c) 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase;

import com.google.bigtable.anviltop.AnviltopData;
import com.google.bigtable.anviltop.AnviltopServices.GetRowRequestOrBuilder;
import com.google.bigtable.anviltop.AnviltopServices.GetRowResponse;
import com.google.cloud.anviltop.hbase.adapters.DeleteAdapter;
import com.google.cloud.anviltop.hbase.adapters.GetAdapter;
import com.google.cloud.anviltop.hbase.adapters.GetRowResponseAdapter;
import com.google.cloud.anviltop.hbase.adapters.MutationAdapter;
import com.google.cloud.anviltop.hbase.adapters.PutAdapter;
import com.google.cloud.anviltop.hbase.adapters.RowMutationsAdapter;
import com.google.cloud.anviltop.hbase.adapters.UnsupportedOperationAdapter;
import com.google.cloud.hadoop.hbase.AnviltopClient;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AnvilTopTable implements HTableInterface {
  protected final TableName tableName;
  protected final AnviltopOptions options;
  protected final AnviltopClient client;
  protected final PutAdapter putAdapter = new PutAdapter();
  protected final DeleteAdapter deleteAdapter = new DeleteAdapter();
  protected final MutationAdapter mutationAdapter =
      new MutationAdapter(
          deleteAdapter,
          putAdapter,
          new UnsupportedOperationAdapter<Increment>("increment"),
          new UnsupportedOperationAdapter<Append>("append"));
  protected final RowMutationsAdapter rowMutationsAdapter =
      new RowMutationsAdapter(mutationAdapter);
  protected final GetAdapter getAdapter = new GetAdapter();
  protected final GetRowResponseAdapter getRowResponseAdapter = new GetRowResponseAdapter();
  protected final Configuration configuration;

  /**
   * Constructed by AnvilTopConnection
   *
   * @param tableName
   * @param client
   */
  public AnvilTopTable(TableName tableName,
      AnviltopOptions options,
      Configuration configuration,
      AnviltopClient client) {
    this.tableName = tableName;
    this.options = options;
    this.client = client;
    this.configuration = configuration;
  }

  @Override
  public byte[] getTableName() {
    return this.tableName.getName();
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean exists(Get get) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public Boolean[] exists(List<Get> gets) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Batch.Callback<R> callback) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public Result get(Get get) throws IOException {
    GetRowRequestOrBuilder getRowRequest = getAdapter.adapt(get);
    try {
      GetRowResponse response =
          client.getRow(
              options.getProjectId(),
              tableName.getQualifierAsString(),
              getRowRequest.getRowKey().toByteArray());

      return getRowResponseAdapter.adaptResponse(response);
    } catch (ServiceException e) {
      throw new IOException(
          makeGenericExceptionMessage(
              "get",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              getRowRequest.getRowKey().toByteArray()),
          e);
    }
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void put(Put put) throws IOException {
    AnviltopData.RowMutation rowMutation = putAdapter.adapt(put).build();
    try {
      client.mutateAtomic(
          options.getProjectId(),
          tableName.getQualifierAsString(),
          rowMutation);
    } catch (ServiceException e) {
      throw new IOException(
          makeGenericExceptionMessage(
              "put",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              rowMutation.getRowKey().toByteArray()),
          e);
    }
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void delete(Delete delete) throws IOException {
    AnviltopData.RowMutation rowMutation = deleteAdapter.adapt(delete).build();
    try {
      client.mutateAtomic(options.getProjectId(),
          tableName.getQualifierAsString(),
          rowMutation);
    } catch (ServiceException e) {
      throw new IOException(
          makeGenericExceptionMessage(
              "delete",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              rowMutation.getRowKey().toByteArray()),
          e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    try {
      client.mutateAtomic(options.getProjectId(),
          tableName.getQualifierAsString(),
          rowMutationsAdapter.adapt(rm).build());
    } catch (ServiceException e) {
      throw new IOException("Failed to mutate.", e);
    }
  }

  @Override
  public Result append(Append append) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      boolean writeToWAL) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean isAutoFlush() {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void flushCommits() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Batch.Call<T, R> callable) throws ServiceException, Throwable {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public long getWriteBufferSize() {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes2,
      R r) throws ServiceException, Throwable {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <R extends Message> void batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes2,
      R r, Batch.Callback<R> rCallback) throws ServiceException, Throwable {
    throw new UnsupportedOperationException();  // TODO
  }

  static String makeGenericExceptionMessage(
      String operation,
      String projectId,
      String tableName,
      byte[] rowKey) {
    return String.format(
        "Failed to perform operation. Operation='%s', projectId='%s', tableName='%s', rowKey='%s'",
        operation,
        projectId,
        tableName,
        Bytes.toStringBinary(rowKey));
  }
}
