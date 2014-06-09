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

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AnvilTop implements HTableInterface {
  protected final TableName tableName;

  /**
   * Constructed by AnvilTopConnection
   *
   * @param tableName
   */
  public AnvilTop(TableName tableName) {
    this.tableName = tableName;
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
    throw new UnsupportedOperationException();  // TODO
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
    throw new UnsupportedOperationException();  // TODO
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
    throw new UnsupportedOperationException();  // TODO
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
    throw new UnsupportedOperationException();  // TODO
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
    throw new UnsupportedOperationException();  // TODO
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
}
