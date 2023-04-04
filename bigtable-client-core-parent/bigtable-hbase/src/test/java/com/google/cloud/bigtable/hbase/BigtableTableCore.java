/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.bigtable.hbase;

import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Service;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;

public class BigtableTableCore extends AbstractBigtableTable {

  public BigtableTableCore(
      AbstractBigtableConnection bigtableConnection, HBaseRequestAdapter hbaseAdapter) {
    super(bigtableConnection, hbaseAdapter);
  }

  @Override
  public void mutateRow(RowMutations rowMutations) throws IOException {
    mutateRowBase(rowMutations);
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(
      Class<T> aClass, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T extends Service, R> void coprocessorService(
      Class<T> aClass,
      byte[] bytes,
      byte[] bytes1,
      Batch.Call<T, R> call,
      Batch.Callback<R> callback)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor,
      Message message,
      byte[] bytes,
      byte[] bytes1,
      R r)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <R extends Message> void batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor,
      Message message,
      byte[] bytes,
      byte[] bytes1,
      R r,
      Batch.Callback<R> callback)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }
}
