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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.TableDescriptor;

import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;

public class BigtableTable extends AbstractBigtableTable {

  public BigtableTable(AbstractBigtableConnection bigtableConnection,
      HBaseRequestAdapter hbaseAdapter) {
    super(bigtableConnection, hbaseAdapter);
  }

  @Override
  public boolean checkAndDelete(byte[] arg0, byte[] arg1, byte[] arg2, CompareOperator arg3,
      byte[] arg4, Delete arg5) throws IOException {
    return false;
  }

  @Override
  public boolean checkAndMutate(byte[] arg0, byte[] arg1, byte[] arg2, CompareOperator arg3,
      byte[] arg4, RowMutations arg5) throws IOException {
    return false;
  }

  @Override
  public boolean checkAndPut(byte[] arg0, byte[] arg1, byte[] arg2, CompareOperator arg3,
      byte[] arg4, Put arg5) throws IOException {
    return false;
  }

  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    return existsAll(gets);
  }

  @Override
  public TableDescriptor getDescriptor() throws IOException {
    return null;
  }

  @Override
  public long getOperationTimeout(TimeUnit arg0) {
    return 0;
  }

  @Override
  public int getReadRpcTimeout() {
    return 0;
  }

  @Override
  public long getReadRpcTimeout(TimeUnit arg0) {
    return 0;
  }

  @Override
  public long getRpcTimeout(TimeUnit arg0) {
    return 0;
  }

  @Override
  public int getWriteRpcTimeout() {
    return 0;
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit arg0) {
    return 0;
  }

  @Override
  public void setReadRpcTimeout(int arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setWriteRpcTimeout(int arg0) {
    // TODO Auto-generated method stub

  }

}
