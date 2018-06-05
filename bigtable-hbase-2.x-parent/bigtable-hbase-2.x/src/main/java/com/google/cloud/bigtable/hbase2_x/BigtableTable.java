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

import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.TimeRange;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BigtableTable extends AbstractBigtableTable {

  @SuppressWarnings("deprecation")
  public static final CompareOp toCompareOp(CompareOperator compareOp) {
    switch(compareOp) {
    case EQUAL:
      return CompareOp.EQUAL;
    case GREATER:
      return CompareOp.GREATER;
    case GREATER_OR_EQUAL:
      return CompareOp.GREATER_OR_EQUAL;
    case LESS:
      return CompareOp.LESS;
    case LESS_OR_EQUAL:
      return CompareOp.LESS_OR_EQUAL;
    case NO_OP:
      return CompareOp.NO_OP;
    case NOT_EQUAL:
      return CompareOp.NOT_EQUAL;
    default:
      throw new IllegalArgumentException("CompareOp type: " + compareOp + " cannot be converted");
    }
  }

  public BigtableTable(AbstractBigtableConnection bigtableConnection,
      HBaseRequestAdapter hbaseAdapter) {
    super(bigtableConnection, hbaseAdapter);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      CompareOperator compareOp, byte[] value, Delete delete) throws IOException {
    return super.checkAndDelete(row, family, qualifier, toCompareOp(compareOp), value, delete);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndMutate(final byte[] row, final byte[] family, final byte[] qualifier,
      final CompareOperator compareOp, final byte[] value, final RowMutations rm)
      throws IOException {
    return super.checkAndMutate(row, family, qualifier, toCompareOp(compareOp), value, rm);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      CompareOperator compareOp, byte[] value, Put put) throws IOException {
    return super.checkAndPut(row, family, qualifier, toCompareOp(compareOp), value, put);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    return existsAll(gets);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TableDescriptor getDescriptor() throws IOException {
    return super.getTableDescriptor();
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

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    BigtableDataClient asyncClient = new BigtableDataClient(this.client);
    final BigtableAsyncTable.CheckAndMutateBuilderImpl delegate =
        new BigtableAsyncTable.CheckAndMutateBuilderImpl(asyncClient, hbaseAdapter, row, family);
    return new CheckAndMutateBuilder() {

      /**
       * {@inheritDoc}
       */
      @Override
      public CheckAndMutateBuilder qualifier(byte[] qualifier) {
        delegate.qualifier(qualifier);
        return this;
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public CheckAndMutateBuilder ifNotExists() {
        delegate.ifNotExists();
        return this;
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public CheckAndMutateBuilder ifMatches(CompareOperator compareOperator, byte[] value) {
        delegate.ifMatches(compareOperator, value);
        return this;
      }

      /**
       * {@inheritDoc}
       */
      public CheckAndMutateBuilder timeRange(TimeRange timeRange) {
        delegate.timeRange(timeRange);
        return this;
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public boolean thenPut(Put put) throws IOException {
        return get(delegate.thenPut(put), "Put");
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public boolean thenDelete(Delete delete) throws IOException {
        return get(delegate.thenDelete(delete), "Delete");
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public boolean thenMutate(RowMutations rowMutations) throws IOException {
        return get(delegate.thenMutate(rowMutations), "RowMutations");
      }

      private boolean get(CompletableFuture<Boolean> future, String type) throws IOException {
        try {
          return future.get();
        } catch (InterruptedException e) {
          Thread.interrupted();
          throw new IOException(type + " was interrputed", e);
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            throw (IOException) cause;
          }
          throw new IOException("Exception occurred in " + type, cause);
        }
      }

    };
  }

}
