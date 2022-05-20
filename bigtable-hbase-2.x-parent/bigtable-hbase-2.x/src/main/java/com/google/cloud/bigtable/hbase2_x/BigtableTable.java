/*
 * Copyright 2017 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.adapters.CheckAndMutateUtil;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.util.FutureUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.TimeRange;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public abstract class BigtableTable extends AbstractBigtableTable {

  @SuppressWarnings("deprecation")
  public static final CompareOp toCompareOp(CompareOperator compareOp) {
    switch (compareOp) {
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

  public BigtableTable(
      AbstractBigtableConnection bigtableConnection, HBaseRequestAdapter hbaseAdapter) {
    super(bigtableConnection, hbaseAdapter);
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndDelete(
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOperator compareOp,
      byte[] value,
      Delete delete)
      throws IOException {
    return super.checkAndDelete(row, family, qualifier, toCompareOp(compareOp), value, delete);
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndMutate(
      final byte[] row,
      final byte[] family,
      final byte[] qualifier,
      final CompareOperator compareOp,
      final byte[] value,
      final RowMutations rm)
      throws IOException {
    return super.checkAndMutate(row, family, qualifier, toCompareOp(compareOp), value, rm);
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndPut(
      byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, byte[] value, Put put)
      throws IOException {
    return super.checkAndPut(row, family, qualifier, toCompareOp(compareOp), value, put);
  }

  /** {@inheritDoc} */
  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    return existsAll(gets);
  }

  /** {@inheritDoc} */
  @Override
  public TableDescriptor getDescriptor() throws IOException {
    return super.getTableDescriptor();
  }

  @Override
  public RegionLocator getRegionLocator() throws IOException {
    throw new UnsupportedOperationException("not implemented");
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
    final CheckAndMutateUtil.RequestBuilder builder =
        new CheckAndMutateUtil.RequestBuilder(hbaseAdapter, row, family);

    return new CheckAndMutateBuilder() {
      /** {@inheritDoc} */
      @Override
      public CheckAndMutateBuilder qualifier(byte[] qualifier) {
        builder.qualifier(qualifier);
        return this;
      }

      /** {@inheritDoc} */
      @Override
      public CheckAndMutateBuilder ifNotExists() {
        builder.ifNotExists();
        return this;
      }

      /** {@inheritDoc} */
      @Override
      public CheckAndMutateBuilder ifMatches(CompareOperator compareOp, byte[] value) {
        Preconditions.checkNotNull(compareOp, "compareOp is null");
        if (compareOp != CompareOperator.NOT_EQUAL) {
          Preconditions.checkNotNull(value, "value is null for compareOperator: " + compareOp);
        }
        builder.ifMatches(BigtableTable.toCompareOp(compareOp), value);
        return this;
      }

      /** {@inheritDoc} */
      public CheckAndMutateBuilder timeRange(TimeRange timeRange) {
        builder.timeRange(timeRange.getMin(), timeRange.getMax());
        return this;
      }

      /** {@inheritDoc} */
      @Override
      public boolean thenPut(Put put) throws IOException {
        try {
          builder.withPut(put);
          return call();
        } catch (Exception e) {
          throw new IOException("Could not CheckAndMutate.thenPut: " + e.getMessage(), e);
        }
      }

      /** {@inheritDoc} */
      @Override
      public boolean thenDelete(Delete delete) throws IOException {
        try {
          builder.withDelete(delete);
          return call();
        } catch (Exception e) {
          throw new IOException("Could not CheckAndMutate.thenDelete: " + e.getMessage(), e);
        }
      }

      /** {@inheritDoc} */
      @Override
      public boolean thenMutate(RowMutations rowMutations) throws IOException {
        try {
          builder.withMutations(rowMutations);
          return call();
        } catch (Exception e) {
          throw new IOException("Could not CheckAndMutate.thenMutate: " + e.getMessage(), e);
        }
      }

      private boolean call() throws IOException {
        ConditionalRowMutation conditionalRowMutation = builder.build();
        Boolean response =
            FutureUtil.unwrap(clientWrapper.checkAndMutateRowAsync(conditionalRowMutation));
        return CheckAndMutateUtil.wasMutationApplied(conditionalRowMutation, response);
      }
    };
  }

  @Override
  public <R> void batchCallback(
      List<? extends Row> actions, Object[] results, Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    // This check is intentional because HBase-2.x does not perform param validation before return.
    if (actions.isEmpty()) {
      return;
    }
    super.batchCallback(actions, results, callback);
  }

  private static Class<? extends BigtableTable> tableClass = null;

  /**
   * This is a workaround for incompatible changes in hbase minor versions. Dynamically generates a
   * class that extends AbstractBigtableTable so incompatible methods won't be accessed unless the
   * methods are called. If a method is implemented by AbstractBigtableTable, the generated class
   * will invoke the implementation in AbstractBigtableTable.
   */
  private static synchronized Class<? extends BigtableTable> getSubclass()
      throws NoSuchMethodException {
    if (tableClass == null) {
      tableClass =
          new ByteBuddy()
              .subclass(BigtableTable.class)
              .method(ElementMatchers.named("coprocessorService"))
              .intercept(
                  InvocationHandlerAdapter.of(
                      new AbstractBigtableAdmin.UnsupportedOperationsHandler()))
              .method(ElementMatchers.named("batchCoprocessorService"))
              .intercept(
                  InvocationHandlerAdapter.of(
                      new AbstractBigtableAdmin.UnsupportedOperationsHandler()))
              .method(
                  ElementMatchers.named("mutateRow")
                      .and(ElementMatchers.returns(TypeDescription.VOID)))
              .intercept(
                  MethodCall.invoke(
                          AbstractBigtableTable.class.getDeclaredMethod(
                              "mutateRowVoid", RowMutations.class))
                      .withAllArguments())
              .method(ElementMatchers.named("mutateRow").and(ElementMatchers.returns(Result.class)))
              .intercept(
                  MethodCall.invoke(
                          AbstractBigtableTable.class.getDeclaredMethod(
                              "mutateRowResult", RowMutations.class))
                      .withAllArguments())
              .make()
              .load(
                  AbstractBigtableTable.class.getClassLoader(),
                  ClassLoadingStrategy.Default.INJECTION)
              .getLoaded();
    }
    return tableClass;
  }

  public static BigtableTable createInstance(
      AbstractBigtableConnection bigtableConnection, HBaseRequestAdapter hbaseAdapter)
      throws IOException {
    try {
      return getSubclass()
          .getDeclaredConstructor(AbstractBigtableConnection.class, HBaseRequestAdapter.class)
          .newInstance(bigtableConnection, hbaseAdapter);
    } catch (InvocationTargetException e) {
      // Unwrap and throw IOException or RuntimeException as is, and convert all other exceptions to
      // IOException because
      // org.apache.hadoop.hbase.client.Connection#getTable() only throws
      // IOException
      Throwables.throwIfInstanceOf(e.getTargetException(), IOException.class);
      Throwables.throwIfInstanceOf(e.getTargetException(), RuntimeException.class);
      throw new IOException(e);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
