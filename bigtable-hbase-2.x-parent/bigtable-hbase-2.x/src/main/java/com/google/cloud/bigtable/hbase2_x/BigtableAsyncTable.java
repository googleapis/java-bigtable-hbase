/*
 * Copyright 2017 Google Inc.
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

import static com.google.cloud.bigtable.hbase2_x.ApiFutureUtils.toCompletableFuture;
import static java.util.stream.Collectors.toList;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.BatchExecutor;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.CheckAndMutateUtil;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.GetAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.grpc.stub.StreamObserver;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.CommonConnection;
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
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;

/**
 * Bigtable implementation of {@link AsyncTable}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public abstract class BigtableAsyncTable implements AsyncTable<ScanResultConsumer> {

  private static final Logger LOG = new Logger(AbstractBigtableTable.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private static <T, R> List<R> map(List<T> list, Function<T, R> f) {
    return list.stream().map(f).collect(toList());
  }

  private final CommonConnection connection;
  private final DataClientWrapper clientWrapper;
  private final HBaseRequestAdapter hbaseAdapter;
  private final TableName tableName;
  private BatchExecutor batchExecutor;

  public BigtableAsyncTable(CommonConnection connection, HBaseRequestAdapter hbaseAdapter) {
    this.connection = connection;
    this.clientWrapper = connection.getBigtableApi().getDataClient();
    this.hbaseAdapter = hbaseAdapter;
    this.tableName = hbaseAdapter.getTableName();
  }

  protected synchronized BatchExecutor getBatchExecutor() {
    if (batchExecutor == null) {
      batchExecutor =
          new BatchExecutor(
              connection.getBigtableApi(), connection.getBigtableSettings(), hbaseAdapter);
    }
    return batchExecutor;
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Result> append(Append append) {
    return toCompletableFuture(clientWrapper.readModifyWriteRowAsync(hbaseAdapter.adapt(append)))
        .thenApply(response -> append.isReturnResults() ? response : null);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    // TODO: The CompletableFutures need to return Void for Put/Delete.
    return map(asyncRequests(actions), f -> (CompletableFuture<T>) f);
  }

  /** {@inheritDoc} */
  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return new CheckAndMutateBuilderImpl(clientWrapper, hbaseAdapter, row, family);
  }

  @Override
  public CheckAndMutateWithFilterBuilder checkAndMutate(byte[] bytes, Filter filter) {
    throw new UnsupportedOperationException("not implemented");
  }

  static final class CheckAndMutateBuilderImpl implements CheckAndMutateBuilder {

    private final CheckAndMutateUtil.RequestBuilder builder;
    private final DataClientWrapper clientWrapper;

    public CheckAndMutateBuilderImpl(
        DataClientWrapper clientWrapper,
        HBaseRequestAdapter hbaseAdapter,
        byte[] row,
        byte[] family) {
      this.clientWrapper = clientWrapper;
      this.builder = new CheckAndMutateUtil.RequestBuilder(hbaseAdapter, row, family);
    }

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
    public CompletableFuture<Boolean> thenPut(Put put) {
      try {
        builder.withPut(put);
        return call();
      } catch (Exception e) {
        return ApiFutureUtils.failedFuture(e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> thenDelete(Delete delete) {
      try {
        builder.withDelete(delete);
        return call();
      } catch (Exception e) {
        return ApiFutureUtils.failedFuture(e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> thenMutate(RowMutations mutation) {
      try {
        builder.withMutations(mutation);
        return call();
      } catch (Exception e) {
        return ApiFutureUtils.failedFuture(e);
      }
    }

    private CompletableFuture<Boolean> call() {
      ConditionalRowMutation mutation = builder.build();
      return toCompletableFuture(clientWrapper.checkAndMutateRowAsync(mutation))
          .thenApply(response -> CheckAndMutateUtil.wasMutationApplied(mutation, response));
    }
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    // figure out how to time this with Opencensus
    return toCompletableFuture(clientWrapper.mutateRowAsync(hbaseAdapter.adapt(delete)));
  }

  /** {@inheritDoc} */
  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    return map(asyncRequests(deletes), cf -> cf.thenApply(r -> null));
  }

  private <T> List<CompletableFuture<?>> asyncRequests(List<? extends Row> actions) {
    return map(
        getBatchExecutor().issueAsyncRowRequests(actions, new Object[actions.size()], null),
        ApiFutureUtils::toCompletableFuture);
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Result> get(Get get) {
    Filters.Filter filter = Adapters.GET_ADAPTER.buildFilter(get);
    return toCompletableFuture(
        clientWrapper.readRowAsync(
            tableName.getNameAsString(), ByteStringer.wrap(get.getRow()), filter));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Boolean> exists(Get get) {
    return get(GetAdapter.setCheckExistenceOnly(get)).thenApply(r -> !r.isEmpty());
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public List<CompletableFuture<Result>> get(List<Get> gets) {
    return map(asyncRequests(gets), (f -> (CompletableFuture<Result>) f));
  }

  /** {@inheritDoc} */
  @Override
  public List<CompletableFuture<Boolean>> exists(List<Get> gets) {
    List<Get> existGets = map(gets, GetAdapter::setCheckExistenceOnly);
    return map(get(existGets), cf -> cf.thenApply(r -> !r.isEmpty()));
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConfiguration() {
    return this.connection.getConfiguration(); // TODO
  }

  @Override
  public CompletableFuture<TableDescriptor> getDescriptor() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator() {
    throw new UnsupportedOperationException("not implemented");
  }

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    return toCompletableFuture(
        clientWrapper.readModifyWriteRowAsync(hbaseAdapter.adapt(increment)));
  }

  /** {@inheritDoc} */
  public CompletableFuture<Void> mutateRowVoid(RowMutations rowMutations) {
    return toCompletableFuture(clientWrapper.mutateRowAsync(hbaseAdapter.adapt(rowMutations)));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> put(Put put) {
    // figure out how to time this with Opencensus
    return toCompletableFuture(clientWrapper.mutateRowAsync(hbaseAdapter.adapt(put)));
  }

  /** {@inheritDoc} */
  @Override
  public List<CompletableFuture<Void>> put(List<Put> puts) {
    return map(asyncRequests(puts), f -> f.thenApply(r -> null));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    if (AbstractBigtableTable.hasWhileMatchFilter(scan.getFilter())) {
      throw new UnsupportedOperationException("scanAll with while match filter is not allowed");
    }
    return toCompletableFuture(clientWrapper.readRowsAsync(hbaseAdapter.adapt(scan)));
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(Scan scan) {
    LOG.trace("getScanner(Scan)");
    final Span span = TRACER.spanBuilder("BigtableTable.scan").startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
      ResultScanner scanner = clientWrapper.readRows(hbaseAdapter.adapt(scan));
      if (AbstractBigtableTable.hasWhileMatchFilter(scan.getFilter())) {
        return Adapters.BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER.adapt(scanner, span);
      }
      return scanner;
    } catch (final Throwable throwable) {
      LOG.error("Encountered exception when executing getScanner.", throwable);
      span.setStatus(Status.UNKNOWN);
      // Close the span only when throw an exception and not on finally because if no exception
      // the span will be ended by the adapter.
      span.end();
      return new ResultScanner() {
        @Override
        public boolean renewLease() {
          return false;
        }

        @Override
        public Result next() throws IOException {
          throw throwable;
        }

        @Override
        public ScanMetrics getScanMetrics() {
          return null;
        }

        @Override
        public void close() {}
      };
    }
  }

  /** {@inheritDoc} */
  public void scan(Scan scan, final ScanResultConsumer consumer) {
    if (AbstractBigtableTable.hasWhileMatchFilter(scan.getFilter())) {
      throw new UnsupportedOperationException(
          "scan with consumer and while match filter is not allowed");
    }
    Query query = hbaseAdapter.adapt(scan);
    clientWrapper.readRowsAsync(
        query,
        new StreamObserver<Result>() {
          @Override
          public void onNext(Result value) {
            consumer.onNext(value);
          }

          @Override
          public void onError(Throwable t) {
            consumer.onError(t);
          }

          @Override
          public void onCompleted() {
            consumer.onComplete();
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture coprocessorService(Function arg0, ServiceCaller arg1, byte[] arg2) {
    throw new UnsupportedOperationException("coprocessorService");
  }

  /** {@inheritDoc} */
  @Override
  public CoprocessorServiceBuilder coprocessorService(
      Function arg0, ServiceCaller arg1, CoprocessorCallback arg2) {
    throw new UnsupportedOperationException("coprocessorService");
  }

  private static Class<? extends BigtableAsyncTable> tableClass = null;

  /**
   * This is a workaround for incompatible changes in hbase minor versions. Dynamically generates a
   * class that extends BigtableAdmin so incompatible methods won't be accessed unless the methods
   * are called. If a method is implemented by BigtableAdmin, the generated class will invoke the
   * implementation in BigtableAdmin. Otherwise it'll throw {@link UnsupportedOperationException}.
   */
  private static synchronized Class<? extends BigtableAsyncTable> getSubclass()
      throws NoSuchMethodException {
    if (tableClass == null) {
      tableClass =
          new ByteBuddy()
              .subclass(BigtableAsyncTable.class)
              .method(ElementMatchers.isAbstract())
              .intercept(
                  InvocationHandlerAdapter.of(
                      new AbstractBigtableAdmin.UnsupportedOperationsHandler()))
              .method(
                  ElementMatchers.named("mutateRow")
                      .and(ElementMatchers.returns(CompletableFuture.class)))
              .intercept(
                  MethodCall.invoke(
                          BigtableTable.class.getDeclaredMethod(
                              "mutateRowVoid", RowMutations.class))
                      .withAllArguments())
              .make()
              .load(
                  BigtableAsyncTable.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
              .getLoaded();
    }
    return tableClass;
  }

  public static BigtableAsyncTable createInstance(
      CommonConnection connection, HBaseRequestAdapter hbaseAdapter) throws IOException {
    try {
      return getSubclass()
          .getDeclaredConstructor(CommonConnection.class, HBaseRequestAdapter.class)
          .newInstance(connection, hbaseAdapter);
    } catch (InvocationTargetException e) {
      // Unwrap and throw IOException or RuntimeException as is, and convert all other exceptions to
      // IOException because
      // org.apache.hadoop.hbase.client.Connection#getAdmin() only throws
      // IOException
      Throwables.throwIfInstanceOf(e.getTargetException(), IOException.class);
      Throwables.throwIfInstanceOf(e.getTargetException(), RuntimeException.class);
      throw new IOException(e);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
