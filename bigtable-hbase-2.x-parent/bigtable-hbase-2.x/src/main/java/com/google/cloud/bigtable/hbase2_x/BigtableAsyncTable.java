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

import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
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
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.BatchExecutor;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.CheckAndMutateUtil;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;

import io.grpc.stub.StreamObserver;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

/**
 * Bigtable implementation of {@link AsyncTable}.
 * 
 * @author spollapally
 */
public class BigtableAsyncTable implements AsyncTable<ScanResultConsumer> {

  private static final Logger LOG = new Logger(AbstractBigtableTable.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private static <T, R> List<R> map(List<T> list, Function<T, R> f) {
    return list.stream().map(f).collect(toList());
  }

  private final BigtableAsyncConnection asyncConnection;
  private final BigtableDataClient client;
  private final HBaseRequestAdapter hbaseAdapter;
  private final TableName tableName;
  private BatchExecutor batchExecutor;

  public BigtableAsyncTable(BigtableAsyncConnection asyncConnection,
      HBaseRequestAdapter hbaseAdapter) {
    this.asyncConnection = asyncConnection;
    BigtableSession session = asyncConnection.getSession();
    this.client = new BigtableDataClient(session.getDataClient());
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
    ReadModifyWriteRowRequest request = hbaseAdapter.adapt(append);
    Function<? super ReadModifyWriteRowResponse, ? extends Result> adaptRowFunction = response -> 
        append.isReturnResults()
            ? Adapters.ROW_ADAPTER.adaptResponse(response.getRow())
            : null;
    return client.readModifyWriteRowAsync(request).thenApply(adaptRowFunction);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    List<? extends Row> updatedActions =
        map(actions, row -> 
            row instanceof Get 
              ? fromHB2Get((Get) row)
              : row);
    // TODO: The CompletableFutures need to return Void for Put/Delete.
    return map(asyncRequests(updatedActions), f -> (CompletableFuture<T>) f);
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return new CheckAndMutateBuilderImpl(row, family);
  }


  private final class CheckAndMutateBuilderImpl implements CheckAndMutateBuilder {

    private final byte[] row;

    private final byte[] family;

    private byte[] qualifier;

    private CompareOperator op;

    private byte[] value;

    CheckAndMutateBuilderImpl(byte[] row, byte[] family) {
      this.row = Preconditions.checkNotNull(row, "row is null");
      this.family = Preconditions.checkNotNull(family, "family is null");
    }

    @Override
    public CheckAndMutateBuilder qualifier(byte[] qualifier) {
      this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier is null. Consider using" +
          " an empty byte array, or just do not call this method if you want a null qualifier");
      return this;
    }

    @Override
    public CheckAndMutateBuilder ifNotExists() {
      this.op = CompareOperator.EQUAL;
      this.value = null;
      return this;
    }

    @Override
    public CheckAndMutateBuilder ifMatches(CompareOperator compareOp, byte[] value) {
      this.op = Preconditions.checkNotNull(compareOp, "compareOp is null");
      if (compareOp != CompareOperator.EQUAL && compareOp != CompareOperator.NOT_EQUAL) {
        this.value =
            Preconditions.checkNotNull(value, "value is null for compareOperator: " + compareOp);
      } else {
        this.value = value;
      }
      return this;
    }

    private void preCheck() {
      Preconditions.checkNotNull(op, "condition is null. You need to specify the condition by" +
          " calling ifNotExists/ifEquals/ifMatches before executing the request");
    }

    @Override
    public CompletableFuture<Boolean> thenPut(Put put) {
      try {
        return call(put.getRow(), hbaseAdapter.adapt(put));
      } catch (Exception e) {
        return FutureUtils.failedFuture(e);
      }
    }

    @Override
    public CompletableFuture<Boolean> thenDelete(Delete delete) {
      try {
        return call(delete.getRow(), hbaseAdapter.adapt(delete));
      } catch (Exception e) {
        return FutureUtils.failedFuture(e);
      }
    }

    @Override
    public CompletableFuture<Boolean> thenMutate(RowMutations mutation) {
      try {
        return call(mutation.getRow(), hbaseAdapter.adapt(mutation));
      } catch (Exception e) {
        return FutureUtils.failedFuture(e);
      }
    }

    private CompletableFuture<Boolean> call(byte[] actionRow, MutateRowRequest mutateRowRequest)
        throws IOException {
      preCheck();
      CheckAndMutateRowRequest request =
          CheckAndMutateUtil.makeConditionalMutationRequest(
              hbaseAdapter,
              row,
              family,
              qualifier,
              BigtableTable.toCompareOp(op),
              value,
              actionRow,
              mutateRowRequest.getMutationsList());
      return client.checkAndMutateRowAsync(request).thenApply(
        response -> CheckAndMutateUtil.wasMutationApplied(request, response));
    }
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    // figure out how to time this with Opencensus
    return client.mutateRowAsync(hbaseAdapter.adapt(delete))
        .thenApply(r -> null);
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    return map(asyncRequests(deletes), cf -> cf.thenApply(r -> null));
  }

  private <T> List<CompletableFuture<?>> asyncRequests(List<? extends Row> actions) {
    return map(getBatchExecutor().issueAsyncRowRequests(actions, new Object[actions.size()], null),
      FutureUtils::toCompletableFuture);
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    ReadRowsRequest request = hbaseAdapter.adapt(fromHB2Get(get));
    return client.readFlatRowsAsync(request).thenApply(BigtableAsyncTable::toResult);
  }
  
  private static Get fromHB2Get(Get get) {
    return get.isCheckExistenceOnly()
      ? addKeyOnlyFilter(get)
      : get;
  }

  private static Get addKeyOnlyFilter(Get get) {
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

  private static Result toResult(List<FlatRow> list) {
    return Adapters.FLAT_ROW_ADAPTER.adaptResponse(getSingleResult(list));
  }

  private static FlatRow getSingleResult(List<FlatRow> list) {
    switch (list.size()) {
    case 0:
      return null;
    case 1:
      return list.get(0);
    default:
      throw new IllegalStateException("Multiple responses found for Get");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<CompletableFuture<Result>> get(List<Get> gets) {
    List<Get> hb1Gets = map(gets, BigtableAsyncTable::fromHB2Get);
    return map(asyncRequests(hb1Gets), 
      (f -> (CompletableFuture<Result>) f));
  }

  @Override
  public List<CompletableFuture<Boolean>> exists(List<Get> gets) {
    List<Get> existGets = map(gets, BigtableAsyncTable::addKeyOnlyFilter);
    return map(get(existGets), cf -> cf.thenApply(r -> !r.isEmpty()));
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
  public CompletableFuture<Result> increment(Increment increment) {
    return client.readModifyWriteRowAsync(hbaseAdapter.adapt(increment))
        .thenApply(response -> Adapters.ROW_ADAPTER.adaptResponse(response.getRow()));
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations rowMutations) {
    return client.mutateRowAsync(hbaseAdapter.adapt(rowMutations))
        .thenApply(r -> null);
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    // figure out how to time this with Opencensus
    return client.mutateRowAsync(hbaseAdapter.adapt(put))
        .thenApply(r -> null);
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> puts) {
    return map(asyncRequests(puts), f -> f.thenApply(r -> null));
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    if (AbstractBigtableTable.hasWhileMatchFilter(scan.getFilter())) {
      throw new UnsupportedOperationException(
          "scanAll with while match filter is not allowed");
    }
    return client.readFlatRowsAsync(hbaseAdapter.adapt(scan))
         .thenApply(list -> map(list, Adapters.FLAT_ROW_ADAPTER::adaptResponse));
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(Scan scan) {
    LOG.trace("getScanner(Scan)");
    Span span = TRACER.spanBuilder("BigtableTable.scan").startSpan();
    try (Closeable c = TRACER.withSpan(span)) {
      com.google.cloud.bigtable.grpc.scanner.ResultScanner<FlatRow> scanner =
          client.getClient().readFlatRows(hbaseAdapter.adapt(scan));
      if (AbstractBigtableTable.hasWhileMatchFilter(scan.getFilter())) {
        return Adapters.BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER.adapt(scanner, span);
      }
      return Adapters.BIGTABLE_RESULT_SCAN_ADAPTER.adapt(scanner, span);
    } catch (final Throwable throwable) {
      LOG.error("Encountered exception when executing getScanner.", throwable);

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
        public void close() {
        }
      };
    }
  }

  public void scan(Scan scan, final ScanResultConsumer consumer) {
    if (AbstractBigtableTable.hasWhileMatchFilter(scan.getFilter())) {
      throw new UnsupportedOperationException(
          "scan with consumer and while match filter is not allowed");
    }
    client.getClient().readFlatRows(hbaseAdapter.adapt(scan), new StreamObserver<FlatRow>() {
      @Override
      public void onNext(FlatRow value) {
        consumer.onNext(Adapters.FLAT_ROW_ADAPTER.adaptResponse(value));
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

  @Override
  public CompletableFuture coprocessorService(Function arg0, ServiceCaller arg1, byte[] arg2) {
    throw new UnsupportedOperationException("coprocessorService");
  }

  @Override
  public CoprocessorServiceBuilder coprocessorService(Function arg0, ServiceCaller arg1, CoprocessorCallback arg2) {
    throw new UnsupportedOperationException("coprocessorService");
  }
}
