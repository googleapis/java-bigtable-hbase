/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.StateCheckingResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableHBaseVeneerSettings.ClientOperationTimeouts;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableHBaseVeneerSettings.OperationTimeouts;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.Timer.Context;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class DataClientVeneerApi implements DataClientWrapper {

  private static final RowResultAdapter RESULT_ADAPTER = new RowResultAdapter();

  private final BigtableDataClient delegate;
  private final ClientOperationTimeouts clientOperationTimeouts;

  DataClientVeneerApi(
      BigtableDataClient delegate, ClientOperationTimeouts clientOperationTimeouts) {
    this.delegate = delegate;
    this.clientOperationTimeouts = clientOperationTimeouts;
  }

  interface paginatorFunction {
    public ServerStream<Result> func(Query.QueryPaginator paginator);
  }

  @Override
  public BulkMutationWrapper createBulkMutation(String tableId) {
    return new BulkMutationVeneerApi(delegate.newBulkMutationBatcher(tableId), 0);
  }

  @Override
  public BulkMutationWrapper createBulkMutation(String tableId, long closeTimeoutMilliseconds) {
    return new BulkMutationVeneerApi(
        delegate.newBulkMutationBatcher(tableId), closeTimeoutMilliseconds);
  }

  @Override
  public BulkReadWrapper createBulkRead(String tableId) {
    return new BulkReadVeneerApi(delegate, tableId, createScanCallContext());
  }

  @Override
  public ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    return delegate.mutateRowAsync(rowMutation);
  }

  @Override
  public ApiFuture<Result> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    return ApiFutures.transform(
        delegate.readModifyWriteRowAsync(readModifyWriteRow),
        new ApiFunction<Row, Result>() {
          @Override
          public Result apply(Row row) {
            return Adapters.ROW_ADAPTER.adaptResponse(row);
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation) {
    return delegate.checkAndMutateRowAsync(conditionalRowMutation);
  }

  @Override
  public ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId) {
    return delegate.sampleRowKeysAsync(tableId);
  }

  @Override
  public ApiFuture<Result> readRowAsync(
      String tableId, ByteString rowKey, @Nullable Filters.Filter filter) {

    Query query = Query.create(tableId).rowKey(rowKey).limit(1);
    if (filter != null) {
      query.filter(filter);
    }

    return ApiFutures.transform(
        delegate.readRowCallable().futureCall(query, createReadRowCallContext()),
        new ApiFunction<Row, Result>() {
          @Override
          public Result apply(Row row) {
            return Adapters.ROW_ADAPTER.adaptResponse(row);
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public ResultScanner readRows(Query.QueryPaginator paginator) {
    return new PaginatedRowResultScanner(
        paginator,
        (p) -> {
          return delegate
              .readRowsCallable(RESULT_ADAPTER)
              .call(p.getNextQuery(), createScanCallContext());
        });
  }

  @Override
  public ResultScanner readRows(Query request) {
    return new RowResultScanner(
        delegate.readRowsCallable(RESULT_ADAPTER).call(request, createScanCallContext()));
  }

  @Override
  public ApiFuture<List<Result>> readRowsAsync(Query request) {
    return delegate
        .readRowsCallable(RESULT_ADAPTER)
        .all()
        .futureCall(request, createScanCallContext());
  }

  @Override
  public void readRowsAsync(Query request, StreamObserver<Result> observer) {
    delegate
        .readRowsCallable(RESULT_ADAPTER)
        .call(request, new StreamObserverAdapter<>(observer), createScanCallContext());
  }

  // Point reads are implemented using a streaming ReadRows RPC. So timeouts need
  // to be managed
  // similar to scans below.
  private ApiCallContext createReadRowCallContext() {
    GrpcCallContext ctx = GrpcCallContext.createDefault();
    OperationTimeouts callSettings = clientOperationTimeouts.getUnaryTimeouts();

    if (callSettings.getAttemptTimeout().isPresent()) {
      ctx = ctx.withTimeout(callSettings.getAttemptTimeout().get());
    }
    // TODO: remove this after fixing it in veneer/gax
    // If the attempt timeout was overridden, it disables overall timeout limiting
    // Fix it by settings the underlying grpc deadline
    if (callSettings.getOperationTimeout().isPresent()) {
      ctx =
          ctx.withCallOptions(
              CallOptions.DEFAULT.withDeadline(
                  Deadline.after(
                      callSettings.getOperationTimeout().get().toMillis(), TimeUnit.MILLISECONDS)));
    }

    return ctx;
  }

  // Support 2 bigtable-hbase features not directly available in veneer:
  // - per attempt deadlines - vener doesn't implement deadlines for attempts. To
  // workaround this,
  // the timeouts are set per call in the ApiCallContext. However this creates a
  // separate issue of
  // over running the operation deadline, so gRPC deadline is also set.
  private GrpcCallContext createScanCallContext() {
    GrpcCallContext ctx = GrpcCallContext.createDefault();
    OperationTimeouts callSettings = clientOperationTimeouts.getScanTimeouts();

    if (callSettings.getOperationTimeout().isPresent()) {
      ctx =
          ctx.withCallOptions(
              CallOptions.DEFAULT.withDeadline(
                  Deadline.after(
                      callSettings.getOperationTimeout().get().toMillis(), TimeUnit.MILLISECONDS)));
    }
    if (callSettings.getAttemptTimeout().isPresent()) {
      ctx = ctx.withTimeout(callSettings.getAttemptTimeout().get());
    }

    return ctx;
  }

  @Override
  public void close() {
    delegate.close();
  }

  /** wraps {@link StreamObserver} onto GCJ {@link com.google.api.gax.rpc.ResponseObserver}. */
  private static class StreamObserverAdapter<T> extends StateCheckingResponseObserver<T> {
    private final StreamObserver<T> delegate;

    StreamObserverAdapter(StreamObserver<T> delegate) {
      this.delegate = delegate;
    }

    protected void onStartImpl(StreamController controller) {}

    protected void onResponseImpl(T response) {
      this.delegate.onNext(response);
    }

    protected void onErrorImpl(Throwable t) {
      this.delegate.onError(t);
    }

    protected void onCompleteImpl() {
      this.delegate.onCompleted();
    }
  }

  /** wraps {@link ServerStream} onto HBase {@link ResultScanner}. */
  private static class PaginatedRowResultScanner extends AbstractClientScanner {
    // Percentage of max number of rows allowed in the buffer
    private static final double WATERMARK_PERCENTAGE = .1;
    private static final int MIN_BYTE_BUFFER_SIZE = 100 * 1024 * 1024;
    private static final double DEFAULT_BYTE_LIMIT_PERCENTAGE = .1;
    private static final RowResultAdapter RESULT_ADAPTER = new RowResultAdapter();

    private final Meter scannerResultMeter =
        BigtableClientMetrics.meter(BigtableClientMetrics.MetricLevel.Info, "scanner.results");
    private final Timer scannerResultTimer =
        BigtableClientMetrics.timer(
            BigtableClientMetrics.MetricLevel.Debug, "scanner.results.latency");

    private ServerStream<Result> serverStream;
    private ByteString lastSeenRowKey = ByteString.EMPTY;
    private Boolean hasMore = true;
    private final Queue<Result> buffer;
    private final Query.QueryPaginator paginator;
    private final paginatorFunction wrapper;
    private final int refillSegmentWaterMark;

    private static final long maxSegmentByteSize =
        (long)
            Math.max(
                MIN_BYTE_BUFFER_SIZE,
                (Runtime.getRuntime().totalMemory() * DEFAULT_BYTE_LIMIT_PERCENTAGE));
    private long currentByteSize = 0;

    PaginatedRowResultScanner(
        Query.QueryPaginator paginator, paginatorFunction wrapper) {
      this.paginator = paginator;
      this.wrapper = wrapper;
      this.buffer = new ArrayDeque<>();
      this.refillSegmentWaterMark = (int) Math.max(1, paginator.getPageSize() * WATERMARK_PERCENTAGE);
      this.serverStream = this.wrapper.func(this.paginator);
    }

    @Override
    public Result next() {
      try (Context ignored = scannerResultTimer.time()) {
        if (this.buffer.size() < this.refillSegmentWaterMark
            && this.serverStream == null
            && hasMore) {
          this.serverStream = this.wrapper.func(this.paginator);
        }
        if (this.buffer.isEmpty() && this.serverStream != null) {
          this.waitReadRowsFuture();
        }
        scannerResultMeter.mark();
        Result result = this.buffer.poll();
        if (result != null) {
          currentByteSize -= Result.getTotalSizeOfCells(result);
        }
        return result;
      }
    }

    @Override
    public void close() {
      if (this.serverStream != null) {
        this.serverStream.cancel();
        this.serverStream = null;
      }
    }

    public boolean renewLease() {
      throw new UnsupportedOperationException("renewLease");
    }

    private void waitReadRowsFuture() {
      Iterator<Result> iterator = this.serverStream.iterator();
      while (iterator.hasNext()) {
        Result result = iterator.next();
        this.buffer.add(result);
        if (result == null || result.rawCells() == null) {
          continue;
        }
        this.lastSeenRowKey = RESULT_ADAPTER.getKey(result);
        this.currentByteSize += Result.getTotalSizeOfCells(result);
        if (this.currentByteSize >= maxSegmentByteSize) {
          break;
        }
      }
      this.hasMore = this.paginator.advance(this.lastSeenRowKey);
      this.serverStream = null;
    }
  }

  /** wraps {@link ServerStream} onto HBase {@link ResultScanner}. */
  private static class RowResultScanner extends AbstractClientScanner {
    private final Meter scannerResultMeter =
        BigtableClientMetrics.meter(BigtableClientMetrics.MetricLevel.Info, "scanner.results");
    private final Timer scannerResultTimer =
        BigtableClientMetrics.timer(
            BigtableClientMetrics.MetricLevel.Debug, "scanner.results.latency");

    private final ServerStream<Result> serverStream;
    private final Iterator<Result> iterator;

    RowResultScanner(ServerStream<Result> serverStream) {
      this.serverStream = serverStream;
      this.iterator = serverStream.iterator();
    }

    @Override
    public Result next() {
      try (Context ignored = scannerResultTimer.time()) {
        if (!iterator.hasNext()) {
          // null signals EOF
          return null;
        }

        scannerResultMeter.mark();
        return iterator.next();
      }
    }

    @Override
    public void close() {
      this.serverStream.cancel();
    }

    public boolean renewLease() {
      throw new UnsupportedOperationException("renewLease");
    }
  }
}
