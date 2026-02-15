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
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStream;
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
import com.google.cloud.bigtable.hbase.util.Logger;
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
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Deadline;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.threeten.bp.Duration;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class DataClientVeneerApi implements DataClientWrapper {

  private final Logger LOG = new Logger(DataClientVeneerApi.class);

  private static final RowResultAdapter RESULT_ADAPTER = new RowResultAdapter();

  private final BigtableDataClient delegate;
  private final ClientOperationTimeouts clientOperationTimeouts;

  DataClientVeneerApi(
      BigtableDataClient delegate, ClientOperationTimeouts clientOperationTimeouts) {
    this.delegate = delegate;
    this.clientOperationTimeouts = clientOperationTimeouts;
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
  public ResultScanner readRows(Query.QueryPaginator paginator, long maxSegmentByteSize) {
    return new PaginatedRowResultScanner(
        paginator, delegate, maxSegmentByteSize, this.createScanCallContext());
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
  public void readRowsAsync(Query request, ResponseObserver<Result> observer) {
    delegate.readRowsCallable(RESULT_ADAPTER).call(request, observer, createScanCallContext());
  }

  // Point reads are implemented using a streaming ReadRows RPC. So timeouts need to be managed
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
  // - per attempt deadlines - vener doesn't implement deadlines for attempts. To workaround this,
  //   the timeouts are set per call in the ApiCallContext. However this creates a separate issue of
  //   over running the operation deadline, so gRPC deadline is also set.
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
      Duration attemptTimeout = callSettings.getAttemptTimeout().get();
      LOG.info("effective attempt timeout for scan is %s", attemptTimeout);
      ctx = ctx.withTimeout(attemptTimeout);
    }

    return ctx;
  }

  @Override
  public void close() {
    delegate.close();
  }

  /**
   * wraps {@link ServerStream} onto HBase {@link ResultScanner}. {@link PaginatedRowResultScanner}
   * gets a paginator and a {@link Query.QueryPaginator} used to get a {@link ServerStream}<{@link
   * Result}> using said paginator to iterate over pages of rows. The {@link Query.QueryPaginator}
   * pageSize property indicates the size of each page in every API call. A cache of a maximum size
   * of 1.1*pageSize and a minimum of 0.1*pageSize is held at all times. In order to avoid OOM
   * exceptions, there is a limit for the total byte size held in cache.
   */
  static class PaginatedRowResultScanner extends AbstractClientScanner {
    // Percentage of max number of rows allowed in the buffer
    private static final double WATERMARK_PERCENTAGE = .1;
    private static final RowResultAdapter RESULT_ADAPTER = new RowResultAdapter();

    private final Meter scannerResultMeter =
        BigtableClientMetrics.meter(BigtableClientMetrics.MetricLevel.Info, "scanner.results");
    private final Timer scannerResultTimer =
        BigtableClientMetrics.timer(
            BigtableClientMetrics.MetricLevel.Debug, "scanner.results.latency");

    private ByteString lastSeenRowKey = ByteString.EMPTY;
    private Boolean hasMore = true;
    private final Queue<Result> buffer;
    private final Query.QueryPaginator paginator;
    private final int refillSegmentWaterMark;

    private final BigtableDataClient dataClient;

    private final long maxSegmentByteSize;

    private long currentByteSize = 0;

    private @Nullable Future<List<Result>> future;
    private GrpcCallContext scanCallContext;

    PaginatedRowResultScanner(
        Query.QueryPaginator paginator,
        BigtableDataClient dataClient,
        long maxSegmentByteSize,
        GrpcCallContext scanCallContext) {
      this.maxSegmentByteSize = maxSegmentByteSize;

      this.paginator = paginator;
      this.dataClient = dataClient;
      this.buffer = new ArrayDeque<>();
      this.refillSegmentWaterMark =
          (int) Math.max(1, paginator.getPageSize() * WATERMARK_PERCENTAGE);
      this.scanCallContext = scanCallContext;
      this.future = fetchNextSegment();
    }

    @Override
    public Result next() {
      try (Context ignored = scannerResultTimer.time()) {
        if (this.future != null && this.future.isDone()) {
          this.consumeReadRowsFuture();
        }
        if (this.buffer.size() < this.refillSegmentWaterMark && this.future == null && hasMore) {
          future = fetchNextSegment();
        }
        if (this.buffer.isEmpty() && this.future != null) {
          this.consumeReadRowsFuture();
        }
        Result result = this.buffer.poll();
        if (result != null) {
          scannerResultMeter.mark();
          currentByteSize -= Result.getTotalSizeOfCells(result);
        }
        return result;
      }
    }

    @Override
    public void close() {
      if (this.future != null) {
        this.future.cancel(true);
      }
    }

    public boolean renewLease() {
      return true;
    }

    private Future<List<Result>> fetchNextSegment() {
      SettableFuture<List<Result>> resultsFuture = SettableFuture.create();

      dataClient
          .readRowsCallable(RESULT_ADAPTER)
          .call(
              paginator.getNextQuery(),
              new ResponseObserver<Result>() {
                private StreamController controller;
                List<Result> results = new ArrayList();

                @Override
                public void onStart(StreamController controller) {
                  this.controller = controller;
                }

                @Override
                public void onResponse(Result result) {
                  // calculate size of the response
                  currentByteSize += Result.getTotalSizeOfCells(result);
                  results.add(result);
                  if (result != null && result.rawCells() != null) {
                    lastSeenRowKey = RESULT_ADAPTER.getKey(result);
                  }

                  if (currentByteSize > maxSegmentByteSize) {
                    controller.cancel();
                    return;
                  }
                }

                @Override
                public void onError(Throwable t) {
                  if (currentByteSize > maxSegmentByteSize) {
                    onComplete();
                  } else {
                    resultsFuture.setException(t);
                  }
                }

                @Override
                public void onComplete() {
                  resultsFuture.set(results);
                }
              },
              this.scanCallContext);
      return resultsFuture;
    }

    private void consumeReadRowsFuture() {
      try {
        List<Result> results = this.future.get();
        this.buffer.addAll(results);
        this.hasMore = this.paginator.advance(this.lastSeenRowKey);
        this.future = null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        // Do nothing.
      }
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
      serverStream.cancel();
    }

    public boolean renewLease() {
      return true;
    }
  }
}
