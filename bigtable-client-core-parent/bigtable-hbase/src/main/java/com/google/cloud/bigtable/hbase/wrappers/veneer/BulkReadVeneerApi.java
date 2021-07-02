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
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Result;

/**
 * Adapter for sending batch reads.
 *
 * <p>This class works with {@link com.google.cloud.bigtable.hbase.BatchExecutor} to enable bulk
 * reads from the hbase api.
 *
 * <p>This class is not thread safe. It must be used on a single thread.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BulkReadVeneerApi implements BulkReadWrapper {
  private static final Executor CLEANUP_EXECUTOR =
      Executors.newSingleThreadExecutor(
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread thread = new Thread(r);
              thread.setDaemon(true);
              thread.setName("bigtable-bulkread-cleanup");
              return thread;
            }
          });
  private final BigtableDataClient client;
  private final String tableId;
  private final Map<RowFilter, Batcher<ByteString, Row>> batchers;

  // TODO: remove this once gax-java's Batcher supports asyncClose(). This will eliminate the need
  //  to track individual entries
  private final AtomicLong cleanupBarrier;
  private AtomicBoolean isClosed = new AtomicBoolean(false);
  private final GrpcCallContext callContext;

  BulkReadVeneerApi(BigtableDataClient client, String tableId, GrpcCallContext callContext) {
    this.client = client;
    this.tableId = tableId;
    this.callContext = callContext;

    this.batchers = new HashMap<>();
    this.cleanupBarrier = new AtomicLong(1);
  }

  @Override
  public void close() {
    // TODO: use closeAsync after https://github.com/googleapis/gax-java/pull/1423 is available
    if (!isClosed.compareAndSet(false, true)) {
      return;
    }
    notifyArrival();
  }

  @Override
  public ApiFuture<Result> add(ByteString rowKey, @Nullable Filters.Filter filter) {
    Preconditions.checkState(!isClosed.get());

    cleanupBarrier.incrementAndGet();

    ApiFuture<Row> rowFuture = getOrCreateBatcher(filter).add(rowKey);
    rowFuture.addListener(
        new Runnable() {
          @Override
          public void run() {
            notifyArrival();
          }
        },
        MoreExecutors.directExecutor());

    return ApiFutures.transform(
        rowFuture,
        new ApiFunction<Row, Result>() {
          @Override
          public Result apply(Row row) {
            return Adapters.ROW_ADAPTER.adaptResponse(row);
          }
        },
        MoreExecutors.directExecutor());
  }

  /** Called when a completion action (getting a result for an element or close) occurs */
  private void notifyArrival() {
    if (cleanupBarrier.decrementAndGet() == 0) {
      cleanUp();
    }
  }

  /** close all outstanding Batchers to avoid orphaned batcher warnings */
  private void cleanUp() {
    // Close batchers out of band to avoid deadlock.
    // See https://github.com/googleapis/java-bigtable-hbase/pull/2484#issuecomment-612972727 for
    // more details
    CLEANUP_EXECUTOR.execute(
        new Runnable() {
          @Override
          public void run() {
            for (Batcher<ByteString, Row> batcher : batchers.values()) {
              try {
                batcher.close();
              } catch (Throwable ignored) {
                // Ignored
              }
            }
          }
        });
  }

  @Override
  public void sendOutstanding() {
    for (Batcher<ByteString, Row> batcher : batchers.values()) {
      batcher.sendOutstanding();
    }
  }

  private Batcher<ByteString, Row> getOrCreateBatcher(@Nullable Filters.Filter filter) {
    RowFilter proto = filter == null ? null : filter.toProto();

    Batcher<ByteString, Row> batcher = batchers.get(proto);
    if (batcher == null) {
      batcher = client.newBulkReadRowsBatcher(tableId, filter, callContext);
      batchers.put(proto, batcher);
    }
    return batcher;
  }
}
