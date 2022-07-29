/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.mirroring.core.asyncwrappers;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.utils.CallableThrowingIOAndInterruptedException;
import com.google.cloud.bigtable.mirroring.core.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.core.utils.Logger;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

/**
 * MirroringClient verifies consistency between two databases asynchronously - after the results are
 * delivered to the user. HBase Table object does not have an asynchronous API, so we simulate it by
 * wrapping the regular Table into AsyncTableWrapper.
 *
 * <p>Table instances are not thread-safe, every operation is synchronized to prevent concurrent
 * accesses to the table from different threads in the executor.
 *
 * <p>Note that the most of the class' interface is wrapped in Supplier<> as the results are only
 * used in callbacks.
 */
@InternalApi("For internal usage only")
public class AsyncTableWrapper {
  private static final Logger Log = new Logger(AsyncTableWrapper.class);
  private final Table table;
  private final ListeningExecutorService executorService;
  private final MirroringTracer mirroringTracer;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public AsyncTableWrapper(
      Table table, ListeningExecutorService executorService, MirroringTracer mirroringTracer) {
    this.table = table;
    this.executorService = executorService;
    this.mirroringTracer = mirroringTracer;
  }

  public Supplier<ListenableFuture<Result>> get(final Get gets) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOException<Result>() {
          @Override
          public Result call() throws IOException {
            Log.trace("get(Get)");
            return table.get(gets);
          }
        },
        HBaseOperation.GET);
  }

  public Supplier<ListenableFuture<Result[]>> get(final List<Get> gets) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOException<Result[]>() {
          @Override
          public Result[] call() throws IOException {
            Log.trace("get(List<Get>)");
            return table.get(gets);
          }
        },
        HBaseOperation.GET_LIST);
  }

  public Supplier<ListenableFuture<Boolean>> exists(final Get get) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOException<Boolean>() {
          @Override
          public Boolean call() throws IOException {
            Log.trace("exists(Get)");
            return table.exists(get);
          }
        },
        HBaseOperation.EXISTS);
  }

  public Supplier<ListenableFuture<boolean[]>> existsAll(final List<Get> gets) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOException<boolean[]>() {
          @Override
          public boolean[] call() throws IOException {
            Log.trace("existsAll(List<Get>)");
            return table.existsAll(gets);
          }
        },
        HBaseOperation.EXISTS_ALL);
  }

  public void close() throws IOException {
    if (this.closed.getAndSet(true)) {
      return;
    }

    try {
      this.mirroringTracer.spanFactory.wrapSecondaryOperation(
          new CallableThrowingIOException<Void>() {
            @Override
            public Void call() throws IOException {
              synchronized (table) {
                Log.trace("performing close()");
                table.close();
              }
              return null;
            }
          },
          HBaseOperation.TABLE_CLOSE);
    } finally {
      Log.trace("asyncClose() completed");
    }
  }

  public AsyncResultScannerWrapper getScanner(Scan scan) throws IOException {
    Log.trace("getScanner(Scan)");
    return new AsyncResultScannerWrapper(
        this.table.getScanner(scan), this.executorService, this.mirroringTracer);
  }

  public <T> Supplier<ListenableFuture<T>> createSubmitTaskSupplier(
      final CallableThrowingIOAndInterruptedException<T> task, final HBaseOperation operationName) {

    final Callable<T> secondaryOperationCallable =
        new Callable<T>() {
          @Override
          public T call() throws Exception {
            return AsyncTableWrapper.this.mirroringTracer.spanFactory.wrapSecondaryOperation(
                new CallableThrowingIOAndInterruptedException<T>() {
                  @Override
                  public T call() throws IOException, InterruptedException {
                    synchronized (table) {
                      return task.call();
                    }
                  }
                },
                operationName);
          }
        };

    return new Supplier<ListenableFuture<T>>() {
      @Override
      public ListenableFuture<T> get() {
        return submitTask(
            AsyncTableWrapper.this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
                secondaryOperationCallable));
      }
    };
  }

  public <T> ListenableFuture<T> submitTask(Callable<T> task) {
    return this.executorService.submit(task);
  }

  public Supplier<ListenableFuture<Void>> put(final Put put) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOException<Void>() {
          @Override
          public Void call() throws IOException {
            Log.trace("put(Put)");
            table.put(put);
            return null;
          }
        },
        HBaseOperation.PUT);
  }

  public Supplier<ListenableFuture<Void>> append(final Append append) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOException<Void>() {
          @Override
          public Void call() throws IOException {
            Log.trace("append(Append)");
            table.append(append);
            return null;
          }
        },
        HBaseOperation.APPEND);
  }

  public Supplier<ListenableFuture<Void>> increment(final Increment increment) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOException<Void>() {
          @Override
          public Void call() throws IOException {
            Log.trace("increment(Increment)");
            table.increment(increment);
            return null;
          }
        },
        HBaseOperation.INCREMENT);
  }

  public Supplier<ListenableFuture<Void>> mutateRow(final RowMutations rowMutations) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOException<Void>() {
          @Override
          public Void call() throws IOException {
            Log.trace("mutateRow(RowMutations)");
            table.mutateRow(rowMutations);
            return null;
          }
        },
        HBaseOperation.MUTATE_ROW);
  }

  public Supplier<ListenableFuture<Void>> delete(final Delete delete) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOException<Void>() {
          @Override
          public Void call() throws IOException {
            Log.trace("delete(Delete)");
            table.delete(delete);
            return null;
          }
        },
        HBaseOperation.DELETE);
  }

  public Supplier<ListenableFuture<Void>> batch(
      final List<? extends Row> operations, final Object[] results) {
    return createSubmitTaskSupplier(
        new CallableThrowingIOAndInterruptedException<Void>() {
          @Override
          public Void call() throws IOException, InterruptedException {
            Log.trace("batch(List<Row>, Object[])");
            table.batch(operations, results);
            return null;
          }
        },
        HBaseOperation.BATCH);
  }
}
