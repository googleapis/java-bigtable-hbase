/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableCloseable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.Callable;
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
 * delivered to the user. HBase Table object does not have an synchronous API, so we simulate it by
 * wrapping the regular Table into AsyncTableWrapper.
 *
 * <p>Table instances are not thread-safe, every operation is synchronized to prevent concurrent
 * accesses to the table from different threads in the executor.
 *
 * <p>Note that the most of the class' interface is wrapped in Supplier<> as the results are only
 * used in callbacks.
 */
@InternalApi("For internal usage only")
public class AsyncTableWrapper implements ListenableCloseable {
  private final Table table;
  private final ListeningExecutorService executorService;
  private static final Logger Log = new Logger(AsyncTableWrapper.class);
  /**
   * We are counting references to this object to be able to call {@link Table#close()} on
   * underlying table in a predictable way. The reference count is increased before submitting each
   * asynchronous task or when creating a ResultScanner, and decreased after it finishes. Moreover,
   * this object holds an implicit self-reference, which in released in {@link #asyncClose()}.
   *
   * <p>In this way we are able to call Table#close() only if all scheduled tasks have finished, all
   * scanners are closed, and #asyncClose() was called.
   */
  private final ListenableReferenceCounter pendingOperationsReferenceCounter;

  private SettableFuture<Void> closeResultFuture;

  public AsyncTableWrapper(Table table, ListeningExecutorService executorService) {
    this.table = table;
    this.executorService = executorService;
    this.pendingOperationsReferenceCounter = new ListenableReferenceCounter();
  }

  public Supplier<ListenableFuture<Result>> get(final Get gets) {
    return createSubmitTaskSupplier(
        new Callable<Result>() {
          @Override
          public Result call() throws Exception {
            synchronized (table) {
              Log.trace("get(Get)");
              return table.get(gets);
            }
          }
        });
  }

  public Supplier<ListenableFuture<Result[]>> get(final List<Get> gets) {
    return createSubmitTaskSupplier(
        new Callable<Result[]>() {
          @Override
          public Result[] call() throws Exception {
            synchronized (table) {
              Log.trace("get(List<Get>)");
              return table.get(gets);
            }
          }
        });
  }

  public Supplier<ListenableFuture<Boolean>> exists(final Get get) {
    return createSubmitTaskSupplier(
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            synchronized (table) {
              Log.trace("exists(Get)");
              return table.exists(get);
            }
          }
        });
  }

  public Supplier<ListenableFuture<boolean[]>> existsAll(final List<Get> gets) {
    return createSubmitTaskSupplier(
        new Callable<boolean[]>() {
          @Override
          public boolean[] call() throws Exception {
            synchronized (table) {
              Log.trace("existsAll(List<Get>)");
              return table.existsAll(gets);
            }
          }
        });
  }

  public synchronized ListenableFuture<Void> asyncClose() {
    Log.trace("asyncClose()");
    if (this.closeResultFuture != null) {
      return this.closeResultFuture;
    }
    Log.trace("performing asyncClose()");

    this.pendingOperationsReferenceCounter.decrementReferenceCount();
    this.closeResultFuture = SettableFuture.create();

    this.pendingOperationsReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(
            new Runnable() {
              @Override
              public void run() {
                try {
                  synchronized (table) {
                    Log.trace("performing close()");
                    table.close();
                  }
                  AsyncTableWrapper.this.closeResultFuture.set(null);
                } catch (IOException e) {
                  AsyncTableWrapper.this.closeResultFuture.setException(e);
                } finally {
                  Log.trace("asyncClose() completed");
                }
              }
            },
            MoreExecutors.directExecutor());
    return this.closeResultFuture;
  }

  public AsyncResultScannerWrapper getScanner(Scan scan) throws IOException {
    Log.trace("getScanner(Scan)");
    AsyncResultScannerWrapper result =
        new AsyncResultScannerWrapper(
            this.table, this.table.getScanner(scan), this.executorService);
    this.pendingOperationsReferenceCounter.holdReferenceUntilClosing(result);
    return result;
  }

  public <T> Supplier<ListenableFuture<T>> createSubmitTaskSupplier(final Callable<T> task) {
    return new Supplier<ListenableFuture<T>>() {
      @Override
      public ListenableFuture<T> get() {
        return submitTask(task);
      }
    };
  }

  public <T> ListenableFuture<T> submitTask(Callable<T> task) {
    ListenableFuture<T> future = this.executorService.submit(task);
    this.pendingOperationsReferenceCounter.holdReferenceUntilCompletion(future);
    return future;
  }

  public Supplier<ListenableFuture<Void>> put(final Put put) {
    return createSubmitTaskSupplier(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              Log.trace("put(Put)");
              table.put(put);
            }
            return null;
          }
        });
  }

  public Supplier<ListenableFuture<Void>> append(final Append append) {
    return createSubmitTaskSupplier(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              Log.trace("append(Append)");
              table.append(append);
            }
            return null;
          }
        });
  }

  public Supplier<ListenableFuture<Void>> increment(final Increment increment) {
    return createSubmitTaskSupplier(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              Log.trace("increment(Increment)");
              table.increment(increment);
            }
            return null;
          }
        });
  }

  public Supplier<ListenableFuture<Void>> mutateRow(final RowMutations rowMutations) {
    return createSubmitTaskSupplier(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              Log.trace("mutateRow(RowMutations)");
              table.mutateRow(rowMutations);
            }
            return null;
          }
        });
  }

  public Supplier<ListenableFuture<Void>> delete(final Delete delete) {
    return createSubmitTaskSupplier(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              Log.trace("delete(Delete)");
              table.delete(delete);
            }
            return null;
          }
        });
  }

  public Supplier<ListenableFuture<Void>> batch(
      final List<? extends Row> operations, final Object[] results) {
    return createSubmitTaskSupplier(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              try {
                Log.trace("batch(List<Row>, Object[])");
                table.batch(operations, results);
              } catch (InterruptedException e) {
                IOException exception = new InterruptedIOException();
                exception.initCause(e);
                throw exception;
              }
            }
            return null;
          }
        });
  }

  @Override
  public void addOnCloseListener(Runnable listener) {
    this.pendingOperationsReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(listener, MoreExecutors.directExecutor());
  }
}
