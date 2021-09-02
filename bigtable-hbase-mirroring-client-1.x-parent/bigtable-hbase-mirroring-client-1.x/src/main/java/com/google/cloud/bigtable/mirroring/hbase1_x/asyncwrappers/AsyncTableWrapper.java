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
 */
@InternalApi("For internal usage only")
public class AsyncTableWrapper implements ListenableCloseable {
  private final Table table;
  private final ListeningExecutorService executorService;
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

  public ListenableFuture<Result> get(final Get gets) {
    return submitTask(
        new Callable<Result>() {
          @Override
          public Result call() throws Exception {
            synchronized (table) {
              return table.get(gets);
            }
          }
        });
  }

  public ListenableFuture<Result[]> get(final List<Get> gets) {
    return submitTask(
        new Callable<Result[]>() {
          @Override
          public Result[] call() throws Exception {
            synchronized (table) {
              return table.get(gets);
            }
          }
        });
  }

  public ListenableFuture<Boolean> exists(final Get get) {
    return submitTask(
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            synchronized (table) {
              return table.exists(get);
            }
          }
        });
  }

  public ListenableFuture<boolean[]> existsAll(final List<Get> gets) {
    return submitTask(
        new Callable<boolean[]>() {
          @Override
          public boolean[] call() throws Exception {
            synchronized (table) {
              return table.existsAll(gets);
            }
          }
        });
  }

  public synchronized ListenableFuture<Void> asyncClose() {
    if (this.closeResultFuture != null) {
      return this.closeResultFuture;
    }

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
                    table.close();
                  }
                  AsyncTableWrapper.this.closeResultFuture.set(null);
                } catch (IOException e) {
                  AsyncTableWrapper.this.closeResultFuture.setException(e);
                }
              }
            },
            MoreExecutors.directExecutor());
    return this.closeResultFuture;
  }

  public AsyncResultScannerWrapper getScanner(Scan scan) throws IOException {
    AsyncResultScannerWrapper result =
        new AsyncResultScannerWrapper(
            this.table, this.table.getScanner(scan), this.executorService);
    this.pendingOperationsReferenceCounter.holdReferenceUntilClosing(result);
    return result;
  }

  public <T> ListenableFuture<T> submitTask(Callable<T> task) {
    ListenableFuture<T> future = this.executorService.submit(task);
    this.pendingOperationsReferenceCounter.holdReferenceUntilCompletion(future);
    return future;
  }

  public ListenableFuture<Void> put(final Put put) {
    return submitTask(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              table.put(put);
            }
            return null;
          }
        });
  }

  public ListenableFuture<Void> append(final Append append) {
    return submitTask(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              table.append(append);
            }
            return null;
          }
        });
  }

  public ListenableFuture<Void> increment(final Increment increment) {
    return submitTask(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              table.increment(increment);
            }
            return null;
          }
        });
  }

  public ListenableFuture<Void> mutateRow(final RowMutations rowMutations) {
    return submitTask(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              table.mutateRow(rowMutations);
            }
            return null;
          }
        });
  }

  public ListenableFuture<Void> delete(final Delete delete) {
    return submitTask(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              table.delete(delete);
            }
            return null;
          }
        });
  }

  public ListenableFuture<Void> batch(
      final List<? extends Row> operations, final Object[] results) {
    return submitTask(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              try {
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
