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
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringResultScanner;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableCloseable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;

/**
 * {@link MirroringResultScanner} schedules asynchronous next()s after synchronous operations to
 * verify consistency. HBase doesn't provide any Asynchronous API for Scanners thus we wrap
 * ResultScanner into AsyncResultScannerWrapper to enable async operations on scanners.
 */
@InternalApi("For internal usage only")
public class AsyncResultScannerWrapper implements ListenableCloseable {
  private final Table table;
  private ResultScanner scanner;
  private ListeningExecutorService executorService;
  /**
   * We are counting references to this object to be able to call {@link ResultScanner#close()} on
   * underlying scanner in a predictable way. The reference count is increased before submitting
   * each asynchronous task, and decreased after it finishes. Moreover, this object holds an
   * implicit self-reference, which in released in {@link #asyncClose()}.
   *
   * <p>In this way we are able to call ResultScanner#close() only if all scheduled tasks have
   * finished and #asyncClose() was called.
   */
  private ListenableReferenceCounter pendingOperationsReferenceCounter;

  private ListenableFuture<Void> onClosedFuture;

  public AsyncResultScannerWrapper(
      Table table, ResultScanner scanner, ListeningExecutorService executorService) {
    super();
    this.table = table;
    this.scanner = scanner;
    this.pendingOperationsReferenceCounter = new ListenableReferenceCounter();
    this.executorService = executorService;
  }

  public ListenableFuture<Result> next() {
    return submitTask(
        new Callable<Result>() {
          @Override
          public Result call() throws IOException {
            Result result;
            synchronized (table) {
              result = scanner.next();
            }
            return result;
          }
        });
  }

  public ListenableFuture<Result[]> next(final int nbRows) {
    return submitTask(
        new Callable<Result[]>() {
          @Override
          public Result[] call() throws Exception {
            Result[] result;
            synchronized (table) {
              result = scanner.next(nbRows);
            }
            return result;
          }
        });
  }

  public ListenableFuture<Boolean> renewLease() {
    return submitTask(
        new Callable<Boolean>() {
          @Override
          public Boolean call() {
            boolean result;
            synchronized (table) {
              result = scanner.renewLease();
            }
            return result;
          }
        });
  }

  public synchronized ListenableFuture<Void> asyncClose() {
    if (this.onClosedFuture != null) {
      return this.onClosedFuture;
    }

    this.pendingOperationsReferenceCounter.decrementReferenceCount();
    this.onClosedFuture = this.pendingOperationsReferenceCounter.getOnLastReferenceClosed();
    this.onClosedFuture.addListener(
        new Runnable() {
          @Override
          public void run() {
            synchronized (table) {
              scanner.close();
            }
          }
        },
        MoreExecutors.directExecutor());
    return this.onClosedFuture;
  }

  public <T> ListenableFuture<T> submitTask(Callable<T> task) {
    ListenableFuture<T> future = this.executorService.submit(task);
    this.pendingOperationsReferenceCounter.holdReferenceUntilCompletion(future);
    return future;
  }

  @Override
  public void addOnCloseListener(Runnable listener) {
    this.pendingOperationsReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(listener, MoreExecutors.directExecutor());
  }
}
