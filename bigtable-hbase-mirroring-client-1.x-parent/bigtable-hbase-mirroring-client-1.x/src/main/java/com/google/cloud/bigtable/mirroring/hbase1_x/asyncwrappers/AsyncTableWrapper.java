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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
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
public class AsyncTableWrapper {
  private final Table table;
  private final ListeningExecutorService executorService;

  public AsyncTableWrapper(Table table, ListeningExecutorService executorService) {
    this.table = table;
    this.executorService = executorService;
  }

  public ListenableFuture<Result> get(final Get gets) {
    return this.executorService.submit(
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
    return this.executorService.submit(
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
    return this.executorService.submit(
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
    return this.executorService.submit(
        new Callable<boolean[]>() {
          @Override
          public boolean[] call() throws Exception {
            synchronized (table) {
              return table.existsAll(gets);
            }
          }
        });
  }

  public ListenableFuture<Void> close() {
    // TODO(mwalkiewicz): There is a race condition here, we should wait until all scheduled
    // operations are finished before closing the scanner.
    return this.executorService.submit(
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            synchronized (table) {
              table.close();
            }
            return null;
          }
        });
  }

  public AsyncResultScannerWrapper getScanner(Scan scan) throws IOException {
    return new AsyncResultScannerWrapper(
        this.table, this.table.getScanner(scan), this.executorService);
  }
}
