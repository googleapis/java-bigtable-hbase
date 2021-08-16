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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
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
public class AsyncResultScannerWrapper {
  private final Table table;
  private ResultScanner scanner;
  private ListeningExecutorService executorService;

  public AsyncResultScannerWrapper(
      Table table, ResultScanner scanner, ListeningExecutorService executorService) {
    super();
    this.table = table;
    this.scanner = scanner;
    this.executorService = executorService;
  }

  public ListenableFuture<Result> next() {
    return this.executorService.submit(
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
    return this.executorService.submit(
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
    return this.executorService.submit(
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

  public void close() {
    // TODO(mwalkiewicz): There is a race condition here, we should wait until all scheduled
    // operations are finished before closing the scanner.
    this.executorService.submit(
        new Callable<Void>() {
          @Override
          public Void call() {
            synchronized (table) {
              scanner.close();
              return null;
            }
          }
        });
  }
}
