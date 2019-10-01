/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.scanner;

import com.google.api.core.InternalApi;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.Timer;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A {@link ResultScanner} that wraps GCJ {@link ServerStream}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class RowResultScanner<T> implements ResultScanner<T> {

  private static final Meter resultsMeter =
      BigtableClientMetrics.meter(BigtableClientMetrics.MetricLevel.Info, "scanner.results");
  private static final Timer resultsTimer =
      BigtableClientMetrics.timer(
          BigtableClientMetrics.MetricLevel.Debug, "scanner.results.latency");

  private final ServerStream<T> stream;
  private final Iterator<T> iterator;
  private final T[] arr;

  /**
   * @param stream a Stream of type Row/FlatRow.
   * @param arr An array of type T length zero.
   */
  public RowResultScanner(ServerStream<T> stream, T[] arr) {
    this.stream = stream;
    this.iterator = stream.iterator();
    this.arr = arr;
  }

  @Override
  public T next() {
    if (!iterator.hasNext()) {
      return null;
    }

    try (Timer.Context ignored = resultsTimer.time()) {
      T result = iterator.next();
      resultsMeter.mark();
      return result;
    } catch (RuntimeException e) {
      throw e;
    }
  }

  @Override
  public T[] next(int count) {
    ArrayList<T> resultList = new ArrayList<>(count);
    for (int i = 0; iterator.hasNext() && i < count; i++) {
      T row = next();
      if (row == null) {
        break;
      }
      resultList.add(row);
    }
    return resultList.toArray(arr);
  }

  @Override
  public int available() {
    return stream.isReceiveReady() ? 1 : 0;
  }

  @Override
  public void close() {
    if (iterator.hasNext()) {
      stream.cancel();
    }
  }
}
