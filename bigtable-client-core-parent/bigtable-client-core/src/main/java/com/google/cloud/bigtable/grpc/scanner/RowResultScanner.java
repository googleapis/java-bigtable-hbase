package com.google.cloud.bigtable.grpc.scanner;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.Timer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class RowResultScanner<T> implements ResultScanner<T> {

  private static final Meter resultsMeter =
      BigtableClientMetrics.meter(BigtableClientMetrics.MetricLevel.Info, "scanner.results");
  private static final Timer resultsTimer = BigtableClientMetrics
      .timer(BigtableClientMetrics.MetricLevel.Debug, "scanner.results.latency");

  private final ServerStream<T> stream;
  private final Iterator<T> iterator;
  private final T[] arr;

  public RowResultScanner(ServerStream<T> stream, T[] arr) {
    this.stream = stream;
    this.iterator = stream.iterator();
    this.arr = arr;
  }

  @Override
  public T next() throws IOException {
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
  public T[] next(int count) throws IOException {
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
    throw new UnsupportedOperationException("this is not supported");
  }

  @Override
  public void close() throws IOException {
    if (iterator.hasNext()) {
      stream.cancel();
    }
  }
}
