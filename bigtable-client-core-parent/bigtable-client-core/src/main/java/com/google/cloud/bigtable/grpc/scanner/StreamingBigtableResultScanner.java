/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.common.base.Preconditions;

/**
 * A {@link ResultScanner} implementation against the v2 bigtable API.
 */
public class StreamingBigtableResultScanner extends AbstractBigtableResultScanner {

  private static final Timer startToFirstMetric = BigtableSession.metrics.timer("StreamingBigtableResultScanner.start.toFirst");
  private static final Timer firstRowMetric = BigtableSession.metrics.timer("StreamingBigtableResultScanner.first");
  private static final Timer zeroRowMetric = BigtableSession.metrics.timer("StreamingBigtableResultScanner.no.rows");
  private static final Timer allRowMetric = BigtableSession.metrics.timer("StreamingBigtableResultScanner.rows");

  private final CancellationToken cancellationToken;
  private final ResponseQueueReader responseQueueReader;
  private Long startNanos;

  public StreamingBigtableResultScanner(ResponseQueueReader responseQueueReader,
      CancellationToken cancellationToken) {
    Preconditions.checkArgument(cancellationToken != null, "cancellationToken cannot be null");
    this.cancellationToken = cancellationToken;
    this.responseQueueReader = responseQueueReader;
    startNanos = System.nanoTime();
  }

  @Override
  public Row next() throws IOException {
    Timer.Context allRowMetricContext = allRowMetric.time();
    try {
      if (startNanos != null) {
        final Context firstRowTime = firstRowMetric.time();
        try {
          return responseQueueReader.getNextMergedRow();
        } finally {
          startToFirstMetric.update(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
          firstRowTime.stop();
          startNanos = null;
        }
      } else {
        return responseQueueReader.getNextMergedRow();
      }
    } finally {
      allRowMetricContext.stop();
    }
  }

  @Override
  public int available() {
    return responseQueueReader.available();
  }

  @Override
  public void close() throws IOException {
    if (startNanos != null) {
      zeroRowMetric.update(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
      startNanos = null;
    }
    cancellationToken.cancel();
  }
}
