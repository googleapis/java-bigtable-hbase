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

import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.common.base.Preconditions;

/**
 * A {@link com.google.cloud.bigtable.grpc.scanner.ResultScanner} implementation against the v2 bigtable API.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class StreamingBigtableResultScanner extends AbstractBigtableResultScanner {

  private final CancellationToken cancellationToken;
  private final ResponseQueueReader responseQueueReader;
  private final Meter resultsMeter =
      BigtableClientMetrics.meter(MetricLevel.Info, "scanner.results");
  private final Timer resultsTimer =
      BigtableClientMetrics.timer(MetricLevel.Debug, "scanner.results.latency");

  /**
   * <p>Constructor for StreamingBigtableResultScanner.</p>
   *
   * @param responseQueueReader a {@link com.google.cloud.bigtable.grpc.scanner.ResponseQueueReader} object.
   * @param cancellationToken a {@link com.google.cloud.bigtable.grpc.io.CancellationToken} object.
   */
  public StreamingBigtableResultScanner(ResponseQueueReader responseQueueReader,
      CancellationToken cancellationToken) {
    Preconditions.checkArgument(cancellationToken != null, "cancellationToken cannot be null");
    this.cancellationToken = cancellationToken;
    this.responseQueueReader = responseQueueReader;
  }

  /** {@inheritDoc} */
  @Override
  public FlatRow next() throws IOException {
    Timer.Context timerContext = resultsTimer.time();
    FlatRow row = responseQueueReader.getNextMergedRow();
    if (row != null) {
      resultsMeter.mark();
    }
    timerContext.close();
    return row;
  }

  /** {@inheritDoc} */
  @Override
  public int available() {
    return responseQueueReader.available();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    cancellationToken.cancel();
  }
}
