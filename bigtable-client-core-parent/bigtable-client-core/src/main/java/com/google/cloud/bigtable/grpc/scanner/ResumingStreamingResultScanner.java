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

import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A ResultScanner that attempts to resume the readRows call when it encounters gRPC INTERNAL
 * errors.
 * @author sduskis
 * @version $Id: $Id
 */
@NotThreadSafe
public class ResumingStreamingResultScanner implements ResultScanner<FlatRow> {

  private static final Meter resultsMeter =
      BigtableClientMetrics.meter(MetricLevel.Info, "scanner.results");
  private static final Timer resultsTimer =
      BigtableClientMetrics.timer(MetricLevel.Debug, "scanner.results.latency");

  // Member variables from the constructor.
  private final ResponseQueueReader responseQueueReader;

  private boolean isConsumed;

  /**
   * <p>
   * Constructor for ResumingStreamingResultScanner.
   * </p>
   * @param responseQueueReader a {@link ResponseQueueReader} which queues up {@link FlatRow}s.
   */
  public ResumingStreamingResultScanner(ResponseQueueReader responseQueueReader) {
    this.responseQueueReader = responseQueueReader;
  }

  /** {@inheritDoc} */
  @Override
  public final FlatRow[] next(int count) throws IOException {
    ArrayList<FlatRow> resultList = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      FlatRow row = next();
      if (row == null) {
        break;
      }
      resultList.add(row);
    }
    return resultList.toArray(new FlatRow[resultList.size()]);
  }

  /** {@inheritDoc} */
  @Override
  public FlatRow next() throws IOException {
    Preconditions.checkState(!isConsumed, "Scanner is already closed");

    try(Timer.Context ignored = resultsTimer.time()) {
      FlatRow result = responseQueueReader.getNextMergedRow();
      if (result != null) {
        resultsMeter.mark();
      } else {
        isConsumed = true;
      }
      return result;
    } catch(RuntimeException|IOException e) {
      isConsumed = true;
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int available() {
    return responseQueueReader.available();
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    if (isConsumed) {
      return;
    }

    isConsumed = true;
    responseQueueReader.close();
  }
}
