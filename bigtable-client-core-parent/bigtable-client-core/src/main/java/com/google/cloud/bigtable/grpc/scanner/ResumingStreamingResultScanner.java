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

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.common.annotations.VisibleForTesting;

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

  private static final Logger LOG = new Logger(ResumingStreamingResultScanner.class);
  private static final Meter resultsMeter =
      BigtableClientMetrics.meter(MetricLevel.Info, "scanner.results");
  private static final Timer resultsTimer =
      BigtableClientMetrics.timer(MetricLevel.Debug, "scanner.results.latency");

  // Member variables from the constructor.
  private final ScannerRetryListener listener;
  private final Logger logger;

  private RetryOptions retryOptions;

  // The number of times we've retried after a timeout
  private int timeoutRetryCount = 0;

  private final ResponseQueueReader responseQueueReader;

  /**
   * <p>
   * Constructor for ResumingStreamingResultScanner.
   * </p>
   * @param retryOptions a {@link com.google.cloud.bigtable.config.RetryOptions} object.
   * @param originalRequest a {@link com.google.bigtable.v2.ReadRowsRequest} object.
   * @param scannerFactory a
   *          {@link com.google.cloud.bigtable.grpc.scanner.BigtableResultScannerFactory} object.
   * @param rpcMetrics a {@link com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc.RpcMetrics}
   *          object to keep track of retries and failures.
   */
  public ResumingStreamingResultScanner(ResponseQueueReader responseQueueReader,
      RetryOptions retryOptions, ReadRowsRequest originalRequest, ScannerRetryListener listener) {
    this(responseQueueReader, retryOptions, originalRequest, listener, LOG);
  }

  @VisibleForTesting
  ResumingStreamingResultScanner(ResponseQueueReader responseQueueReader, RetryOptions retryOptions,
      ReadRowsRequest originalRequest, ScannerRetryListener listener, Logger logger) {
    this.responseQueueReader = responseQueueReader;
    this.retryOptions = retryOptions;
    this.logger = logger;
    this.listener = listener;
    listener.start();
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
    while (true) {
      try {
        Timer.Context timerContext = resultsTimer.time();
        FlatRow result = responseQueueReader.getNextMergedRow();
        if (result != null) {
          resultsMeter.mark();
        }
        timerContext.close();
        if (result != null) {
          // We've had at least one successful RPC, reset the backoff and retry counter
          timeoutRetryCount = 0;
        }
        return result;
      } catch (ScanTimeoutException rte) {
        listener.resetBackoff();
        logger.info("The client could not get a response in %d ms. Retrying the scan.",
          retryOptions.getReadPartialRowTimeoutMillis());

        if (retryOptions.enableRetries()
            && ++timeoutRetryCount <= retryOptions.getMaxScanTimeoutRetries()) {
          listener.run();
        } else {
          listener.cancel();
          throw new BigtableRetriesExhaustedException(
              "Exhausted streaming retries after too many timeouts", rte);
        }
      } catch (Throwable e) {
        listener.cancel();
        throw new BigtableRetriesExhaustedException("Exhausted streaming retries.", e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public int available() {
    return responseQueueReader.available();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    listener.cancel();
  }
}
