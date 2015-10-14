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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.ExponentialBackOff.Builder;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Sleeper;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

import io.grpc.Status;

import java.io.IOException;


/**
 * A ResultScanner that attempts to resume the readRows call when it
 * encounters gRPC INTERNAL errors.
 */
public class ResumingStreamingResultScanner extends AbstractBigtableResultScanner {

  private static final Logger LOG = new Logger(ResumingStreamingResultScanner.class);

  private static final ByteString NEXT_ROW_SUFFIX = ByteString.copyFrom(new byte[]{0x00});
  private final BigtableResultScannerFactory scannerFactory;

  /**
   * Construct a ByteString containing the next possible row key.
   */
  static ByteString nextRowKey(ByteString previous) {
    return previous.concat(NEXT_ROW_SUFFIX);
  }

  private final Builder backOffBuilder;
  private final ReadRowsRequest originalRequest;
  private final RetryOptions retryOptions;

  private BackOff currentBackoff;
  private ResultScanner<Row> currentDelegate;
  private ByteString lastRowKey = null;
  private Sleeper sleeper = Sleeper.DEFAULT;
  // The number of rows read so far.
  private long rowCount = 0;

  private final Logger logger;

  public ResumingStreamingResultScanner(
    RetryOptions retryOptions,
    ReadRowsRequest originalRequest,
    BigtableResultScannerFactory scannerFactory) {
    this(retryOptions, originalRequest, scannerFactory, LOG);
  }

  @VisibleForTesting
  ResumingStreamingResultScanner(
      RetryOptions retryOptions,
      ReadRowsRequest originalRequest,
      BigtableResultScannerFactory scannerFactory,
      Logger logger) {
    Preconditions.checkArgument(
        !originalRequest.getAllowRowInterleaving(),
        "Row interleaving is not supported when using resumable streams");
    this.backOffBuilder = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(retryOptions.getInitialBackoffMillis())
        .setMaxElapsedTimeMillis(retryOptions.getMaxElaspedBackoffMillis())
        .setMultiplier(retryOptions.getBackoffMultiplier());
    this.originalRequest = originalRequest;
    this.scannerFactory = scannerFactory;
    this.currentBackoff = backOffBuilder.build();
    this.currentDelegate = scannerFactory.createScanner(originalRequest);
    this.retryOptions = retryOptions;
    this.logger = logger;
  }

  @Override
  public Row next() throws IOException {
    while (true) {
      try {
        Row result = currentDelegate.next();
        if (result != null) {
          lastRowKey = result.getKey();
          rowCount++;
        }
        // We've had at least one successful RPC, reset the backoff
        currentBackoff.reset();

        return result;
      } catch (ScanTimeoutException rte) {
        logger.warn("ReadTimeoutException: ", rte);
        backOffAndRetry(rte);
      } catch (IOExceptionWithStatus ioe) {
        logger.warn("IOExceptionWithStatus: ", ioe);
        Status.Code code = ioe.getStatus().getCode();
        if (retryOptions.isRetryableRead(code)) {
          backOffAndRetry(ioe);
        } else {
          throw ioe;
        }
      }
    }
  }

  /**
   * Backs off and reissues request.
   *
   * @param cause
   * @throws IOException
   * @throws ScanRetriesExhaustedException if retry is exhausted.
   */
  private void backOffAndRetry(IOException cause) throws IOException,
      ScanRetriesExhaustedException {
    long nextBackOff = currentBackoff.nextBackOffMillis();
    if (nextBackOff == BackOff.STOP) {
      logger.warn("RetriesExhausted: ", cause);
      throw new ScanRetriesExhaustedException(
          "Exhausted streaming retries.", cause);
    }

    sleep(nextBackOff);
    reissueRequest();
  }

  @Override
  public void close() throws IOException {
    currentDelegate.close();
  }

  private void reissueRequest() {
    try {
      currentDelegate.close();
    } catch (IOException ioe) {
      logger.warn("Error closing scanner before reissuing request: ", ioe);
    }

    ReadRowsRequest.Builder newRequest = originalRequest.toBuilder();
    if (lastRowKey != null) {
      newRequest.getRowRangeBuilder().setStartKey(nextRowKey(lastRowKey));
    }

    // If the row limit is set, update it.
    long numRowsLimit = newRequest.getNumRowsLimit();
    if (numRowsLimit > 0) {
      // Updates the {@code numRowsLimit} by removing the number of rows already read.
      numRowsLimit -= rowCount;

      checkArgument(numRowsLimit > 0, "The remaining number of rows must be greater than 0.");

      // Sets the updated {@code numRowsLimit} in {@code newRequest}.
      newRequest.setNumRowsLimit(numRowsLimit);
    }

    currentDelegate = scannerFactory.createScanner(newRequest.build());
  }

  private void sleep(long millis) throws IOException {
    try {
      sleeper.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while sleeping for resume", e);
    }
  }
}
