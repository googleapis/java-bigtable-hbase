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
import com.google.api.client.util.Sleeper;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.grpc.Status;

import java.io.IOException;

/**
 * A ResultScanner that attempts to retry the readRows call when it
 * encounters gRPC INTERNAL errors.
 */
public class RetryingStreamingResultScanner extends ResumingBigtableResultScanner {

  private static final Log LOG = LogFactory.getLog(RetryingStreamingResultScanner.class);

  private final Builder backOffBuilder;
  private final ReadRowsRequest originalRequest;
  private final boolean retryOnDeadlineExceeded;

  private BackOff currentBackoff;
  private Sleeper sleeper = Sleeper.DEFAULT;

  public RetryingStreamingResultScanner(
      RetryOptions retryOptions,
      ReadRowsRequest originalRequest,
      BigtableResultScannerFactory scannerFactory) {
    super(originalRequest, scannerFactory);
    checkArgument(
        !originalRequest.getAllowRowInterleaving(),
        "Row interleaving is not supported when using resumable streams");
    retryOnDeadlineExceeded = retryOptions.retryOnDeadlineExceeded();
    this.backOffBuilder = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(retryOptions.getInitialBackoffMillis())
        .setMaxElapsedTimeMillis(retryOptions.getMaxElaspedBackoffMillis())
        .setMultiplier(retryOptions.getBackoffMultiplier());
    this.originalRequest = originalRequest;
    this.currentBackoff = backOffBuilder.build();
  }

  @Override
  public Row next() throws IOException {
    while (true) {
      try {
        Row result = super.next();

        // We've had at least one successful RPC, reset the backoff
        currentBackoff.reset();

        return result;
      } catch (ScanTimeoutException rte) {
        LOG.warn("ReadTimeoutException: ", rte);
        backOffAndRetry(rte);
      } catch (IOExceptionWithStatus ioe) {
        LOG.warn("IOExceptionWithStatus: ", ioe);
        Status.Code code = ioe.getStatus().getCode();
        if (code == Status.INTERNAL.getCode()
            || code == Status.UNAVAILABLE.getCode()
            || code == Status.ABORTED.getCode()
            || (retryOnDeadlineExceeded && code == Status.DEADLINE_EXCEEDED.getCode())) {
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
      LOG.warn("RetriesExhausted: ", cause);
      throw new ScanRetriesExhaustedException(
          "Exhausted streaming retries.", cause);
    }

    sleep(nextBackOff);
    resume(originalRequest.toBuilder(), true);
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
