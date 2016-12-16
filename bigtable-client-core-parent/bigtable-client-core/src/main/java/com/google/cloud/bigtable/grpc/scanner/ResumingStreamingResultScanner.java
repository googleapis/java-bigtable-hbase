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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc.RpcMetrics;
import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.cloud.bigtable.metrics.Timer.Context;
import com.google.common.annotations.VisibleForTesting;

import io.grpc.Status;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A ResultScanner that attempts to resume the readRows call when it encounters gRPC INTERNAL
 * errors.
 * @author sduskis
 * @version $Id: $Id
 */
@NotThreadSafe
public class ResumingStreamingResultScanner extends AbstractBigtableResultScanner {

  private static final Logger LOG = new Logger(ResumingStreamingResultScanner.class);

  // Member variables from the constructor.
  private final ReadRowsRequestManager requestManager;
  private final BigtableResultScannerFactory<FlatRow> scannerFactory;
  private final Logger logger;
  private final RpcMetrics rpcMetrics;

  private ResultScanner<FlatRow> currentDelegate;

  private Context operationContext;
  private Context rpcContext;

  private RetryOptions retryOptions;

  // The number of times we've retried after a timeout
  private int timeoutRetryCount = 0;

  private BackOff currentErrorBackoff;
  private Sleeper sleeper = Sleeper.DEFAULT;

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
  public ResumingStreamingResultScanner(RetryOptions retryOptions, ReadRowsRequest originalRequest,
      BigtableResultScannerFactory<FlatRow> scannerFactory, RpcMetrics rpcMetrics) {
    this(retryOptions, originalRequest, scannerFactory, rpcMetrics, LOG);
  }

  @VisibleForTesting
  ResumingStreamingResultScanner(
      RetryOptions retryOptions,
      ReadRowsRequest originalRequest,
      BigtableResultScannerFactory<FlatRow> scannerFactory,
      RpcMetrics rpcMetrics,
      Logger logger) {
    this.operationContext = rpcMetrics.timeOperation();
    this.retryOptions = retryOptions;
    this.requestManager =  new ReadRowsRequestManager(originalRequest);
    this.scannerFactory = scannerFactory;
    this.logger = logger;
    this.rpcMetrics = rpcMetrics;

    this.currentDelegate = scannerFactory.createScanner(originalRequest);
    this.rpcContext = rpcMetrics.timeRpc();
  }

  /** {@inheritDoc} */
  @Override
  public FlatRow next() throws IOException {
    while (true) {
      try {
        FlatRow result = currentDelegate.next();
        if (result != null) {
          requestManager.updateLastFoundKey(result.getRowKey());
          // We've had at least one successful RPC, reset the backoff and retry counter
          currentErrorBackoff = null;
          timeoutRetryCount = 0;
        }
        return result;
      } catch (ScanTimeoutException rte) {
        closeRpcContext();
        closeCurrentDelegate();
        ReadRowsRequest newRequest = handleScanTimeout(rte);
        currentDelegate = scannerFactory.createScanner(newRequest);
        this.rpcContext = rpcMetrics.timeRpc();
      } catch (IOExceptionWithStatus ioe) {
        closeRpcContext();
        closeCurrentDelegate();
        ReadRowsRequest newRequest = handleIOException(ioe);
        currentDelegate = scannerFactory.createScanner(newRequest);
        this.rpcContext = rpcMetrics.timeRpc();
      }
    }
  }

  private synchronized ReadRowsRequest handleScanTimeout(ScanTimeoutException rte)
      throws IOException {
    logger.info("The client could not get a response in %d ms. Retrying the scan.",
      retryOptions.getReadPartialRowTimeoutMillis());

    // Reset the error backoff in case we encountered this timeout after an error.
    // Otherwise, we will likely have already exceeded the max elapsed time for the backoff
    // and won't retry after the next error.
    currentErrorBackoff = null;

    if (retryOptions.enableRetries()
        && ++timeoutRetryCount <= retryOptions.getMaxScanTimeoutRetries()) {
      return requestManager.buildUpdatedRequest();
    } else {
      rpcMetrics.markRetriesExhasted();
      throw new BigtableRetriesExhaustedException(
          "Exhausted streaming retries after too many timeouts", rte);
    }
  }

  private synchronized ReadRowsRequest handleIOException(IOExceptionWithStatus ioe)
      throws IOException {
    Status.Code code = ioe.getStatus().getCode();
    if (!retryOptions.enableRetries() || !retryOptions.isRetryable(code)) {
      rpcMetrics.markFailure();
      throw ioe;
    }

    if (currentErrorBackoff == null) {
      currentErrorBackoff = retryOptions.createBackoff();
    }
    long nextBackOffMillis = currentErrorBackoff.nextBackOffMillis();
    if (nextBackOffMillis == BackOff.STOP) {
      rpcMetrics.markRetriesExhasted();
      throw new BigtableRetriesExhaustedException("Exhausted streaming retries.", ioe);
    }

    logger.info("Reissuing scan after receiving error with status: %s.", ioe, code.name());
    sleep(nextBackOffMillis);
    return requestManager.buildUpdatedRequest();
  }


  private void sleep(long millis) throws IOException {
    try {
      sleeper.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while sleeping for resume", e);
    }
  }
  private void closeCurrentDelegate() {
    try {
      currentDelegate.close();
    } catch (IOException ioe) {
      logger.warn("Error closing scanner before reissuing request: ", ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int available() {
    return currentDelegate.available();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    closeRpcContext();
    closeOperationContext();
    currentDelegate.close();
  }

  private void closeOperationContext() {
    if (operationContext != null) {
      operationContext.close();
      operationContext = null;
    }
  }

  private void closeRpcContext() {
    if (rpcContext != null) {
      rpcContext.close();
      rpcContext = null;
    }
  }
}
