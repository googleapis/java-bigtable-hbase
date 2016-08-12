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
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc.RpcMetrics;
import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.cloud.bigtable.metrics.Timer.Context;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

/**
 * A ResultScanner that attempts to resume the readRows call when it encounters gRPC INTERNAL
 * errors.
 * @author sduskis
 * @version $Id: $Id
 */
public class ResumingStreamingResultScanner extends AbstractBigtableResultScanner {

  private static final Logger LOG = new Logger(ResumingStreamingResultScanner.class);

  // Member variables from the constructor.
  private final ReadRowsRequestRetryHandler retryHandler;
  private final BigtableResultScannerFactory<ReadRowsRequest, Row> scannerFactory;
  private final Logger logger;
  private final RpcMetrics rpcMetrics;

  private ResultScanner<Row> currentDelegate;

  private Context operationContext;
  private Context rpcContext;

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
      BigtableResultScannerFactory<ReadRowsRequest, Row> scannerFactory, RpcMetrics rpcMetrics) {
    this(retryOptions, originalRequest, scannerFactory, rpcMetrics, LOG);
  }

  @VisibleForTesting
  ResumingStreamingResultScanner(
      RetryOptions retryOptions,
      ReadRowsRequest originalRequest,
      BigtableResultScannerFactory<ReadRowsRequest, Row> scannerFactory,
      RpcMetrics rpcMetrics,
      Logger logger) {
    this.operationContext = rpcMetrics.timeOperation();
    this.retryHandler = new ReadRowsRequestRetryHandler(retryOptions, originalRequest,
        rpcMetrics, logger);
    this.scannerFactory = scannerFactory;
    this.logger = logger;
    this.rpcMetrics = rpcMetrics;

    this.currentDelegate = scannerFactory.createScanner(originalRequest);
    this.rpcContext = rpcMetrics.timeRpc();
  }

  /** {@inheritDoc} */
  @Override
  public Row next() throws IOException {
    while (true) {
      try {
        Row result = currentDelegate.next();
        if (result == null) {
          close();
        } else {
          retryHandler.update(result);
        }
        return result;
      } catch (ScanTimeoutException rte) {
        closeRpcContext();
        closeCurrentDelegate();
        ReadRowsRequest newRequest = retryHandler.handleScanTimeout(rte);
        currentDelegate = scannerFactory.createScanner(newRequest);
        this.rpcContext = rpcMetrics.timeRpc();
      } catch (IOExceptionWithStatus ioe) {
        closeRpcContext();
        closeCurrentDelegate();
        ReadRowsRequest newRequest = retryHandler.handleIOException(ioe);
        currentDelegate = scannerFactory.createScanner(newRequest);
        this.rpcContext = rpcMetrics.timeRpc();
      }
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
  public void close() throws IOException {
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
