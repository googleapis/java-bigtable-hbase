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
import com.google.api.client.util.Sleeper;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowRange.EndKeyCase;
import com.google.bigtable.v2.RowRange.StartKeyCase;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc.RpcMetrics;
import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.protobuf.ByteString;

import io.grpc.Status;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Keeps track of Rows returned from a readRows RPC for information relevant to resuming the RPC
 * after temporary problems like DEADLINE_EXCEEDED.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ReadRowsRequestRetryHandler {

  // Member variables from the constructor.
  private final RetryOptions retryOptions;
  private final ReadRowsRequest originalRequest;
  private final RpcMetrics rpcMetrics;
  private final Logger logger;

  // Internal member variables.

  // The number of times we've retried after a timeout
  private AtomicInteger timeoutRetryCount = null;

  private BackOff currentErrorBackoff;
  private Sleeper sleeper = Sleeper.DEFAULT;
  // The number of rows read so far.
  private long rowCount = 0;

  private ByteString lastFoundKey;

  /**
   * <p>
   * Constructor for ResumingStreamingResultScanner.
   * </p>
   * @param retryOptions a {@link RetryOptions} object.
   * @param originalRequest a {@link ReadRowsRequest} object.
   * @param rpcMetrics a {@link com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc.RpcMetrics} object to keep track of retries and
   *          failures.
   * @param logger a {@link Logger} to log info messages about the state of retries.
   */
  public ReadRowsRequestRetryHandler(RetryOptions retryOptions, ReadRowsRequest originalRequest,
      RpcMetrics rpcMetrics, Logger logger) {
    this.originalRequest = originalRequest;
    this.retryOptions = retryOptions;
    this.rpcMetrics = rpcMetrics;
    this.logger = logger;
  }

  public void update(Row result) throws IOException {
    updateLastFoundKey(result.getKey());
    rowCount++;
    // We've had at least one successful RPC, reset the backoff and retry counter
    currentErrorBackoff = null;
    timeoutRetryCount = null;
  }

  void updateLastFoundKey(ByteString key) {
    this.lastFoundKey = key;
  }

  public ReadRowsRequest handleScanTimeout(ScanTimeoutException rte) throws IOException {
    logger.info("The client could not get a response in %d ms. Retrying the scan.",
      retryOptions.getReadPartialRowTimeoutMillis());

    // Reset the error backoff in case we encountered this timeout after an error.
    // Otherwise, we will likely have already exceeded the max elapsed time for the backoff
    // and won't retry after the next error.
    currentErrorBackoff = null;

    if (timeoutRetryCount == null) {
      timeoutRetryCount = new AtomicInteger();
    }

    if (retryOptions.enableRetries()
        && timeoutRetryCount.incrementAndGet() <= retryOptions.getMaxScanTimeoutRetries()) {
      return updateRequest();
    } else {
      rpcMetrics.incrementRetriesExhastedCounter();
      throw new BigtableRetriesExhaustedException(
          "Exhausted streaming retries after too many timeouts", rte);
    }
  }

  public ReadRowsRequest handleIOException(IOExceptionWithStatus ioe) throws IOException {
    Status.Code code = ioe.getStatus().getCode();
    if (!retryOptions.enableRetries() || !retryOptions.isRetryable(code)) {
      rpcMetrics.incrementFailureCount();
      throw ioe;
    }

    logger.info("Reissuing scan after receiving error with status: %s.", ioe, code.name());
    if (currentErrorBackoff == null) {
      currentErrorBackoff = retryOptions.createBackoff();
    }
    long nextBackOffMillis = currentErrorBackoff.nextBackOffMillis();
    if (nextBackOffMillis == BackOff.STOP) {
      rpcMetrics.incrementRetriesExhastedCounter();
      throw new BigtableRetriesExhaustedException("Exhausted streaming retries.", ioe);
    }

    sleep(nextBackOffMillis);
    return updateRequest();
  }

  private ReadRowsRequest updateRequest() {
    ReadRowsRequest.Builder newRequest = ReadRowsRequest.newBuilder()
        .setRows(filterRows())
        .setTableName(originalRequest.getTableName());

    if (originalRequest.hasFilter()) {
      newRequest.setFilter(originalRequest.getFilter());
    }

    // If the row limit is set, update it.
    long numRowsLimit = originalRequest.getRowsLimit();
    if (numRowsLimit > 0) {
      // Updates the {@code numRowsLimit} by removing the number of rows already read.
      numRowsLimit -= rowCount;

      checkArgument(numRowsLimit > 0, "The remaining number of rows must be greater than 0.");
      newRequest.setRowsLimit(numRowsLimit);
    }

    rpcMetrics.incrementRetries();
    return newRequest.build();
  }

  private RowSet filterRows() {
    RowSet originalRows = originalRequest.getRows();
    if (lastFoundKey == null) {
      return originalRows;
    }
    RowSet.Builder rowSetBuilder = RowSet.newBuilder();
    ByteBuffer lastFoundKeyByteBuffer = lastFoundKey.asReadOnlyByteBuffer();
    for (ByteString key : originalRows.getRowKeysList()) {
      if (lastFoundKeyByteBuffer.compareTo(key.asReadOnlyByteBuffer()) < 0) {
        rowSetBuilder.addRowKeys(key);
      }
    }

    for (RowRange rowRange : originalRows.getRowRangesList()) {
      EndKeyCase endKeyCase = rowRange.getEndKeyCase();
      if ((endKeyCase == EndKeyCase.END_KEY_CLOSED
              && endKeyIsAlreadyRead(lastFoundKeyByteBuffer, rowRange.getEndKeyClosed()))
          || (endKeyCase == EndKeyCase.END_KEY_OPEN
              && endKeyIsAlreadyRead(lastFoundKeyByteBuffer, rowRange.getEndKeyOpen()))) {
        continue;
      }
      RowRange newRange = rowRange;
      StartKeyCase startKeyCase = rowRange.getStartKeyCase();
      if ((startKeyCase == StartKeyCase.START_KEY_CLOSED
              && lastFoundKeyByteBuffer.compareTo(
                      rowRange.getStartKeyClosed().asReadOnlyByteBuffer())
                  >= 0)
          || (startKeyCase == StartKeyCase.START_KEY_OPEN
              && lastFoundKeyByteBuffer.compareTo(rowRange.getStartKeyOpen().asReadOnlyByteBuffer())
                  > 0)
          || startKeyCase == StartKeyCase.STARTKEY_NOT_SET) {
        newRange = rowRange.toBuilder().setStartKeyOpen(lastFoundKey).build();
      }
      rowSetBuilder.addRowRanges(newRange);
    }

    return rowSetBuilder.build();
  }

  private boolean endKeyIsAlreadyRead(ByteBuffer lastFoundKeyByteBuffer, ByteString endKey) {
    return !endKey.isEmpty()
        && lastFoundKeyByteBuffer.compareTo(endKey.asReadOnlyByteBuffer()) >= 0;
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
