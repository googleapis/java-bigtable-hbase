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

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.protobuf.ByteString;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * A ResultScanner that can resume scan by invoking {@link #resume(ReadRowsRequest.Builder,
 * boolean)}.
 */
public class ResumingBigtableResultScanner
    extends AbstractBigtableResultScanner implements ResumableResultScanner<Row> {

  private static final Log LOG = LogFactory.getLog(ResumingBigtableResultScanner.class);

  private static final ByteString NEXT_ROW_SUFFIX = ByteString.copyFrom(new byte[]{0x00});
  private final BigtableResultScannerFactory scannerFactory;

  private ResultScanner<Row> delegate;

  /**
   * Construct a ByteString containing the next possible row key.
   */
  static ByteString nextRowKey(ByteString previous) {
    return previous.concat(NEXT_ROW_SUFFIX);
  }

  private ByteString lastRowKey = null;
  // The number of rows read so far.
  private long rowCount = 0;

  public ResumingBigtableResultScanner(
      ReadRowsRequest originalRequest,
      BigtableResultScannerFactory scannerFactory) {
    this.scannerFactory = scannerFactory;
    delegate = scannerFactory.createScanner(originalRequest);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public Row next() throws IOException {
    Row result = delegate.next();
    if (result != null) {
      lastRowKey = result.getKey();
      rowCount++;
    }

    return result;
  }

  @Override
  public void resume(ReadRowsRequest.Builder newRequest, boolean skipLastRow) {
    try {
      delegate.close();
    } catch (IOException ioe) {
      LOG.warn("Error closing scanner before reissuing request: ", ioe);
    }

    if (lastRowKey != null) {
      ByteString startKey = lastRowKey;
      if (skipLastRow) {
        startKey = nextRowKey(lastRowKey);
      }
      newRequest.getRowRangeBuilder().setStartKey(startKey);
    }

    // If the row limit is set, update it.
    long numRowsLimit = newRequest.getNumRowsLimit();
    if (numRowsLimit > 0) {
      // Updates the {@code numRowsLimit} by removing the number of rows already read.
      numRowsLimit -= rowCount;

      // Includes the last row read by incrementing {@code numRowsLimit} if it was reduced earlier.
      if (!skipLastRow && rowCount > 0) {
        numRowsLimit++;
      }

      checkArgument(numRowsLimit > 0, "The remaining number of rows must be greater than 0.");

      // Sets the updated {@code numRowsLimit} in {@code newRequest}.
      newRequest.setNumRowsLimit(numRowsLimit);
    }

    delegate = scannerFactory.createScanner(newRequest.build());
  }

  @VisibleForTesting
  ByteString getLastRowKey() {
    return lastRowKey;
  }

  @VisibleForTesting
  long getRowCount() {
    return rowCount;
  }
}
