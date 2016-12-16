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

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowRange.EndKeyCase;
import com.google.bigtable.v2.RowRange.StartKeyCase;
import com.google.bigtable.v2.RowSet;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

/**
 * Keeps track of Rows returned from a readRows RPC for information relevant to resuming the RPC
 * after temporary problems.
 *
 * @author sduskis
 * @version $Id: $Id
 */
class ReadRowsRequestManager {

  // Member variables from the constructor.
  private final ReadRowsRequest originalRequest;

  // The number of rows read so far.
  private long rowCount = 0;

  private ByteString lastFoundKey;

  /**
   * <p>
   * Constructor for ResumingStreamingResultScanner.
   * </p>
   * @param originalRequest a {@link ReadRowsRequest} object.
   */
  ReadRowsRequestManager(ReadRowsRequest originalRequest) {
    this.originalRequest = originalRequest;
  }

  void updateLastFoundKey(ByteString key) {
    this.lastFoundKey = key;
    rowCount++;
  }

  ReadRowsRequest buildUpdatedRequest() {
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

}
