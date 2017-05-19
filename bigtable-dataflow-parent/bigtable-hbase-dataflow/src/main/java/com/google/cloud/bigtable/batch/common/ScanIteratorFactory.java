/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.batch.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import com.google.bigtable.repackaged.com.google.cloud.grpc.scanner.FlatRow;
import com.google.bigtable.repackaged.com.google.cloud.grpc.scanner.ResultScanner;
import com.google.bigtable.repackaged.com.google.cloud.hbase.adapters.read.FlatRowAdapter;

public final class ScanIteratorFactory {

  private static final FlatRowAdapter FLAT_ROW_ADAPTER = new FlatRowAdapter();

  /**
   * Iterates the {@link ResultScanner} via {@link ResultScanner#next()}.
   */
  private static final ScanIterator<Result> RESULT_ADVANCER = new ScanIterator<Result>() {
    private static final long serialVersionUID = 1L;

    @Override
    public Result next(ResultScanner<FlatRow> resultScanner, SplitTracker splitTracker)
        throws IOException {
      FlatRow row = resultScanner.next();
      if (row != null && !splitTracker.isAfterSplit(row.getRowKey())) {
        return FLAT_ROW_ADAPTER.adaptResponse(row);
      }
      return null;
    }

    @Override
    public boolean isCompletionMarker(Result result) {
      return result == null;
    }

    @Override
    public long getRowCount(Result result) {
      return result == null ? 0 : 1;
    }
  };

  /**
   * Iterates the {@link ResultScanner} via {@link ResultScanner#next(int)}.
   */
  private static final class ResultArrayIterator implements ScanIterator<Result[]> {
    private static final long serialVersionUID = 1L;
    private final int arraySize;

    public ResultArrayIterator(int arraySize) {
      this.arraySize = arraySize;
    }


    @Override
    public Result[] next(ResultScanner<FlatRow> resultScanner, SplitTracker splitTracker)
        throws IOException {
      List<Result> results = new ArrayList<>();
      for (int i = 0; i < arraySize; i++) {
        FlatRow row = resultScanner.next();
        if (row == null) {
          // The scan completed.
          break;
        }
        if (splitTracker.isAfterSplit(row.getRowKey())) {
          // A split occurred and the split key was before this key.
          break;
        }
        results.add(FLAT_ROW_ADAPTER.adaptResponse(row));
      }
      return results.toArray(new Result[results.size()]);
    }


    @Override
    public boolean isCompletionMarker(Result[] result) {
      return result == null || result.length == 0;
    }

    @Override
    public long getRowCount(Result[] result) {
      return result == null ? 0 : result.length;
    }
  }

  public static ScanIterator<Result> getSingleResultIterator() {
    return RESULT_ADVANCER;
  }

  public static ScanIterator<Result[]> getMultiResultIterator(int resultCount) {
    return new ResultArrayIterator(resultCount);
  }
}
