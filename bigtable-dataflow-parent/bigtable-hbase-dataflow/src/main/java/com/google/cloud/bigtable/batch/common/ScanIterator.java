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
import java.io.Serializable;

import org.apache.hadoop.hbase.client.Result;

import com.google.bigtable.repackaged.com.google.cloud.grpc.scanner.FlatRow;
import com.google.bigtable.repackaged.com.google.cloud.grpc.scanner.ResultScanner;

/**
 * Performs a {@link ResultScanner#next()} or {@link ResultScanner#next(int)}.  It also checks if
 * the ResultOutputType marks the last value in the {@link ResultScanner}.
 *
 * @param <ResultOutputType> is either a {@link Result} or {@link Result}[];
 */
public interface ScanIterator<ResultOutputType> extends Serializable {
  /**
   * Get the next unit of work.
   * @param resultScanner The {@link ResultScanner} on which to operate.
   * @param splitTracker The {@link SplitTracker} that determines if a split occurred.
   */
  ResultOutputType next(ResultScanner<FlatRow> resultScanner, SplitTracker splitTracker)
      throws IOException;

  /**
   * Is the work complete? Checks for null in the case of {@link Result}, or empty in the case of an
   * array of ResultOutputType.
   */
  boolean isCompletionMarker(ResultOutputType result);

  /**
   * This is used to figure out how many results were read. This is more useful for
   * {@link Result}[].
   */
  long getRowCount(ResultOutputType result);
}