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
package com.google.cloud.bigtable.hbase.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * Defines a filter that only returns cells whose version matches a start and end (both are
 * inclusive)
 */
public class TimestampRangeFilter extends FilterBase {

  private final long startTimestampInclusive;
  private final long endTimestampInclusive;

  public TimestampRangeFilter(long startTimestamp, long endTimestamp) {
    this.startTimestampInclusive = startTimestamp;
    this.endTimestampInclusive = endTimestamp;
  }

  public long getStartTimestampInclusive() {
    return startTimestampInclusive;
  }

  public long getEndTimestampInclusive() {
    return endTimestampInclusive;
  }

  @Override
  /**
   * This is for HBase compatibility, and will not be used for Cloud Bigtable
   */
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    long timestamp = cell.getTimestamp();
    if (this.startTimestampInclusive <= timestamp && timestamp <= endTimestampInclusive) {
      return ReturnCode.INCLUDE;
    }
    return ReturnCode.SKIP;
  }
}
