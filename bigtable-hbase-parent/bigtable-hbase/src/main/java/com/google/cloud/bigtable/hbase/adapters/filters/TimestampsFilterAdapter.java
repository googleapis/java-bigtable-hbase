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
package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Interleave;

import org.apache.hadoop.hbase.filter.TimestampsFilter;

/**
 * Convert a TimestampsFilter into a RowFilter containing
 * interleaved timestamp range filters.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class TimestampsFilterAdapter
    extends TypedFilterAdapterBase<TimestampsFilter> {

  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(FilterAdapterContext context, TimestampsFilter filter) {
    Interleave.Builder interleaveBuilder =
        RowFilter.Interleave.newBuilder();
    for (long timestamp : filter.getTimestamps()) {
      interleaveBuilder
          .addFilters(TimestampFilterUtil.hbaseToTimestampRangeFilter(timestamp, timestamp));
    }
    return RowFilter.newBuilder().setInterleave(interleaveBuilder).build();
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      TimestampsFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
