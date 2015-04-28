/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.bigtable.v1.TimestampRange;
import com.google.cloud.bigtable.hbase.BigtableConstants;

import org.apache.hadoop.hbase.filter.TimestampsFilter;

/**
 * Convert a TimestampsFilter into a RowFilter containing
 * interleaved timestamp range filters.
 */
public class TimestampsFilterAdapter
    implements TypedFilterAdapter<TimestampsFilter> {

  @Override
  public RowFilter adapt(FilterAdapterContext context, TimestampsFilter filter) {
    Interleave.Builder interleaveBuilder =
        RowFilter.Interleave.newBuilder();
    for (long timestamp : filter.getTimestamps()) {
      // HBase TimestampsFilters are of the form: [N, M], however; bigtable
      // uses [N, M) to express timestamp ranges. In order to express an HBase
      // single point timestamp [M, M], we need to specify [M, M+1) to bigtable.
      long bigtableStartTimestamp =
          BigtableConstants.BIGTABLE_TIMEUNIT.convert(
              timestamp, BigtableConstants.HBASE_TIMEUNIT);
      long bigtableEndTimestamp =
          BigtableConstants.BIGTABLE_TIMEUNIT.convert(
              timestamp + 1, BigtableConstants.HBASE_TIMEUNIT);
      interleaveBuilder.addFilters(
          RowFilter.newBuilder()
              .setTimestampRangeFilter(
                  TimestampRange.newBuilder()
                      .setStartTimestampMicros(bigtableStartTimestamp)
                      .setEndTimestampMicros(bigtableEndTimestamp)));
    }
    return RowFilter.newBuilder().setInterleave(interleaveBuilder).build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      TimestampsFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
