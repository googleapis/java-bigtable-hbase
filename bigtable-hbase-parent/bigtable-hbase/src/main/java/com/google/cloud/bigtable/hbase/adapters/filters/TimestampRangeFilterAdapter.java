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
package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.hbase.filter.TimestampRangeFilter;

/**
 * Converts a {@link TimestampRangeFilter} into a Cloud Bigtable {@link RowFilter}.
 */
public class TimestampRangeFilterAdapter extends TypedFilterAdapterBase<TimestampRangeFilter> {

  @Override
  public RowFilter adapt(FilterAdapterContext context, TimestampRangeFilter filter) {
    return TimestampFilterUtil.hbaseToTimestampRangeFilter(
        filter.getStartTimestampInclusive(),
        filter.getEndTimestampExclusive())
        .toProto();
  }

  @Override
  public FilterSupportStatus isFilterSupported(FilterAdapterContext context,
      TimestampRangeFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
