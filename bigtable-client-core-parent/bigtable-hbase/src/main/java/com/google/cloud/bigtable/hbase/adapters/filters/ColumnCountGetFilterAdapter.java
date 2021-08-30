/*
 * Copyright 2015 Google LLC
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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import java.io.IOException;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;

/**
 * Adapter for the ColumnCountGetFilter. This filter does not work properly with Scans.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ColumnCountGetFilterAdapter extends TypedFilterAdapterBase<ColumnCountGetFilter> {

  /** {@inheritDoc} */
  @Override
  public Filter adapt(FilterAdapterContext context, ColumnCountGetFilter filter)
      throws IOException {
    // This is fairly broken for all scans, but I'm simply going for bug-for-bug
    // compatible with string reader expressions.
    return FILTERS
        .chain()
        .filter(FILTERS.limit().cellsPerColumn(1))
        .filter(FILTERS.limit().cellsPerRow(filter.getLimit()));
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, ColumnCountGetFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
