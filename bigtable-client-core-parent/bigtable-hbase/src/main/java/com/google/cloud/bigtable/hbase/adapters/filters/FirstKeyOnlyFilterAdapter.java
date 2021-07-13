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
import com.google.cloud.bigtable.data.v2.models.Filters;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

/**
 * Adapter for FirstKeyOnlyFilter to RowFilter.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class FirstKeyOnlyFilterAdapter extends TypedFilterAdapterBase<FirstKeyOnlyFilter> {

  private static Filters.Filter LIMIT_ONE = FILTERS.limit().cellsPerRow(1);

  /** {@inheritDoc} */
  @Override
  public Filters.Filter adapt(FilterAdapterContext context, FirstKeyOnlyFilter filter) {
    return LIMIT_ONE;
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, FirstKeyOnlyFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
