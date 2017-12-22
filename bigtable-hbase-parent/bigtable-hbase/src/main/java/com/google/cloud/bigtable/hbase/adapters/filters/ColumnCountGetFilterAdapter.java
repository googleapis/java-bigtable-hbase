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

import java.io.IOException;

import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.filter.RowFilters.R;

/**
 * Adapter for the ColumnCountGetFilter.
 * This filter does not work properly with Scans.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ColumnCountGetFilterAdapter extends TypedFilterAdapterBase<ColumnCountGetFilter> {

  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(FilterAdapterContext context, ColumnCountGetFilter filter)
      throws IOException {
    // This is fairly broken for all scans, but I'm simply going for bug-for-bug
    // compatible with string reader expressions.
    return R.chain(
      R.cellsPerColumnLimit(1),
      R.cellsPerRowLimit(filter.getLimit()));
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(FilterAdapterContext context,
      ColumnCountGetFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
