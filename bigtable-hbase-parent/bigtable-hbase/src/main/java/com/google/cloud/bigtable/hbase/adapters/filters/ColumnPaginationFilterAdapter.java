/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Adapter to convert a ColumnPaginationFilter to a RowFilter.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ColumnPaginationFilterAdapter implements TypedFilterAdapter<ColumnPaginationFilter> {

  private static final FilterSupportStatus UNSUPPORTED_STATUS =
      FilterSupportStatus.newNotSupported(
          "ColumnPaginationFilter requires specifying a single column family for the Scan "
              + "when specifying a qualifier as the column offset.");

  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(FilterAdapterContext context, ColumnPaginationFilter filter)
      throws IOException {
    if (filter.getColumnOffset() != null) {
      byte[] family = context.getScan().getFamilies()[0];
      // Include all cells starting at the qualifier scan.getColumnOffset()
      // up to limit cells.
      return createChain(
          filter,
          RowFilter.newBuilder()
              .setColumnRangeFilter(
                  ColumnRange.newBuilder()
                      .setFamilyName(Bytes.toString(family))
                      .setStartQualifierClosed(
                          ByteString.copyFrom(filter.getColumnOffset()))));
    } else if (filter.getOffset() > 0) {
      // Include starting at an integer offset up to limit cells.
      return createChain(
          filter,
          RowFilter.newBuilder()
              .setCellsPerRowOffsetFilter(filter.getOffset()));
    } else {
      // No meaningful offset supplied.
      return createChain(filter, null);
    }
  }

  /**
   * Create a filter chain that allows the latest values for each
   * qualifier, those cells that pass an option intermediate filter
   * and are less than the limit per row.
   */
  private RowFilter createChain(
      ColumnPaginationFilter filter, RowFilter.Builder intermediate) {
    Chain.Builder builder = Chain.newBuilder();
    builder.addFilters(
        RowFilter.newBuilder()
            .setCellsPerColumnLimitFilter(1));
    if (intermediate != null) {
      builder.addFilters(intermediate);
    }
    builder.addFilters(
        RowFilter.newBuilder()
            .setCellsPerRowLimitFilter(filter.getLimit()));
    return RowFilter.newBuilder().setChain(builder).build();
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      ColumnPaginationFilter filter) {
    // We require a single column family to be specified:
    int familyCount = context.getScan().numFamilies();
    if (filter.getColumnOffset() != null && familyCount != 1) {
      return UNSUPPORTED_STATUS;
    }
    return FilterSupportStatus.SUPPORTED;
  }
}
