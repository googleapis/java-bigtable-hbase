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

import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Adapter for the {@link org.apache.hadoop.hbase.filter.SingleColumnValueFilter}
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class SingleColumnValueExcludeFilterAdapter
    extends TypedFilterAdapterBase<SingleColumnValueExcludeFilter> {

  private static final String REQUIRE_SINGLE_FAMILY_MESSAGE =
      "Scan or Get operations using SingleColumnValueExcludeFilter must "
          + "have a single family specified with #addFamily().";
  private static final FilterSupportStatus UNSUPPORTED_STATUS =
      FilterSupportStatus.newNotSupported(REQUIRE_SINGLE_FAMILY_MESSAGE);

  private final SingleColumnValueFilterAdapter delegateAdapter;

  /**
   * <p>Constructor for SingleColumnValueExcludeFilterAdapter.</p>
   *
   * @param delegateAdapter a {@link com.google.cloud.bigtable.hbase.adapters.filters.SingleColumnValueFilterAdapter} object.
   */
  public SingleColumnValueExcludeFilterAdapter(SingleColumnValueFilterAdapter delegateAdapter) {
    this.delegateAdapter = delegateAdapter;
  }

  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(FilterAdapterContext context, SingleColumnValueExcludeFilter filter)
      throws IOException {
    RowFilter excludeMatchColumnFilter =
        makeExcludeMatchColumnFilter(context.getScan(), filter);
    return RowFilter.newBuilder()
        .setChain(
            Chain.newBuilder()
                .addFilters(delegateAdapter.adapt(context, filter))
                .addFilters(excludeMatchColumnFilter))
        .build();
  }

  private RowFilter makeExcludeMatchColumnFilter(
      Scan scan, SingleColumnValueExcludeFilter filter) {
    String family = Bytes.toString(scan.getFamilies()[0]);
    ByteString qualifier = ByteString.copyFrom(filter.getQualifier());
    return RowFilter.newBuilder()
        .setInterleave(
            Interleave.newBuilder()
                .addFilters(
                    RowFilter.newBuilder()
                        .setColumnRangeFilter(
                            ColumnRange.newBuilder()
                                .setFamilyName(family)
                                .setEndQualifierOpen(qualifier)))
                .addFilters(
                    RowFilter.newBuilder()
                        .setColumnRangeFilter(
                            ColumnRange.newBuilder()
                                .setFamilyName(family)
                                .setStartQualifierOpen(qualifier))))
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, SingleColumnValueExcludeFilter filter) {
    FilterSupportStatus delegateStatus = delegateAdapter.isFilterSupported(context, filter);
    if (!delegateStatus.isSupported()) {
      return delegateStatus;
    }
    // This filter can only be adapted when there's a single family.
    if (context.getScan().numFamilies() != 1) {
      return UNSUPPORTED_STATUS;
    }
    return FilterSupportStatus.SUPPORTED;
  }
}
