package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;

import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;

import java.io.IOException;

/**
 * Adapter for the ColumnCountGetFilter.
 * This filter does not work properly with Scans.
 */
public class ColumnCountGetFilterAdapter implements TypedFilterAdapter<ColumnCountGetFilter> {

  @Override
  public RowFilter adapt(FilterAdapterContext context, ColumnCountGetFilter filter)
      throws IOException {
    // This is fairly broken for all scans, but I'm simply going for bug-for-bug
    // compatible with string reader expressions.
    return RowFilter.newBuilder()
        .setChain(Chain.newBuilder()
            .addFilters(
                RowFilter.newBuilder()
                    .setCellsPerColumnLimitFilter(1))
            .addFilters(
                RowFilter.newBuilder()
                    .setCellsPerRowLimitFilter(filter.getLimit())))
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(FilterAdapterContext context,
      ColumnCountGetFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
