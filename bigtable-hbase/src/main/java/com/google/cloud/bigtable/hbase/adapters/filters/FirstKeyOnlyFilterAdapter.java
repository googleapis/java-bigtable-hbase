package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;

import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.IOException;

/**
 * Adapter for FirstKeyOnlyFilter to RowFilter.
 */
public class FirstKeyOnlyFilterAdapter implements TypedFilterAdapter<FirstKeyOnlyFilter> {

  @Override
  public RowFilter adapt(FilterAdapterContext context, FirstKeyOnlyFilter filter)
      throws IOException {
    return RowFilter.newBuilder()
        .setCellsPerRowLimitFilter(1)
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(FilterAdapterContext context,
      FirstKeyOnlyFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
