package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;

import org.apache.hadoop.hbase.filter.RandomRowFilter;

import java.io.IOException;

/**
 * Adapter for {@link org.apache.hadoop.hbase.filter.RandomRowFilter}
 */
public class RandomRowFilterAdapter implements TypedFilterAdapter<RandomRowFilter> {
  @Override
  public RowFilter adapt(FilterAdapterContext context, RandomRowFilter filter)
      throws IOException {
    return RowFilter.newBuilder()
        .setRowSampleFilter(filter.getChance())
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, RandomRowFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
