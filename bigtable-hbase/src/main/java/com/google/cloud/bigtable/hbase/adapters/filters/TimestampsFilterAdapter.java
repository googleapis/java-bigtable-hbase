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
