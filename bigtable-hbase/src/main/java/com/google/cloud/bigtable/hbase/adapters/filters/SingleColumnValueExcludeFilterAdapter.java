package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.ColumnRange;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Adapter for the {@link org.apache.hadoop.hbase.filter.SingleColumnValueFilter}
 */
public class SingleColumnValueExcludeFilterAdapter
    implements TypedFilterAdapter<SingleColumnValueExcludeFilter> {

  private static final String REQUIRE_SINGLE_FAMILY_MESSAGE =
      "Scan or Get operations using SingleColumnValueExcludeFilter must "
          + "have a single family specified with #addFamily().";
  private static final FilterSupportStatus UNSUPPORTED_STATUS =
      FilterSupportStatus.newNotSupported(REQUIRE_SINGLE_FAMILY_MESSAGE);

  private final SingleColumnValueFilterAdapter delegateAdapter;

  public SingleColumnValueExcludeFilterAdapter(SingleColumnValueFilterAdapter delegateAdapter) {
    this.delegateAdapter = delegateAdapter;
  }

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
                                .setEndQualifierExclusive(qualifier)))
                .addFilters(
                    RowFilter.newBuilder()
                        .setColumnRangeFilter(
                            ColumnRange.newBuilder()
                                .setFamilyName(family)
                                .setStartQualifierExclusive(qualifier))))
        .build();
  }

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
