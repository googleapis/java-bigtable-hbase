package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.ColumnRange;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Adapter to convert a ColumnPaginationFilter to a RowFilter.
 */
public class ColumnPaginationFilterAdapter implements TypedFilterAdapter<ColumnPaginationFilter> {

  private static final FilterSupportStatus UNSUPPORTED_STATUS =
      FilterSupportStatus.newNotSupported(
          "ColumnPaginationFilter requires specifying a single column family for the Scan "
              + "when specifying a qualifier as the column offset.");

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
                      .setStartQualifierInclusive(
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
