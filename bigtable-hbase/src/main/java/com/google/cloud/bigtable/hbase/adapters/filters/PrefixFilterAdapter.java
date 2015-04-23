package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Adapter for HBase {@link PrefixFilter} instances.
 */
public class PrefixFilterAdapter implements TypedFilterAdapter<PrefixFilter> {
  private ReaderExpressionHelper helper = new ReaderExpressionHelper();

  @Override
  public RowFilter adapt(FilterAdapterContext context, PrefixFilter filter)
      throws IOException {
    ByteArrayOutputStream baos =
        new ByteArrayOutputStream(filter.getPrefix().length * 2);
    helper.writeQuotedRegularExpression(filter.getPrefix(), baos);
    // Unquoted all bytes:
    baos.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);
    ByteString quotedValue = ByteString.copyFrom(baos.toByteArray());
    return RowFilter.newBuilder()
        .setRowKeyRegexFilter(quotedValue)
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, PrefixFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
