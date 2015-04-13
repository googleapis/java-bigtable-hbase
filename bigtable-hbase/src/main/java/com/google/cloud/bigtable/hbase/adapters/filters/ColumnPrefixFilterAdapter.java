package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Adapter for a single ColumnPrefixFilter to a Cloud Bigtable RowFilter.
 */
public class ColumnPrefixFilterAdapter implements TypedFilterAdapter<ColumnPrefixFilter> {
  ReaderExpressionHelper readerExpressionHelper = new ReaderExpressionHelper();

  @Override
  public RowFilter adapt(FilterAdapterContext context, ColumnPrefixFilter filter)
      throws IOException {
    byte[] prefix = filter.getPrefix();

    // Quoting for RE2 can result in at most length * 2 characters written. Pre-allocate
    // that much space in the ByteArrayOutputStream to prevent reallocation later.
    ByteArrayOutputStream outputStream =
        new ByteArrayOutputStream(prefix.length * 2);
    readerExpressionHelper.writeQuotedRegularExpression(prefix, outputStream);
    outputStream.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);

    return RowFilter.newBuilder()
        .setColumnQualifierRegexFilter(
            ByteString.copyFrom(
                outputStream.toByteArray()))
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      ColumnPrefixFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
