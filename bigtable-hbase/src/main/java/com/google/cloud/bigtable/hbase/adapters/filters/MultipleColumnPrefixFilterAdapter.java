package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;

import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * An adapter to transform an HBase MultipleColumnPrefixFilter into a
 * Bigtable RowFilter with each column prefix in an interleaved stream.
 */
public class MultipleColumnPrefixFilterAdapter
    implements TypedFilterAdapter<MultipleColumnPrefixFilter> {

  ReaderExpressionHelper readerExpressionHelper = new ReaderExpressionHelper();

  @Override
  public RowFilter adapt(
      FilterAdapterContext context,
      MultipleColumnPrefixFilter filter) throws IOException {
    Interleave.Builder interleaveBuilder = Interleave.newBuilder();
    ByteArrayOutputStream outputStream = null;
    for (byte[] prefix : filter.getPrefix()) {
      if (outputStream == null) {
        outputStream = new ByteArrayOutputStream(prefix.length * 2);
      }
      outputStream.reset();

      readerExpressionHelper.writeQuotedExpression(prefix, outputStream);
      outputStream.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);

      RowFilter.Builder singlePrefixBuilder = RowFilter.newBuilder();
      singlePrefixBuilder.setColumnQualifierRegexFilter(
          ByteString.copyFrom(
              outputStream.toByteArray()));

      interleaveBuilder.addFilters(singlePrefixBuilder);
    }
    return RowFilter.newBuilder()
        .setInterleave(interleaveBuilder)
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      MultipleColumnPrefixFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
