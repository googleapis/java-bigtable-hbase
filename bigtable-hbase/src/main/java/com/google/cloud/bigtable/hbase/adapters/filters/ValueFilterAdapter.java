package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Adapt a single HBase ValueFilter with CompareOp.EQUAL and a BinaryComparator.
 */
public class ValueFilterAdapter implements TypedFilterAdapter<ValueFilter> {

  ReaderExpressionHelper helper = new ReaderExpressionHelper();

  @Override
  public RowFilter adapt(FilterAdapterContext context, ValueFilter filter) throws IOException {
    byte[] comparatorValue = filter.getComparator().getValue();
    ByteArrayOutputStream baos =
        new ByteArrayOutputStream(comparatorValue.length * 2);
    helper.writeQuotedRegularExpression(comparatorValue, baos);
    return RowFilter.newBuilder()
        .setValueRegexFilter(
            ByteString.copyFrom(baos.toByteArray()))
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(FilterAdapterContext context,
      ValueFilter filter) {
    if (!filter.getOperator().equals(CompareFilter.CompareOp.EQUAL)) {
      return FilterSupportStatus.newNotSupported(String.format(
          "CompareOp.EQUAL is the only supported ValueFilter compareOp. Found: '%s'",
          filter.getOperator()));
    } else if (!(filter.getComparator() instanceof BinaryComparator)) {
      return FilterSupportStatus.newNotSupported(String.format(
          "ByteArrayComparisons of type %s are not supported in ValueFilter",
          filter.getComparator().getClass()));
    }
    return FilterSupportStatus.SUPPORTED;
  }
}
