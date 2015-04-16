package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Condition;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Adapt SingleColumnValueFilter instances into bigtable RowFilters.
 */
public class SingleColumnValueFilterAdapter implements TypedFilterAdapter<SingleColumnValueFilter> {

  private static final RowFilter ALL_VALUES_FILTER =
      RowFilter.newBuilder()
          .setCellsPerColumnLimitFilter(Integer.MAX_VALUE)
          .build();
  private final ReaderExpressionHelper helper = new ReaderExpressionHelper();

  public SingleColumnValueFilterAdapter() {
  }

  @Override
  public RowFilter adapt(FilterAdapterContext context, SingleColumnValueFilter filter)
      throws IOException {
    if (filter.getFilterIfMissing()) {
      return createEmitRowsWithValueFilter(filter);
    } else {
      return RowFilter.newBuilder()
          .setCondition(
              Condition.newBuilder()
                  .setPredicateFilter(createColumnSpecFilter(filter))
                  .setTrueFilter(createEmitRowsWithValueFilter(filter))
                  .setFalseFilter(ALL_VALUES_FILTER))
          .build();
    }
  }

  /**
   * Create a filter that will match a given family, qualifier, and cells per qualifier.
   */
  private RowFilter createColumnSpecFilter(SingleColumnValueFilter filter) throws IOException {
    return RowFilter.newBuilder()
        .setChain(Chain.newBuilder()
            .addFilters(RowFilter.newBuilder()
                .setFamilyNameRegexFilter(
                    Bytes.toString(helper.quoteRegularExpression(filter.getFamily()))))
            .addFilters(RowFilter.newBuilder()
                .setColumnQualifierRegexFilter(
                    ByteString.copyFrom(helper.quoteRegularExpression(filter.getQualifier()))))
            .addFilters(createVersionLimitFilter(filter)))
        .build();
  }

  /**
   * Emit a filter that will limit the number of cell versions that will be emitted.
   */
  private RowFilter createVersionLimitFilter(SingleColumnValueFilter filter) {
    return RowFilter.newBuilder()
        .setCellsPerColumnLimitFilter(
            filter.getLatestVersionOnly() ? 1 : Integer.MAX_VALUE)
        .build();
  }

  /**
   * Emit a filter that will match against a single value.
   */
  private RowFilter createValueMatchFilter(
      SingleColumnValueFilter filter) throws IOException {
    return RowFilter.newBuilder()
        .setValueRegexFilter(
            ByteString.copyFrom(
                helper.quoteRegularExpression(
                    filter.getComparator().getValue())))
        .build();
  }

  /**
   * Create a filter that will emit all cells in a row if a given qualifier
   * has a given value.
   */
  private RowFilter createEmitRowsWithValueFilter(SingleColumnValueFilter filter)
      throws IOException {
    return RowFilter.newBuilder()
        .setCondition(
            Condition.newBuilder()
                .setPredicateFilter(
                    RowFilter.newBuilder()
                        .setChain(
                            Chain.newBuilder()
                                .addFilters(createColumnSpecFilter(filter))
                                .addFilters(createValueMatchFilter(filter))))
                .setTrueFilter(ALL_VALUES_FILTER))
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, SingleColumnValueFilter filter) {
    if (!filter.getOperator().equals(CompareFilter.CompareOp.EQUAL)) {
      return FilterSupportStatus.newNotSupported(String.format(
          "CompareOp.EQUAL is the only supported SingleColumnValueFilterAdapter compareOp. "
              + "Found: '%s'",
          filter.getOperator()));
    } else if (!(filter.getComparator() instanceof BinaryComparator)) {
      return FilterSupportStatus.newNotSupported(String.format(
          "ByteArrayComparisons of type %s are not supported in SingleColumnValueFilterAdapter",
          filter.getComparator().getClass()));
    }
    return FilterSupportStatus.SUPPORTED;
  }
}
