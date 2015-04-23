package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Condition;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
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
  private final ValueFilterAdapter delegateAdapter;
  public SingleColumnValueFilterAdapter(ValueFilterAdapter delegateAdapter) {
    this.delegateAdapter = delegateAdapter;
  }

  @Override
  public RowFilter adapt(FilterAdapterContext context, SingleColumnValueFilter filter)
      throws IOException {
    if (filter.getFilterIfMissing()) {
      return createEmitRowsWithValueFilter(context, filter);
    } else {
      return RowFilter.newBuilder()
          .setCondition(
              Condition.newBuilder()
                  .setPredicateFilter(createColumnSpecFilter(filter))
                  .setTrueFilter(createEmitRowsWithValueFilter(context, filter))
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
   * Construct a ValueFilter for a SingleColumnValueFilter.
   */
  private ValueFilter createValueFilter(SingleColumnValueFilter filter) {
    return new ValueFilter(filter.getOperator(), filter.getComparator());
  }

  /**
   * Emit a filter that will match against a single value.
   */
  private RowFilter createValueMatchFilter(
      FilterAdapterContext context, SingleColumnValueFilter filter) throws IOException {
    ValueFilter valueFilter = createValueFilter(filter);
    return delegateAdapter.adapt(context, valueFilter);
  }

  /**
   * Create a filter that will emit all cells in a row if a given qualifier
   * has a given value.
   */
  private RowFilter createEmitRowsWithValueFilter(
      FilterAdapterContext context, SingleColumnValueFilter filter)
      throws IOException {
    return RowFilter.newBuilder()
        .setCondition(
            Condition.newBuilder()
                .setPredicateFilter(
                    RowFilter.newBuilder()
                        .setChain(
                            Chain.newBuilder()
                                .addFilters(createColumnSpecFilter(filter))
                                .addFilters(createValueMatchFilter(context, filter))))
                .setTrueFilter(ALL_VALUES_FILTER))
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, SingleColumnValueFilter filter) {
      return delegateAdapter.isFilterSupported(
          context, createValueFilter(filter));
  }
}
