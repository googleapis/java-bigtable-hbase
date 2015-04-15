package com.google.cloud.bigtable.hbase.adapters;

import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Throwables;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.adapters.ScanAdapter.ReaderExpressionScope;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterSupportStatus;
import com.google.cloud.bigtable.hbase.adapters.filters.UnsupportedFilterException;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

/**
 * An adapter that converts HBase Filter objects into Bigtable filter reader expressions.
 */
public class FilterAdapter {

  private static final byte[] STREAM_FILTER_BYTES = Bytes.toBytes(" | ");
  private static final byte[] STREAM_INTERLEAVE_BYTES = Bytes.toBytes(" + ");

  /**
   * Provides contextual information for transforming and validating filters.
   */
  static class FilterContext {
    private final Scan scan;

    public FilterContext(Scan scan) {
      this.scan = scan;
    }

    public Scan getScan() {
      return scan;
    }
  }

  /**
   * An instance of SingleFilterAdapter knows how to adapt a single Filter class into a bigtable
   * reader expression.
   */
  static interface SingleFilterAdapter {

    /**
     * Adapt the given filter to the given output stream.
     */
    void adaptFilterTo(FilterContext context, Filter filter, OutputStream stream);

    /**
     * Determine if the given Filter is a supported filter.
     */
    FilterSupportStatus isSupported(FilterContext context, Filter filter);
  }

  /**
   * An abstract base class providing typed abstract methods for implementations
   * of SingleFilterAdapter
   */
  static abstract class AbstractSingleFilterAdapter<T extends Filter>
      implements SingleFilterAdapter {
    protected final Class<T> filterClass;

    protected AbstractSingleFilterAdapter(Class<T> filterClass) {
      this.filterClass = filterClass;
    }

    /**
     * Adapt a typed filter to the output stream.
     */
    abstract void adaptTypedFilterTo(
        FilterContext context, T filter, OutputStream outputStream) throws IOException;

    /**
     * Indicate whether a typed filter is supported.
     */
    abstract FilterSupportStatus isTypedFilterSupported(FilterContext context, T filter);

    @SuppressWarnings("unchecked")
    T unchecked(Filter filter) {
      return (T)filter;
    }

    protected T getTypedFilter(Filter filter) {
      if (isFilterAProperSublcass(filter)) {
        return unchecked(filter);
      } else {
        throw new IllegalStateException(
            String.format(
                "FilterAdapter %s cannot operate on a filter of type %s",
                getClass().getCanonicalName(),
                filter.getClass().getCanonicalName()));
      }
    }

    protected boolean isFilterAProperSublcass(Filter filter) {
      return filterClass.isAssignableFrom(filter.getClass());
    }

    @Override
    public void adaptFilterTo(FilterContext context, Filter filter, OutputStream stream) {
      Preconditions.checkState(
          isSupported(context, filter).isSupported(),
          "Unsupported Filter passed to adaptFilterTo.");

      try {
        adaptTypedFilterTo(context, getTypedFilter(filter), stream);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public FilterSupportStatus isSupported(FilterContext context, Filter filter) {
      if (isFilterAProperSublcass(filter)) {
        return isTypedFilterSupported(context, getTypedFilter(filter));
      } else {
        return FilterSupportStatus.NOT_SUPPORTED_WRONG_TYPE;
      }
    }
  }

  /**
   * Adapt {@link ValueFilter} instances
   */
  static class ValueFilterAdapter extends AbstractSingleFilterAdapter<ValueFilter> {

    private final ReaderExpressionHelper readerExpressionHelper;
    ValueFilterAdapter(ReaderExpressionHelper readerExpressionHelper) {
      super(ValueFilter.class);
      this.readerExpressionHelper = readerExpressionHelper;
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, ValueFilter filter, OutputStream outputStream) {
      try {
        outputStream.write(Bytes.toBytes("value_match({"));
        readerExpressionHelper.writeQuotedExpression(
            filter.getComparator().getValue(), outputStream);
        outputStream.write(Bytes.toBytes("})"));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(
        FilterContext context, ValueFilter filter) {
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

  static class SingleColumnValueFilterAdapter
      extends AbstractSingleFilterAdapter<SingleColumnValueFilter> {
    private final ReaderExpressionHelper readerExpressionHelper;
    protected SingleColumnValueFilterAdapter(ReaderExpressionHelper readerExpressionHelper) {
      super(SingleColumnValueFilter.class);
      this.readerExpressionHelper = readerExpressionHelper;
    }

    void writeFilterColumnSpec(SingleColumnValueFilter filter, OutputStream outputStream)
        throws IOException {
      // write col({fam:qualifier}, (all|latest))
      outputStream.write(Bytes.toBytes("col({"));
      outputStream.write(filter.getFamily());
      outputStream.write(':');
      readerExpressionHelper.writeQuotedExpression(filter.getQualifier(), outputStream);
      outputStream.write(Bytes.toBytes("}, ")); // End of regex for col({ },
      if (filter.getLatestVersionOnly()) {
        outputStream.write(Bytes.toBytes(ReaderExpressionHelper.LATEST_VERSION));
      } else {
        outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_VERSIONS));
      }
      outputStream.write(')');
    }

    void writeAllColumnsSpec(OutputStream outputStream)
        throws IOException {
      outputStream.write(Bytes.toBytes("(col({"));
          outputStream.write(ReaderExpressionHelper.ALL_FAMILIES_BYTES);
      outputStream.write(':');
      outputStream.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);
      // The value of scan#maxVersions has been applied already, we can emit all cells that we
      // see:
      outputStream.write(Bytes.toBytes("}, all))"));
    }

    void writeRowHasWithValueMatch(
        SingleColumnValueFilter filter, OutputStream outputStream) throws IOException {
      // We don't include rows that don't have a column with the given value:
      // Expect: (row_has(((col({fam:qual}, N) | (value_match({f:a}))) ? col({*}, all))
      try (ReaderExpressionScope rowHasOuter =
          new ReaderExpressionScope(outputStream, '(', ')')) {
        try (ReaderExpressionScope rowHas =
            new ReaderExpressionScope(outputStream, "row_has((","))")) {
          try (ReaderExpressionScope colWrapper =
              new ReaderExpressionScope(outputStream, '(', ')')) {
            writeFilterColumnSpec(filter, outputStream);
          }

          outputStream.write(ReaderExpressionHelper.PIPE_CHARACTER_BYTES);
          try (ReaderExpressionScope valueMatchWrapper =
              new ReaderExpressionScope(outputStream, "(value_match({", "}))")) {
            readerExpressionHelper.writeQuotedExpression(
                filter.getComparator().getValue(), outputStream);
          }
        } // End of rowHas

        outputStream.write(Bytes.toBytes(" ? "));

        // Condition is now complete - write true statement:
        try (ReaderExpressionScope trueWrapper =
            new ReaderExpressionScope(outputStream, '(', ')')) {
          writeAllColumnsSpec(outputStream);
        }
      } // End of rowHasOuter
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, SingleColumnValueFilter filter, OutputStream outputStream)
        throws IOException {
      if (filter.getFilterIfMissing()) {
        writeRowHasWithValueMatch(filter, outputStream);
      } else {
        // Include rows that don't have a column with the given value:
        try (ReaderExpressionScope rowHasOuter =
            new ReaderExpressionScope(outputStream, '(', ')')) {
          try (ReaderExpressionScope rowHas =
              new ReaderExpressionScope(outputStream, "row_has((", "))")) {
            writeFilterColumnSpec(filter, outputStream);
          }

          outputStream.write(Bytes.toBytes(" ? "));

          // Writing case where the row has the column:
          try (ReaderExpressionScope trueWrapper =
              new ReaderExpressionScope(outputStream, '(', ')')) {
            writeRowHasWithValueMatch(filter, outputStream);
          }

          outputStream.write(Bytes.toBytes(" : "));

          // Writing the case where the row does not have the column:
          try (ReaderExpressionScope falseWrapper =
              new ReaderExpressionScope(outputStream, '(', ')')) {
            writeAllColumnsSpec(outputStream);
          }
        } // End of rowHasOuter
      }
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(
        FilterContext context, SingleColumnValueFilter filter) {
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

  static class ColumnCountGetFilterAdapter
      extends AbstractSingleFilterAdapter<ColumnCountGetFilter> {

    protected ColumnCountGetFilterAdapter() {
      super(ColumnCountGetFilter.class);
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, ColumnCountGetFilter filter, OutputStream outputStream)
        throws IOException {
      outputStream.write(Bytes.toBytes("((col({"));
      outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_FAMILIES));
      outputStream.write(':');
      outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_QUALIFIERS));
      outputStream.write(Bytes.toBytes(
          String.format("}, latest)) | itemlimit(%s))", filter.getLimit())));
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(
        FilterContext context, ColumnCountGetFilter filter) {
      return FilterSupportStatus.SUPPORTED;
    }
  }

  /**
   * Adapt {@link FirstKeyOnlyFilter} instances
   */
  static class FirstKeyOnlyFilterAdapter
      extends AbstractSingleFilterAdapter<FirstKeyOnlyFilter> {

    FirstKeyOnlyFilterAdapter() {
      super(FirstKeyOnlyFilter.class);
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, FirstKeyOnlyFilter filter, OutputStream outputStream)
        throws IOException {
      outputStream.write(Bytes.toBytes("itemlimit(1)"));
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(
        FilterContext context, FirstKeyOnlyFilter filter) {
      return FilterSupportStatus.SUPPORTED;
    }
  }

  /**
   * Adapt {@link TimestampsFilter} instances
   */
  static class TimestampsFilterAdapter
      extends AbstractSingleFilterAdapter<TimestampsFilter> {
    TimestampsFilterAdapter() {
      super(TimestampsFilter.class);
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, TimestampsFilter filter, OutputStream outputStream)
        throws IOException {
      try (ScanAdapter.ReaderExpressionScope scope =
          new ScanAdapter.ReaderExpressionScope(outputStream, '(', ')')) {
        int timestampIndex = 0;
        for (Long timestamp : filter.getTimestamps()) {
          if (timestampIndex++ > 0) {
            outputStream.write(STREAM_INTERLEAVE_BYTES);
          }
          long bigtableTimestamp =
              BigtableConstants.BIGTABLE_TIMEUNIT.convert(
                  timestamp, BigtableConstants.HBASE_TIMEUNIT);
          outputStream.write(Bytes.toBytes(String.format("(ts(%s,%s))",
              bigtableTimestamp, bigtableTimestamp)));
        }
      }
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(
        FilterContext context, TimestampsFilter filter) {
      return FilterSupportStatus.SUPPORTED;
    }
  }

  static class ColumnPaginationFilterAdapter
      extends AbstractSingleFilterAdapter<ColumnPaginationFilter> {

    ColumnPaginationFilterAdapter() {
      super(ColumnPaginationFilter.class);
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, ColumnPaginationFilter filter, OutputStream outputStream)
        throws IOException {
      try (ScanAdapter.ReaderExpressionScope scope =
          new ScanAdapter.ReaderExpressionScope(outputStream, '(', ')')) {
        outputStream.write(Bytes.toBytes("(col({"));
        outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_FAMILIES));
        outputStream.write(':');
        outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_QUALIFIERS));
        outputStream.write(Bytes.toBytes("}, latest))"));
        outputStream.write(STREAM_FILTER_BYTES);
        if (filter.getOffset() > 0) {
          outputStream.write(
              Bytes.toBytes(
                  String.format("skip_items(%s)", filter.getOffset())));
          outputStream.write(STREAM_FILTER_BYTES);
        }
        outputStream.write(
            Bytes.toBytes(
                String.format(
                    "itemlimit(%s)",
                    filter.getLimit())));
      }
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(
        FilterContext context, ColumnPaginationFilter filter) {
      return FilterSupportStatus.SUPPORTED;
    }
  }

  /**
   * Adapt a {@link ColumnPrefixFilter} instance
   */
  static class ColumnPrefixFilterAdapter extends AbstractSingleFilterAdapter<ColumnPrefixFilter> {
    private final ReaderExpressionHelper readerExpressionHelper;

    protected ColumnPrefixFilterAdapter(ReaderExpressionHelper readerExpressionHelper) {
      super(ColumnPrefixFilter.class);
      this.readerExpressionHelper = readerExpressionHelper;
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, ColumnPrefixFilter filter, OutputStream outputStream)
        throws IOException {
      outputStream.write(Bytes.toBytes("(col({"));
      outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_FAMILIES));
      outputStream.write(':');
      readerExpressionHelper.writeQuotedExpression(filter.getPrefix(), outputStream);
      outputStream.write(Bytes.toBytes(".*}, "));
      outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_VERSIONS));
      outputStream.write(Bytes.toBytes("))"));
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(
        FilterContext context, ColumnPrefixFilter filter) {
      return FilterSupportStatus.SUPPORTED;
    }
  }

  /**
   * Adapt an instance of {@link MultipleColumnPrefixFilter}
   */
  static class MultipleColumnPrefixFilterAdapter
      extends AbstractSingleFilterAdapter<MultipleColumnPrefixFilter> {

    private final ReaderExpressionHelper readerExpressionHelper;

    protected MultipleColumnPrefixFilterAdapter(ReaderExpressionHelper readerExpressionHelper) {
      super(MultipleColumnPrefixFilter.class);
      this.readerExpressionHelper = readerExpressionHelper;
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, MultipleColumnPrefixFilter filter, OutputStream outputStream)
        throws IOException {
      try (ScanAdapter.ReaderExpressionScope scope =
          new ScanAdapter.ReaderExpressionScope(outputStream, '(', ')')) {
        int prefixIndex = 0;
        for (byte[] prefix : filter.getPrefix()) {
          if (prefixIndex++ > 0) {
            outputStream.write(STREAM_INTERLEAVE_BYTES);
          }
          outputStream.write(Bytes.toBytes("(col({"));
          outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_FAMILIES));
          outputStream.write(':');
          readerExpressionHelper.writeQuotedExpression(prefix, outputStream);
          outputStream.write(Bytes.toBytes(".*}, "));
          outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_VERSIONS));
          outputStream.write(Bytes.toBytes("))"));
        }
      }
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(
        FilterContext context, MultipleColumnPrefixFilter filter) {
      return FilterSupportStatus.SUPPORTED;
    }
  }

  /**
   * Adapt an instance of {@link KeyOnlyFilter}
   */
  static class KeyOnlyFilterAdapter extends AbstractSingleFilterAdapter<KeyOnlyFilter> {
    protected static final Cell TEST_CELL = new KeyValue(
        Bytes.toBytes('r'), // Row
        Bytes.toBytes('f'), // Family
        Bytes.toBytes('q'), // qualifier
        1L,
        Bytes.toBytes('v'));

    protected KeyOnlyFilterAdapter() {
      super(KeyOnlyFilter.class);
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, KeyOnlyFilter filter, OutputStream outputStream)
        throws IOException {
      outputStream.write(Bytes.toBytes("strip_value()"));
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(
        FilterContext context, KeyOnlyFilter filter) {
      if (filter.transformCell(TEST_CELL).getValueLength() != 0) {
        return FilterSupportStatus.newNotSupported(
            "KeyOnlyFilters with lenAsVal = true are not supported");
      }
      return FilterSupportStatus.SUPPORTED;
    }
  }

  static class ColumnRangeFilterAdapter extends AbstractSingleFilterAdapter<ColumnRangeFilter> {
    static final String UNSUPPORTED_EXCEPTION_MESSAGE =
        "Scan or Get operations using ColumnRangeFilter must "
            + "have a single family specified with #addFamily().";
    private final ReaderExpressionHelper helper;

    protected ColumnRangeFilterAdapter(ReaderExpressionHelper helper) {
      super(ColumnRangeFilter.class);
      this.helper = helper;
    }

    @Override
    void adaptTypedFilterTo(
        FilterContext context, ColumnRangeFilter filter, OutputStream outputStream)
        throws IOException {

      byte lowerBoundMarker = (byte) (filter.getMinColumnInclusive() ? '[' : '(');
      byte upperBoundMarker = (byte) (filter.getMaxColumnInclusive() ? ']' : ')');
      byte[] family = getSingleFamily(context.getScan());

      try (ReaderExpressionScope scope = new ReaderExpressionScope(outputStream, "(col(", "))")) {
        outputStream.write(lowerBoundMarker);

        outputStream.write(Bytes.toBytes("{"));
        helper.writeFilterQuotedExpression(family, outputStream);
        outputStream.write(BigtableConstants.BIGTABLE_COLUMN_SEPARATOR_BYTE);
        helper.writeFilterQuotedExpression(filter.getMinColumn(), outputStream);
        outputStream.write(Bytes.toBytes("}"));
        outputStream.write((byte) ',');

        outputStream.write(Bytes.toBytes("{"));
        helper.writeFilterQuotedExpression(family, outputStream);
        outputStream.write(BigtableConstants.BIGTABLE_COLUMN_SEPARATOR_BYTE);
        helper.writeFilterQuotedExpression(filter.getMaxColumn(), outputStream);
        outputStream.write(Bytes.toBytes("}"));

        outputStream.write(upperBoundMarker);
        outputStream.write((byte)',');
        outputStream.write(ReaderExpressionHelper.ALL_VERSIONS_BYTES);
      }
    }

    byte[] getSingleFamily(Scan scan) {
      return scan.getFamilies()[0];
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(FilterContext context, ColumnRangeFilter filter) {
      // We require a single column family to be specified:
      int familyCount = context.getScan().numFamilies();
      if (familyCount != 1) {
        return FilterSupportStatus.newNotSupported(UNSUPPORTED_EXCEPTION_MESSAGE);
      }
      return FilterSupportStatus.SUPPORTED;
    }
  }

  /**
   * A map of Class entries mapping to SingleFilterAdapter instances. Each supported Filter
   * subclass should have an entry in this map.
   */
  private Map<Class<? extends Filter>, SingleFilterAdapter> adapterMap = new HashMap<>();

  public FilterAdapter() {
    ReaderExpressionHelper readerExpressionHelper = new ReaderExpressionHelper();
    adapterMap.put(
        ValueFilter.class,
        new ValueFilterAdapter(readerExpressionHelper));
    adapterMap.put(
          SingleColumnValueFilter.class,
          new SingleColumnValueFilterAdapter(readerExpressionHelper));
    adapterMap.put(
        ColumnCountGetFilter.class,
        new ColumnCountGetFilterAdapter());
    adapterMap.put(
        ColumnPaginationFilter.class,
        new ColumnPaginationFilterAdapter());
    adapterMap.put(
        ColumnPrefixFilter.class,
        new ColumnPrefixFilterAdapter(readerExpressionHelper));
    adapterMap.put(
        FirstKeyOnlyFilter.class,
        new FirstKeyOnlyFilterAdapter());
    adapterMap.put(
        TimestampsFilter.class,
        new TimestampsFilterAdapter());
    adapterMap.put(
        KeyOnlyFilter.class,
        new KeyOnlyFilterAdapter());
    adapterMap.put(
        MultipleColumnPrefixFilter.class,
        new MultipleColumnPrefixFilterAdapter(readerExpressionHelper));
    adapterMap.put(ColumnRangeFilter.class,
        new ColumnRangeFilterAdapter(readerExpressionHelper));
  }

  /**
   * Get the adapter for the given Filter or throw an UnsupportedFilterException if one is not
   * available.
   */
  protected SingleFilterAdapter getAdapterForFilterOrThrow(Filter filter) {
    if (adapterMap.containsKey(filter.getClass())) {
      return adapterMap.get(filter.getClass());
    } else {
      throw new UnsupportedFilterException(
          ImmutableList.of(FilterSupportStatus.newUnknownFilterType(filter)));
    }
  }

  /**
   * Given a MUST_PASS_ALL FilterList, construct a pipe expression, write the equivalent
   * bigtable reader expression to the output stream.
   */
  protected void adaptFilterListTo(
      FilterContext context, FilterList filterList, OutputStream stream)  throws IOException {

    byte[] operatorBytes = STREAM_INTERLEAVE_BYTES;
    if (filterList.getOperator() == FilterList.Operator.MUST_PASS_ALL) {
      operatorBytes = STREAM_FILTER_BYTES;
    }

    try (ScanAdapter.ReaderExpressionScope scope =
        new ScanAdapter.ReaderExpressionScope(stream, '(', ')')) {
      List<Filter> filters = filterList.getFilters();
      Preconditions.checkState(filters instanceof RandomAccess);
      int filterIndex = 0;
      for (Filter filter : filters) {
        if (filterIndex++ > 0) {
          stream.write(operatorBytes);
        }
        adaptFilterTo(context, filter, stream);
      }
    }
  }

  /**
   * Adapt a single non-FilterList Filter to the given OutputStream.
   */
  protected void adaptSingleFilterTo(FilterContext context, Filter filter, OutputStream stream) {
    SingleFilterAdapter adapter = getAdapterForFilterOrThrow(filter);
    adapter.adaptFilterTo(context, filter, stream);
  }

  /**
   * Adapt a Filter or FilterList to the given OutputStream.
   */
  public void adaptFilterTo(
      Scan scan, Filter filter, OutputStream outputStream) throws IOException {
    FilterContext context = new FilterContext(scan);
    adaptFilterTo(context, filter, outputStream);
  }

  /**
   * Adapt a Filter or FilterList to the given OutputStream.
   */
  public void adaptFilterTo(
      FilterContext context, Filter filter, OutputStream outputStream) throws IOException {
    if (filter instanceof FilterList) {
      FilterList filterList = (FilterList) filter;
      adaptFilterListTo(context, filterList, outputStream);
    } else {
      adaptSingleFilterTo(context, filter, outputStream);
    }
  }

  /**
   * Recursively collect all unsupported filters contained in Filter (which may be a FilterList)
   * @param filter The filter to inspect
   * @param statuses A mutable list of status into which we will add any that indicate an
   * unsupported Filter was found.
   */
  protected void collectUnsuportedFilterStatuses(
      FilterContext context, Filter filter, List<FilterSupportStatus> statuses) {
    if (filter instanceof FilterList) {
      FilterList filterList = (FilterList) filter;
      for (Filter subFilter : filterList.getFilters()) {
        collectUnsuportedFilterStatuses(context, subFilter, statuses);
      }
    } else {
      SingleFilterAdapter adapter = adapterMap.get(filter.getClass());
      if (adapter == null) {
        statuses.add(FilterSupportStatus.newUnknownFilterType(filter));
      } else {
        FilterSupportStatus status = adapter.isSupported(context, filter);
        if (!status.isSupported()) {
          statuses.add(status);
        }
      }
    }
  }

  /**
   * Throw a new UnsupportedFilterException if the given filter cannot be adapted to bigtable
   * reader expressions.
   */
  public void throwIfUnsupportedFilter(Scan scan, Filter filter) {
    List<FilterSupportStatus> filterSupportStatuses = new ArrayList<>();
    FilterContext context = new FilterContext(scan);
    collectUnsuportedFilterStatuses(context, filter, filterSupportStatuses);
    if (!filterSupportStatuses.isEmpty()) {
      throw new UnsupportedFilterException(filterSupportStatuses);
    }
  }
}
