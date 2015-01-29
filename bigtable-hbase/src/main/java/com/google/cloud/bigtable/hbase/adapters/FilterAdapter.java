package com.google.cloud.bigtable.hbase.adapters;

import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Throwables;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
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
   * FilterSupportStatus is a result type indicating whether a filter has a supported adaptation
   * to bigtable reader expressions.
   *
   * The isSupported method indicates whether the Filter is supported and if isSupport() is false
   * a reason may be provided by the adapter.
   */
  public static class FilterSupportStatus {

    /**
     * A static instance for all supported Filter adaptations.
     */
    public static final FilterSupportStatus SUPPORTED = new FilterSupportStatus(true, null);
    /**
     * Used to indicate an internal error where an adapter for a single Filter type is passed an
     * instance of an incompatible Filter type.
     */
    public static final FilterSupportStatus NOT_SUPPORTED_WRONG_TYPE =
        newNotSupported("Wrong filter type passed to adapter.");
    /**
     * Static helper to construct not support adaptations due to no adapter being available for
     * the given Filter type.
     * @param unknownFilterType The unknown filter instance.
     * @return A new FilterSupportStatus.
     */
    static FilterSupportStatus newUnknownFilterType(Filter unknownFilterType) {
      return new FilterSupportStatus(
          false,
          String.format(
              "Don't know how to adapt Filter class '%s'", unknownFilterType.getClass()));
    }

    /**
     * Generic static constructor for not supported adaptations with the stated reason.
     */
    static FilterSupportStatus newNotSupported(String reason) {
      return new FilterSupportStatus(false, reason);
    }

    private boolean isSupported;
    private String reason;

    FilterSupportStatus(boolean isSupported, String reason) {
      this.isSupported = isSupported;
      this.reason = reason;
    }

    /**
     * True if the adaptation is supported, false otherwise.
     */
    boolean isSupported() {
      return isSupported;
    }

    /**
     * The reason why the adaptation is not supported, if any.
     */
    String getReason() {
      return reason;
    }

    @Override
    public String toString() {
      return "FilterSupportStatus{" +
          "isSupported=" + isSupported +
          ", reason='" + reason + '\'' +
          '}';
    }
  }

  /**
   * An exception thrown when attempting to adapt unsupported filters.
   */
  static class UnsupportedFilterException extends UnsupportedOperationException {
    private static Joiner STATUS_JOINER = Joiner.on(", ");
    private final List<FilterSupportStatus> filterSupportStatuses;

    UnsupportedFilterException(List<FilterSupportStatus> filterSupportStatuses) {
      super(String.format(
          "Unsupported filters encountered: %s",
          STATUS_JOINER.join(filterSupportStatuses)));
      this.filterSupportStatuses = filterSupportStatuses;
    }

    /**
     * The failed FilterSupportStatus instances containing reason messages.
     */
    List<FilterSupportStatus> getFilterSupportStatuses() {
      return filterSupportStatuses;
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
    void adaptFilterTo(Filter filter, OutputStream stream);

    /**
     * Determine if the given Filter is a supported filter.
     */
    FilterSupportStatus isSupported(Filter filter);
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
    abstract void adaptTypedFilterTo(T filter, OutputStream outputStream) throws IOException;

    /**
     * Indicate whether a typed filter is supported.
     */
    abstract FilterSupportStatus isTypedFilterSupported(T filter);

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
    public void adaptFilterTo(Filter filter, OutputStream stream) {
      Preconditions.checkState(
          isSupported(filter).isSupported(),
          "Unsupported Filter passed to adaptFilterTo.");

      try {
        adaptTypedFilterTo(getTypedFilter(filter), stream);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public FilterSupportStatus isSupported(Filter filter) {
      if (isFilterAProperSublcass(filter)) {
        return isTypedFilterSupported(getTypedFilter(filter));
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
    void adaptTypedFilterTo(ValueFilter filter, OutputStream outputStream) {
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
    FilterSupportStatus isTypedFilterSupported(ValueFilter filter) {
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

    @Override
    void adaptTypedFilterTo(SingleColumnValueFilter filter, OutputStream outputStream)
        throws IOException {
      outputStream.write(Bytes.toBytes("((col({"));
      outputStream.write(filter.getFamily());
      outputStream.write(':');
      readerExpressionHelper.writeQuotedExpression(filter.getQualifier(), outputStream);
      outputStream.write(Bytes.toBytes("}, "));
      if (filter.getLatestVersionOnly()) {
        outputStream.write(Bytes.toBytes(ReaderExpressionHelper.LATEST_VERSION));
      } else {
        outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_VERSIONS));
      }
      outputStream.write(Bytes.toBytes(")) | value_match({"));
      readerExpressionHelper.writeQuotedExpression(
          filter.getComparator().getValue(), outputStream);
      outputStream.write(Bytes.toBytes("}))"));
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(SingleColumnValueFilter filter) {
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
    private final ReaderExpressionHelper readerExpressionHelper;

    protected ColumnCountGetFilterAdapter(ReaderExpressionHelper readerExpressionHelper) {
      super(ColumnCountGetFilter.class);
      this.readerExpressionHelper = readerExpressionHelper;
    }

    @Override
    void adaptTypedFilterTo(ColumnCountGetFilter filter, OutputStream outputStream)
        throws IOException {
      outputStream.write(Bytes.toBytes("((col({"));
      outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_FAMILIES));
      outputStream.write(':');
      outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_QUALIFIERS));
      outputStream.write(Bytes.toBytes(
          String.format("}, latest)) | itemlimit(%s))", filter.getLimit())));
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(ColumnCountGetFilter filter) {
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
    void adaptTypedFilterTo(FirstKeyOnlyFilter filter, OutputStream outputStream)
        throws IOException {
      outputStream.write(Bytes.toBytes("itemlimit(1)"));
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(FirstKeyOnlyFilter filter) {
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
    void adaptTypedFilterTo(TimestampsFilter filter, OutputStream outputStream) throws IOException {
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
    FilterSupportStatus isTypedFilterSupported(TimestampsFilter filter) {
      return FilterSupportStatus.SUPPORTED;
    }
  }

  static class ColumnPaginationFilterAdapter
      extends AbstractSingleFilterAdapter<ColumnPaginationFilter> {

    ColumnPaginationFilterAdapter() {
      super(ColumnPaginationFilter.class);
    }

    @Override
    void adaptTypedFilterTo(ColumnPaginationFilter filter, OutputStream outputStream)
        throws IOException {
      try (ScanAdapter.ReaderExpressionScope scope =
          new ScanAdapter.ReaderExpressionScope(outputStream, '(', ')')) {
        outputStream.write(Bytes.toBytes("(col({"));
        outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_FAMILIES));
        outputStream.write(':');
        outputStream.write(Bytes.toBytes(ReaderExpressionHelper.ALL_QUALIFIERS));
        outputStream.write(Bytes.toBytes("}, latest))"));
        outputStream.write(STREAM_FILTER_BYTES);
        outputStream.write(
            Bytes.toBytes(
                String.format(
                    "skip_items(%s) | itemlimit(%s)",
                    filter.getOffset(),
                    filter.getLimit())));
      }
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(ColumnPaginationFilter filter) {
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
    void adaptTypedFilterTo(ColumnPrefixFilter filter, OutputStream outputStream)
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
    FilterSupportStatus isTypedFilterSupported(ColumnPrefixFilter filter) {
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
    void adaptTypedFilterTo(MultipleColumnPrefixFilter filter, OutputStream outputStream)
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
    FilterSupportStatus isTypedFilterSupported(MultipleColumnPrefixFilter filter) {
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
    void adaptTypedFilterTo(KeyOnlyFilter filter, OutputStream outputStream) throws IOException {
      outputStream.write(Bytes.toBytes("strip_value()"));
    }

    @Override
    FilterSupportStatus isTypedFilterSupported(KeyOnlyFilter filter) {
      if (filter.transformCell(TEST_CELL).getValueLength() != 0) {
        return FilterSupportStatus.newNotSupported(
            "KeyOnlyFilters with lenAsVal = true are not supported");
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
    // TODO: Re-enable when this settles
    // adapterMap.put(
    //      SingleColumnValueFilter.class,
    //      new SingleColumnValueFilterAdapter(readerExpressionHelper));
    adapterMap.put(
        ColumnCountGetFilter.class,
        new ColumnCountGetFilterAdapter(readerExpressionHelper));
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
  protected void adaptFilterListTo(FilterList filterList, OutputStream stream)
      throws IOException {

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
        adaptFilterTo(filter, stream);
      }
    }
  }

  /**
   * Adapt a single non-FilterList Filter to the given OutputStream.
   */
  protected void adaptSingleFilterTo(Filter filter, OutputStream stream) {
    SingleFilterAdapter adapter = getAdapterForFilterOrThrow(filter);
    adapter.adaptFilterTo(filter, stream);
  }

  /**
   * Adapt a Filter or FilterList to the given OutputStream.
   */
  public void adaptFilterTo(Filter filter, OutputStream outputStream) throws IOException {
    if (filter instanceof FilterList) {
      FilterList filterList = (FilterList) filter;
      adaptFilterListTo(filterList, outputStream);
    } else {
      adaptSingleFilterTo(filter, outputStream);
    }
  }

  /**
   * Recursively collect all unsupported filters contained in Filter (which may be a FilterList)
   * @param filter The filter to inspect
   * @param statuses A mutable list of status into which we will add any that indicate an
   * unsupported Filter was found.
   */
  protected void collectUnsuportedFilterStatuses(
      Filter filter, List<FilterSupportStatus> statuses) {
    if (filter instanceof FilterList) {
      FilterList filterList = (FilterList) filter;
      for (Filter subFilter : filterList.getFilters()) {
        collectUnsuportedFilterStatuses(subFilter, statuses);
      }
    } else {
      SingleFilterAdapter adapter = adapterMap.get(filter.getClass());
      if (adapter == null) {
        statuses.add(FilterSupportStatus.newUnknownFilterType(filter));
      } else {
        FilterSupportStatus status = adapter.isSupported(filter);
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
  public void throwIfUnsupportedFilter(Filter filter) {
    List<FilterSupportStatus> filterSupportStatuses = new ArrayList<>();
    collectUnsuportedFilterStatuses(filter, filterSupportStatuses);
    if (!filterSupportStatuses.isEmpty()) {
      throw new UnsupportedFilterException(filterSupportStatuses);
    }
  }
}
