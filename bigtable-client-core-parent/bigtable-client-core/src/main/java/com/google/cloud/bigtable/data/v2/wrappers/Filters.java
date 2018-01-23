/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.data.v2.wrappers;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.core.InternalApi;
import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.TimestampRange;
import com.google.bigtable.v2.ValueRange;
import com.google.cloud.bigtable.data.v2.internal.RegexUtil;
import com.google.protobuf.ByteString;

/**
 * A Fluent DSL to create a hierarchy of filters for the CheckAndMutateRow RPCs and ReadRows Query.
 *
 * <p>Intended usage is to statically import, or in case of conflict assign the static variable F
 * and use its fluent API to build filters.
 *
 * <p>Sample code:
 *
 * <pre>{@code
 * import static com.google.cloud.bigtable.data.v2.wrappers.Filters.F;
 *
 * void main() {
 *   // Build the filter expression
 *   RowFilter filter = F.chain()
 *     .filter(F.qualifier().regex("prefix.*"))
 *     .filter(F.limit().cellsPerRow(10));
 * }
 *
 * }</pre>
 */
public final class Filters {
  /** Entry point into the DSL. */
  public static final Filters F = new Filters();

  private static final SimpleFilter PASS =
      new SimpleFilter(RowFilter.newBuilder().setPassAllFilter(true).build());
  private static final SimpleFilter BLOCK =
      new SimpleFilter(RowFilter.newBuilder().setBlockAllFilter(true).build());
  private static final SimpleFilter SINK =
      new SimpleFilter(RowFilter.newBuilder().setSink(true).build());
  private static final SimpleFilter STRIP_VALUE =
      new SimpleFilter(RowFilter.newBuilder().setStripValueTransformer(true).build());

  private Filters() {}

  /**
   * Creates an empty chain filter lidst. Filters can be added to the chain by invoking {@link
   * ChainFilter#filter(Filter)}.
   *
   * <p>The elements of "filters" are chained together to process the input row:
   *
   * <pre>{@code in row -> filter0 -> intermediate row -> filter1 -> ... -> filterN -> out row}
   * </pre>
   *
   * The full chain is executed atomically.
   */
  public ChainFilter chain() {
    return new ChainFilter();
  }

  /**
   * Creates an empty interleave filter list. Filters can be added to the interleave by invoking
   * {@link InterleaveFilter#filter(Filter)}.
   *
   * <p>The elements of "filters" all process a copy of the input row, and the results are pooled,
   * sorted, and combined into a single output row. If multiple cells are produced with the same
   * column and timestamp, they will all appear in the output row in an unspecified mutual order.
   * The full chain is executed atomically.
   */
  public InterleaveFilter interleave() {
    return new InterleaveFilter();
  }

  /**
   * Creates an empty condition filter. The filter results of the predicate can be configured by
   * invoking {@link ConditionFilter#then(Filter)} and {@link ConditionFilter#otherwise(Filter)}.
   *
   * <p>A RowFilter which evaluates one of two possible RowFilters, depending on whether or not a
   * predicate RowFilter outputs any cells from the input row.
   *
   * <p>IMPORTANT NOTE: The predicate filter does not execute atomically with the {@link
   * ConditionFilter#then(Filter)} and {@link ConditionFilter#otherwise(Filter)} (Filter)} filters,
   * which may lead to inconsistent or unexpected results. Additionally, {@link ConditionFilter} may
   * have poor performance, especially when filters are set for the {@link
   * ConditionFilter#otherwise(Filter)}.
   */
  public ConditionFilter condition(Filter predicate) {
    return new ConditionFilter(predicate);
  }

  /** Returns the builder for row key related filters. */
  public KeyFilter key() {
    return new KeyFilter();
  }

  /** Returns the builder for column family related filters. */
  public FamilyFilter family() {
    return new FamilyFilter();
  }

  /** Returns the builder for column qualifier related filters. */
  public QualifierFilter qualifier() {
    return new QualifierFilter();
  }

  /** Returns the builder for timestamp related filters. */
  public TimestampFilter timestamp() {
    return new TimestampFilter();
  }

  /** Returns the builder for value related filters. */
  public ValueFilter value() {
    return new ValueFilter();
  }

  /** Returns the builder for offset related filters. */
  public OffsetFilter offset() {
    return new OffsetFilter();
  }

  /** Returns the builder for limit related filters. */
  public LimitFilter limit() {
    return new LimitFilter();
  }

  // Miscellaneous filters without a clear target.
  /** Matches all cells, regardless of input. Functionally equivalent to having no filter. */
  public Filter pass() {
    return PASS;
  }

  /**
   * Does not match any cells, regardless of input. Useful for temporarily disabling just part of a
   * filter.
   */
  public Filter block() {
    return BLOCK;
  }

  /**
   * Outputs all cells directly to the output of the read rather than to any parent filter. For
   * advanced usage, see comments in
   * https://github.com/googleapis/googleapis/blob/master/google/bigtable/v2/data.proto for more
   * details.
   */
  public Filter sink() {
    return SINK;
  }

  /**
   * Applies the given label to all cells in the output row. This allows the caller to determine
   * which results were produced from which part of the filter.
   *
   * <p>Due to a technical limitation, it is not currently possible to apply multiple labels to a
   * cell. As a result, a {@link ChainFilter} may have no more than one sub-filter which contains a
   * label. It is okay for an {@link InterleaveFilter} to contain multiple labels, as they will be
   * applied to separate copies of the input. This may be relaxed in the future.
   */
  public Filter label(String label) {
    return new SimpleFilter(RowFilter.newBuilder().setApplyLabelTransformer(label).build());
  }

  // Implementations of target specific filters.
  /** DSL for adding filters to a chain. */
  public static class ChainFilter extends Filter {

    final RowFilter.Chain.Builder builder;

    public ChainFilter() {
      this(RowFilter.Chain.newBuilder());
    }

    ChainFilter(RowFilter.Chain.Builder builder) {
      this.builder = builder;
    }

    /** Add a filter to chain. */
    public ChainFilter filter(Filter filter) {
      builder.addFilters(filter.toProto());
      return this;
    }

    @InternalApi
    @Override
    public RowFilter toProto() {
      if (builder.getFiltersCount() == 1) {
        return builder.getFilters(0);
      } else {
        return RowFilter.newBuilder().setChain(builder.build()).build();
      }
    }

    @Override
    public ChainFilter clone() {
      return new ChainFilter(builder.build().toBuilder());
    }
  }

  /** DSL for adding filters to the interleave list. */
  public static class InterleaveFilter extends Filter {
    RowFilter.Interleave.Builder builder = RowFilter.Interleave.newBuilder();

    public InterleaveFilter filter(Filter filter) {
      builder.addFilters(filter.toProto());
      return this;
    }

    @InternalApi
    @Override
    public RowFilter toProto() {
      if (builder.getFiltersCount() == 1) {
        return builder.getFilters(0);
      } else {
        return RowFilter.newBuilder().setInterleave(builder.build()).build();
      }
    }
  }

  /** DSL for configuring a conditional filter. */
  public static class ConditionFilter extends Filter {
    RowFilter.Condition.Builder builder = RowFilter.Condition.newBuilder();

    ConditionFilter(Filter predicate) {
      builder.setPredicateFilter(predicate.toProto());
    }

    /** Sets (replaces) the filter to apply when the predicate is true. */
    public ConditionFilter then(Filter filter) {
      builder.setTrueFilter(filter.toProto());
      return this;
    }

    /** Sets (replaces) the filter to apply when the predicate is false. */
    public ConditionFilter otherwise(Filter filter) {
      builder.setFalseFilter(filter.toProto());
      return this;
    }

    @InternalApi
    @Override
    public RowFilter toProto() {
      return RowFilter.newBuilder().setCondition(builder.build()).build();
    }
  }

  public static class KeyFilter {
    /**
     * Matches only cells from rows whose keys satisfy the given <a
     * href="https://github.com/google/re2/wiki/Syntax>RE2 regex</a>. In other words, passes through
     * the entire row when the key matches, and otherwise produces an empty row. Note that, since
     * row keys can contain arbitrary bytes, the `\C` escape sequence must be used if a true
     * wildcard is desired. The `.` character will not match the new line character `\n`, which may
     * be present in a binary key.
     */
    public Filter regex(String regex) {
      return regex(ByteString.copyFromUtf8(regex));
    }

    /**
     * Matches only cells from rows whose keys satisfy the given <a
     * href="https://github.com/google/re2/wiki/Syntax>RE2 regex</a>. In other words, passes through
     * the entire row when the key matches, and otherwise produces an empty row. Note that, since
     * row keys can contain arbitrary bytes, the `\C` escape sequence must be used if a true
     * wildcard is desired. The `.` character will not match the new line character `\n`, which may
     * be present in a binary key.
     */
    public Filter regex(ByteString regex) {
      return new SimpleFilter(
          RowFilter.newBuilder().setRowKeyRegexFilter(regex).build());
    }

    /**
     * Matches only cells from rows whose keys equal the value. In other words, passes through the
     * entire row when the key matches, and otherwise produces an empty row.
     */
    public Filter exactMatch(ByteString value) {
      return new SimpleFilter(
          RowFilter.newBuilder().setRowKeyRegexFilter(RegexUtil.literalRegex(value)).build());
    }

    /**
     * Matches all cells from a row with `probability`, and matches no cells from the row with
     * probability 1-`probability`.
     */
    public Filter sample(double probability) {
      return new SimpleFilter(RowFilter.newBuilder().setRowSampleFilter(probability).build());
    }
  }

  public static class FamilyFilter {

    /**
     * Matches only cells from columns whose families satisfy the given <a
     * href="https://github.com/google/re2/wiki/Syntax>RE2 regex</a>. For technical reasons, the
     * regex must not contain the `:` character, even if it is not being used as a literal. Note
     * that, since column families cannot contain the new line character `\n`, it is sufficient to
     * use `.` as a full wildcard when matching column family names.
     */
    public Filter regex(String regex) {
      return new SimpleFilter(RowFilter.newBuilder().setFamilyNameRegexFilter(regex).build());
    }

    /** Matches only cells from columns whose families match the value. */
    public Filter exactMatch(String value) {
      return new SimpleFilter(
          RowFilter.newBuilder().setFamilyNameRegexFilter(RegexUtil.literalRegex(value)).build());
    }
  }

  /**
   * Matches only cells from columns within the given range.
   */
  public static class QualifierRangeFilter extends Filter{
    private ColumnRange.Builder range = ColumnRange.newBuilder();

    public QualifierRangeFilter(String family) {
      range.setFamilyName(Preconditions.checkNotNull(family));
    }

    public QualifierRangeFilter startClosed(ByteString value) {
      range.setStartQualifierClosed(value);
      return this;
    }

    public QualifierRangeFilter startOpen(ByteString value) {
      range.setStartQualifierOpen(value);
      return this;
    }

    public QualifierRangeFilter endClosed(ByteString value) {
      range.setEndQualifierClosed(value);
      return this;
    }

    public QualifierRangeFilter endOpen(ByteString value) {
      range.setEndQualifierOpen(value);
      return this;
    }

    @Override
    public RowFilter toProto() {
      return RowFilter.newBuilder().setColumnRangeFilter(range.build()).build();
    }
  }

  public static class QualifierFilter {

    /**
     * Matches only cells from columns whose qualifiers satisfy the given <a
     * href="https://github.com/google/re2/wiki/Syntax>RE2 regex</a>. Note that, since column
     * qualifiers can contain arbitrary bytes, the `\C` escape sequence must be used if a true
     * wildcard is desired. The `.` character will not match the new line character `\n`, which may
     * be present in a binary qualifier.
     */
    public Filter regex(String regex) {
      return regex(ByteString.copyFromUtf8(regex));
    }

    /**
     * Matches only cells from columns whose qualifiers satisfy the given <a
     * href="https://github.com/google/re2/wiki/Syntax>RE2 regex</a>. Note that, since column
     * qualifiers can contain arbitrary bytes, the `\C` escape sequence must be used if a true
     * wildcard is desired. The `.` character will not match the new line character `\n`, which may
     * be present in a binary qualifier.
     */
    public Filter regex(ByteString regex) {
      return new SimpleFilter(
          RowFilter.newBuilder()
              .setColumnQualifierRegexFilter(regex)
              .build());
    }

    /** Matches only cells from columns whose qualifiers equal the value. */
    public Filter exactMatch(ByteString value) {
      return new SimpleFilter(
          RowFilter.newBuilder()
              .setColumnQualifierRegexFilter(RegexUtil.literalRegex(value))
              .build());
    }

    /**
     * Construct a {@link QualifierRangeFilter} that can create a {@link ColumnRange} oriented
     * {@link Filter}.
     * @return a new {@link QualifierRangeFilter}
     */
    public QualifierRangeFilter range(String family) {
      return new QualifierRangeFilter(family);
    }
  }

  /**
   * Matches only cells with microsecond timestamps within the given range.  Start is inclusive
   * and end is exclusive.
   */

  public static class TimestampRangeFilter extends Filter {
    private final TimestampRange.Builder range = TimestampRange.newBuilder();

    public TimestampRangeFilter startClosed(long startMicros) {
      range.setStartTimestampMicros(startMicros);
      return this;
    }

    public TimestampRangeFilter endOpen(long endMicros) {
      range.setEndTimestampMicros(endMicros);
      return this;
    }

    @Override
    public RowFilter toProto() {
      return
        RowFilter.newBuilder().setTimestampRangeFilter(range.build()).build();
    }
  }

  public static class TimestampFilter {

    public TimestampRangeFilter range() {
      return new TimestampRangeFilter();
    }

    /**
     * Matches only cells with timestamps within the given range.
     *
     * @param startMicros Inclusive start of the range in microseconds.
     * @param endMicros Exclusive end of the range in microseconds.
     */
    public Filter range(long startMicros, long endMicros) {
      return range().startClosed(startMicros).endOpen(endMicros);
    }
  }

  /** Matches only cells with values that fall within the given value range. */
  public static class ValueRangeFilter extends Filter{
    private ValueRange.Builder range = ValueRange.newBuilder();

    public ValueRangeFilter startClosed(ByteString value) {
      range.setStartValueClosed(value);
      return this;
    }

    public ValueRangeFilter startOpen(ByteString value) {
      range.setStartValueOpen(value);
      return this;
    }

    public ValueRangeFilter endClosed(ByteString value) {
      range.setEndValueClosed(value);
      return this;
    }

    public ValueRangeFilter endOpen(ByteString value) {
      range.setEndValueOpen(value);
      return this;
    }

    @Override
    public RowFilter toProto() {
      return RowFilter.newBuilder().setValueRangeFilter(range.build()).build();
    }
  }

  public static class ValueFilter {
    /**
     * Matches only cells with values that satisfy the given <a
     * href="https://github.com/google/re2/wiki/Syntax>RE2 regex</a>. Note that, since cell values
     * can contain arbitrary bytes, the `\C` escape sequence must be used if a true wildcard is
     * desired. The `.` character will not match the new line character `\n`, which may be present
     * in a binary value.
     *
     * @param regex
     * @return
     */
    public Filter regex(String regex) {
      return regex(ByteString.copyFromUtf8(regex));
    }

    /**
     * Matches only cells with values that satisfy the given <a
     * href="https://github.com/google/re2/wiki/Syntax>RE2 regex</a>. Note that, since cell values
     * can contain arbitrary bytes, the `\C` escape sequence must be used if a true wildcard is
     * desired. The `.` character will not match the new line character `\n`, which may be present
     * in a binary value.
     *
     * @param regex
     * @return
     */
    public Filter regex(ByteString regex) {
      return new SimpleFilter(RowFilter.newBuilder().setValueRegexFilter(regex).build());
    }

    /** Matches only cells with values that match the given value. */
    public Filter exactMatch(ByteString value) {
      return new SimpleFilter(
          RowFilter.newBuilder().setValueRegexFilter(RegexUtil.literalRegex(value)).build());
    }

    /**
     * Construct a {@link ValueRangeBuilder} that can create a {@link ValueRange} oriented
     * {@link Filter}.
     * @return a new {@link ValueRangeBuilder}
     */
    public ValueRangeFilter range() {
      return new ValueRangeFilter();
    }

    /** Replaces each cell's value with the empty string. */
    public Filter strip() {
      return STRIP_VALUE;
    }
  }

  public static class OffsetFilter {

    /**
     * Skips the first N cells of each row, matching all subsequent cells. If duplicate cells are
     * present, as is possible when using an {@link InterleaveFilter}, each copy of the cell is
     * counted separately.
     */
    public Filter cellsPerRow(int count) {
      return new SimpleFilter(RowFilter.newBuilder().setCellsPerRowOffsetFilter(count).build());
    }
  }

  public static class LimitFilter {

    /**
     * Matches only the first N cells of each row. If duplicate cells are present, as is possible
     * when using an Interleave, each copy of the cell is counted separately.
     */
    public Filter cellsPerRow(int count) {
      return new SimpleFilter(RowFilter.newBuilder().setCellsPerRowLimitFilter(count).build());
    }

    /**
     * Matches only the most recent `count` cells within each column. For example, if count=2, this
     * filter would match column `foo:bar` at timestamps 10 and 9 skip all earlier cells in
     * `foo:bar`, and then begin matching again in column `foo:bar2`. If duplicate cells are
     * present, as is possible when using an {@link InterleaveFilter}, each copy of the cell is
     * counted separately.
     */
    public Filter cellsPerColumn(int count) {
      return new SimpleFilter(RowFilter.newBuilder().setCellsPerColumnLimitFilter(count).build());
    }
  }

  private static final class SimpleFilter extends Filter {

    private final RowFilter proto;

    private SimpleFilter(RowFilter proto) {
      this.proto = proto;
    }

    @InternalApi
    @Override
    public RowFilter toProto() {
      return proto;
    }
  }

  public abstract static class Filter {

    final RowFilter.Builder builder = RowFilter.newBuilder();

    Filter() {}

    @InternalApi
    public abstract RowFilter toProto();
  }
}
