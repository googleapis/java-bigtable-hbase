package com.google.cloud.bigtable.filter;

import java.util.Arrays;
import java.util.List;

import com.google.api.client.util.Preconditions;
import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowFilter.Condition;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.bigtable.v2.TimestampRange;
import com.google.protobuf.ByteString;

public final class RowFilters {

  public enum EdgeType {
    None, Open, Closed;
  }

  public final static class ColumnRangeFilterBuilder {
    private String family = null;
    private ByteString start = null;
    private ByteString end = null;
    private EdgeType startEdgeType = EdgeType.None;
    private EdgeType endEdgeType = EdgeType.None;
    
    public ColumnRangeFilterBuilder family(String family) {
      this.family = family;
      return this;
    }

    public ColumnRangeFilterBuilder startOpened(ByteString start) {
      this.start = start;
      this.startEdgeType = EdgeType.Open;
      return this;
    }

    public ColumnRangeFilterBuilder startClosed(ByteString start) {
      this.start = start;
      this.startEdgeType = EdgeType.Closed;
      return this;
    }

    public ColumnRangeFilterBuilder endOpened(ByteString end) {
      this.end = end;
      this.endEdgeType = EdgeType.Open;
      return this;
    }

    public ColumnRangeFilterBuilder endClosed(ByteString end) {
      this.end = end;
      this.endEdgeType = EdgeType.Closed;
      return this;
    }

    public RowFilter build() {
      return RF.columnRange(family, start, startEdgeType, end, endEdgeType);
    }
  }

  public final static Filters RF = new Filters();

  public final static class Filters {
    // Applies several RowFilters to the data in sequence, progressively
    // narrowing the results.
    public RowFilter chain(RowFilter... filters) {
      return chain(Arrays.asList(filters));
    }

    public RowFilter chain(List<RowFilter> list) {
      if (list.size() == 1) {
        return list.get(0);
      }
      Chain chain = Chain.newBuilder().addAllFilters(list).build();
      return RowFilter.newBuilder().setChain(chain).build();
    }

    // Applies several RowFilters to the data in parallel and combines the
    // results.
    public RowFilter interleave(RowFilter... filters) {
      return interleave(Arrays.asList(filters));
    }

    public RowFilter interleave(List<RowFilter> list) {
      if (list.size() == 1) {
        return list.get(0);
      }
      Interleave interleave = Interleave.newBuilder().addAllFilters(list).build();
      return RowFilter.newBuilder().setInterleave(interleave).build();
    }

    /*
     * A RowFilter which evaluates one of two possible RowFilters, depending on whether or not a
     * predicate RowFilter outputs any cells from the input row.<P> IMPORTANT NOTE: The predicate
     * filter does not execute atomically with the true and false filters, which may lead to
     * inconsistent or unexpected results. Additionally, Condition filters have poor performance,
     * especially when filters are set for the false condition. message Condition { If
     * `predicate_filter` outputs any cells, then `true_filter` will be evaluated on the input row.
     * Otherwise, `false_filter` will be evaluated. RowFilter predicate_filter = 1; The filter to
     * apply to the input row if `predicate_filter` returns any results. If not provided, no results
     * will be returned in the true case. RowFilter true_filter = 2; The filter to apply to the
     * input row if `predicate_filter` does not return any results. If not provided, no results will
     * be returned in the false case. RowFilter false_filter = 3; }
     */
    public RowFilter condition(RowFilter predicate, RowFilter trueFilter,
        RowFilter falseFilter) {
      Condition condition = Condition.newBuilder().setPredicateFilter(predicate)
          .setTrueFilter(trueFilter).setFalseFilter(falseFilter).build();
      return RowFilter.newBuilder().setCondition(condition).build();
    }

    // Cannot be used within the `predicate_filter`, `true_filter`, or
    // `false_filter` of a [Condition][google.bigtable.v2.RowFilter.Condition].
    public RowFilter sink() {
      return RowFilter.newBuilder().setSink(true).build();
    }

    // Matches all cells, regardless of input. Functionally equivalent to
    // leaving `filter` unset, but included for completeness.
    public RowFilter passAll() {
      return RowFilter.newBuilder().setPassAllFilter(true).build();
    }

    // Does not match any cells, regardless of input. Useful for temporarily
    // disabling just part of a filter.
    public RowFilter blockallAll() {
      return RowFilter.newBuilder().setBlockAllFilter(true).build();
    }

    // Matches only cells from rows whose keys satisfy the given RE2 regex. In
    // other words, passes through the entire row when the key matches, and
    // otherwise produces an empty row.
    // Note that, since row keys can contain arbitrary bytes, the `\C` escape
    // sequence must be used if a true wildcard is desired. The `.` character
    // will not match the new line character `\n`, which may be present in a
    // binary key.
    public RowFilter rowKeyRegex(ByteString regex) {
      return RowFilter.newBuilder().setRowKeyRegexFilter(regex).build();
    }

    // Matches all cells from a row with probability p, and matches no cells
    // from the row with probability 1-p.
    public RowFilter rowSample(double probability) {
      return RowFilter.newBuilder().setRowSampleFilter(probability).build();
    }

    // Matches only cells from columns whose families satisfy the given RE2
    // regex. For technical reasons, the regex must not contain the `:`
    // character, even if it is not being used as a literal.
    // Note that, since column families cannot contain the new line character
    // `\n`, it is sufficient to use `.` as a full wildcard when matching
    // column family names.
    public RowFilter familyNameRegex(String regex) {
      return RowFilter.newBuilder().setFamilyNameRegexFilter(regex).build();
    }

    // Matches only cells from columns whose qualifiers satisfy the given RE2
    // regex.
    // Note that, since column qualifiers can contain arbitrary bytes, the `\C`
    // escape sequence must be used if a true wildcard is desired. The `.`
    // character will not match the new line character `\n`, which may be
    // present in a binary qualifier.
    public RowFilter columnQualifierRegex(ByteString regex) {
      return RowFilter.newBuilder().setColumnQualifierRegexFilter(regex).build();
    }

    public static ColumnRangeFilterBuilder columnRangeBuilder() {
      return new ColumnRangeFilterBuilder();
    }

    // Matches only cells from columns within the given range.
    public RowFilter columnRange(String familyRegex, ByteString qualifierStart,
        EdgeType startEdgeType, ByteString qualifierEnd, EdgeType endEdgeType) {

      Preconditions.checkArgument(qualifierStart != null || startEdgeType == EdgeType.None);
      Preconditions.checkArgument(qualifierEnd != null || endEdgeType == EdgeType.None);

      ColumnRange.Builder range = ColumnRange.newBuilder();
      range.setFamilyName(familyRegex);
      if (qualifierStart != null) {
        if (startEdgeType == EdgeType.Open) {
          range.setStartQualifierOpen(qualifierStart);
        } else {
          range.setStartQualifierClosed(qualifierStart);
        }
      }
      if (qualifierEnd != null) {
        if (endEdgeType == EdgeType.Open) {
          range.setEndQualifierOpen(qualifierEnd);
        } else {
          range.setEndQualifierClosed(qualifierEnd);
        }
      }
      return RowFilter.newBuilder()
          .setFamilyNameRegexFilter(familyRegex)
          .setColumnRangeFilter(range.build())
          .build();
    }

    // Matches only cells with timestamps within the given range.
    public RowFilter timestampRange(Long timestampStart, Long timestampEnd) {

      TimestampRange.Builder range = TimestampRange.newBuilder();
      if (timestampStart != null) {
        range.setStartTimestampMicros(timestampStart);
      }
      if (timestampEnd != null) {
        range.setEndTimestampMicros(timestampEnd);
      }
      return RowFilter.newBuilder().setTimestampRangeFilter(range.build()).build();
    }

    // Matches only cells with values that satisfy the given regular expression.
    // Note that, since cell values can contain arbitrary bytes, the `\C` escape
    // sequence must be used if a true wildcard is desired. The `.` character
    // will not match the new line character `\n`, which may be present in a
    // binary value.
    public RowFilter valueRegex(ByteString regex) {
      return RowFilter.newBuilder().setValueRegexFilter(regex).build();
    }

    // Skips the first N cells of each row, matching all subsequent cells.
    // If duplicate cells are present, as is possible when using an Interleave,
    // each copy of the cell is counted separately.
    public RowFilter cellsPerRowOffset(int count) {
      return RowFilter.newBuilder().setCellsPerRowOffsetFilter(count).build();
    }

    // Matches only the first N cells of each row.
    // If duplicate cells are present, as is possible when using an Interleave,
    // each copy of the cell is counted separately.
    public RowFilter cellsPerRowLimit(int count) {
      return RowFilter.newBuilder().setCellsPerRowLimitFilter(count).build();
    }

    // Matches only the most recent N cells within each column. For example,
    // if N=2, this filter would match column `foo:bar` at timestamps 10 and 9,
    // skip all earlier cells in `foo:bar`, and then begin matching again in
    // column `foo:bar2`.
    // If duplicate cells are present, as is possible when using an Interleave,
    // each copy of the cell is counted separately.
    public RowFilter cellsPerColumnLimit(int count) {
      return RowFilter.newBuilder().setCellsPerColumnLimitFilter(count).build();
    }

    // Replaces each cell's value with the empty string.
    public RowFilter stripValue() {
      return RowFilter.newBuilder().setStripValueTransformer(true).build();
    }

    // Applies the given label to all cells in the output row. This allows
    // the client to determine which results were produced from which part of
    // the filter.
    //
    // Values must be at most 15 characters in length, and match the RE2
    // pattern `[a-z0-9\\-]+`
    //
    // Due to a technical limitation, it is not currently possible to apply
    // multiple labels to a cell. As a result, a Chain may have no more than
    // one sub-filter which contains a `apply_label_transformer`. It is okay for
    // an Interleave to contain multiple `apply_label_transformers`, as they
    // will be applied to separate copies of the input. This may be relaxed in
    // the future.
    public RowFilter applyLabel(String s) {
      return RowFilter.newBuilder().setApplyLabelTransformer(s).build();
    }
  }
}