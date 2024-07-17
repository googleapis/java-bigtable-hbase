/*
 * Copyright 2015 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.hbase.adapters.filters;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

/**
 * Adapter for qualifier filters.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class QualifierFilterAdapter extends TypedFilterAdapterBase<QualifierFilter> {

  private static final FilterSupportStatus SINGLE_FAMILY_REQUIRED =
      FilterSupportStatus.newNotSupported(
          "QualifierFilter must have a single column family "
              + "specified in the Scan when not using CompareOp.EQUAL.");

  private static final FilterSupportStatus UNSUPPORTED_COMPARABLE =
      FilterSupportStatus.newNotSupported(
          "QualifierFilter must have a BinaryComparator with any "
              + "CompareOp or a RegexStringComparator with a EQUAL COmpareOp.");

  /** {@inheritDoc} */
  @Override
  public Filter adapt(FilterAdapterContext context, QualifierFilter filter) throws IOException {
    if (filter.getComparator() instanceof RegexStringComparator) {
      return adaptRegexStringComparator(
          filter.getOperator(), (RegexStringComparator) filter.getComparator());
    } else if (filter.getComparator() instanceof BinaryComparator) {
      return adaptBinaryComparator(
          context, filter.getOperator(), (BinaryComparator) filter.getComparator());
    }
    throw new IllegalStateException(
        String.format(
            "Cannot adapt comparator %s", filter.getComparator().getClass().getCanonicalName()));
  }

  // Handles conversion of HBase's binary comparators to Bigtable's filters. Inequalities are
  // implemented as ColumnRange filters, while EQUALS is implemented as a literal
  // column_qualifier_regex_filter
  private Filter adaptBinaryComparator(
      FilterAdapterContext context, CompareOp compareOp, BinaryComparator comparator)
      throws IOException {
    ByteString value = ByteString.copyFrom(comparator.getValue());
    switch (compareOp) {
      case LESS:
        return FILTERS
            .qualifier()
            .rangeWithinFamily(FilterAdapterHelper.getSingleFamilyName(context))
            .endOpen(value);
      case LESS_OR_EQUAL:
        return FILTERS
            .qualifier()
            .rangeWithinFamily(FilterAdapterHelper.getSingleFamilyName(context))
            .endClosed(value);
      case EQUAL:
        return FILTERS
            .qualifier()
            .regex(ReaderExpressionHelper.quoteRegularExpression(comparator.getValue()));
      case NOT_EQUAL:
        // This strictly less than + strictly greater than:
        String familyName = FilterAdapterHelper.getSingleFamilyName(context);
        return FILTERS
            .interleave()
            .filter(FILTERS.qualifier().rangeWithinFamily(familyName).endOpen(value))
            .filter(FILTERS.qualifier().rangeWithinFamily(familyName).startOpen(value));
      case GREATER_OR_EQUAL:
        return FILTERS
            .qualifier()
            .rangeWithinFamily(FilterAdapterHelper.getSingleFamilyName(context))
            .startClosed(value);
      case GREATER:
        return FILTERS
            .qualifier()
            .rangeWithinFamily(FilterAdapterHelper.getSingleFamilyName(context))
            .startOpen(value);
      case NO_OP:
        // No-ops are always filtered out.
        // See:
        // https://github.com/apache/hbase/blob/master/hbase-client/src/main/java/org/apache/hadoop/hbase/filter/ColumnValueFilter.java#L127-L138
        return FILTERS.block();
      default:
        throw new IllegalStateException(
            String.format("Cannot handle unknown compare op %s", compareOp));
    }
  }

  private static Filter adaptRegexStringComparator(
      CompareOp compareOp, RegexStringComparator comparator) {
    String pattern = FilterAdapterHelper.extractRegexPattern(comparator);
    // HBase regex matching is unanchored, while Bigtable requires a full string match
    // To align the two, surround the user regex with wildcards
    pattern = "\\C*" + pattern + "\\C*";
    switch (compareOp) {
      case EQUAL:
        return FILTERS.qualifier().regex(pattern);
      case NO_OP:
        return FILTERS.pass();
      case LESS:
      case LESS_OR_EQUAL:
      case NOT_EQUAL:
      case GREATER_OR_EQUAL:
      case GREATER:
      default:
        throw new IllegalStateException(
            String.format("Cannot adapt regex filter with compare op %s", compareOp));
    }
  }

  private static boolean isRegexAndSupported(QualifierFilter filter) {
    return filter.getComparator() instanceof RegexStringComparator
        && CompareOp.EQUAL.equals(filter.getOperator());
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, QualifierFilter filter) {
    // For range filters, we only support a single family:
    if (!CompareOp.EQUAL.equals(filter.getOperator())
        && filter.getComparator() instanceof BinaryComparator
        && context.getScan().numFamilies() != 1) {
      return SINGLE_FAMILY_REQUIRED;
    }
    // Support binary comparators and regex comparators with equal compare op:
    if (!(filter.getComparator() instanceof BinaryComparator) && !isRegexAndSupported(filter)) {
      return UNSUPPORTED_COMPARABLE;
    }
    return FilterSupportStatus.SUPPORTED;
  }
}
