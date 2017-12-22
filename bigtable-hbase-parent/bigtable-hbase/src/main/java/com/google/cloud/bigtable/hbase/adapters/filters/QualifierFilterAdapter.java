/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.filter.RowFilters.R;
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

import java.io.IOException;

/**
 * Adapter for qualifier filters.
 *
 * @author sduskis
 * @version $Id: $Id
 */
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
  public RowFilter adapt(FilterAdapterContext context, QualifierFilter filter)
      throws IOException {
    if (filter.getComparator() instanceof RegexStringComparator) {
      return adaptRegexStringComparator(
          filter.getOperator(), (RegexStringComparator) filter.getComparator());
    } else if (filter.getComparator() instanceof BinaryComparator) {
      return adaptBinaryComparator(
          context, filter.getOperator(), (BinaryComparator) filter.getComparator());
    }
    throw new IllegalStateException(
        String.format(
            "Cannot adapt comparator %s",
            filter.getComparator().getClass().getCanonicalName()));
  }

  private RowFilter adaptBinaryComparator(
      FilterAdapterContext context, CompareOp compareOp, BinaryComparator comparator)
      throws IOException {
    byte[] quoted = ReaderExpressionHelper.quoteRegularExpression(comparator.getValue());
    ByteString quotedValue = ByteStringer.wrap(quoted);
    switch (compareOp) {
      case LESS:
        return R.columnRangeBuilder().family(fmaily(context)).endOpened(quotedValue).build();
      case LESS_OR_EQUAL:
        return R.columnRangeBuilder().family(fmaily(context)).endClosed(quotedValue).build();
      case EQUAL:
        return R.chain(
            R.familyNameRegex(fmaily(context)),
            R.columnQualifierRegex(quotedValue));
      case NOT_EQUAL:
        // This strictly less than + strictly greater than:
        String familyName = fmaily(context);
        return R.interleave(
            R.columnRangeBuilder().family(familyName).endOpened(quotedValue).build(),
            R.columnRangeBuilder().family(familyName).startOpened(quotedValue).build());
      case GREATER_OR_EQUAL:
        return R.columnRangeBuilder().family(fmaily(context)).startClosed(quotedValue).build();
      case GREATER:
        return R.columnRangeBuilder().family(fmaily(context)).startOpened(quotedValue).build();
      case NO_OP:
        // No-op always passes. Instead of attempting to return null or default instance,
        // include an always-match filter.
        return FilterAdapterHelper.ACCEPT_ALL_FILTER;
      default:
        throw new IllegalStateException(
            String.format("Cannot handle unknown compare op %s", compareOp));
    }
  }

  public String fmaily(FilterAdapterContext context) {
    return FilterAdapterHelper.getSingleFamilyName(context);
  }

  private RowFilter adaptRegexStringComparator(
      CompareOp compareOp, RegexStringComparator comparator) {
    String pattern = FilterAdapterHelper.extractRegexPattern(comparator);
    switch (compareOp) {
      case EQUAL:
        return R.columnQualifierRegex(ByteString.copyFromUtf8(pattern));
      case NO_OP:
        return FilterAdapterHelper.ACCEPT_ALL_FILTER;
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
    if ( !(filter.getComparator() instanceof BinaryComparator)
        && !isRegexAndSupported(filter)) {
      return UNSUPPORTED_COMPARABLE;
    }
    return FilterSupportStatus.SUPPORTED;
  }
}
