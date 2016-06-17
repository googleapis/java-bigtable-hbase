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
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.bigtable.v2.ValueRange;
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;

import java.io.IOException;

/**
 * Adapt a single HBase ValueFilter.
 */
public class ValueFilterAdapter implements TypedFilterAdapter<ValueFilter> {

  @Override
  public RowFilter adapt(FilterAdapterContext context, ValueFilter filter) throws IOException {
    if (filter.getComparator() instanceof BinaryComparator) {
      return adaptBinaryComparator(
          filter.getOperator(), (BinaryComparator) filter.getComparator());
    } else if (filter.getComparator() instanceof RegexStringComparator) {
      return adaptRegexStringComparator(
          filter.getOperator(), (RegexStringComparator) filter.getComparator());
    }
    throw new IllegalStateException(
        String.format("Cannot adapt filter with comparator%s", filter.getComparator()));
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, ValueFilter filter) {
    if (filter.getComparator() instanceof BinaryComparator
        || (filter.getComparator() instanceof RegexStringComparator
        &&  filter.getOperator() == CompareOp.EQUAL)) {
      return FilterSupportStatus.SUPPORTED;
    }
    return FilterSupportStatus.newNotSupported(
        String.format(
            "ValueFilter must have either a BinaryComparator with any compareOp "
                + "or a RegexStringComparator with an EQUAL compareOp. Found (%s, %s)",
            filter.getComparator().getClass().getSimpleName(),
            filter.getOperator()));
  }

  private RowFilter adaptBinaryComparator(
      CompareOp compareOp, BinaryComparator comparator) throws IOException {
    ByteString value = ByteString.copyFrom(comparator.getValue());
    switch (compareOp) {
      case LESS:
        return createRowFilter(ValueRange.newBuilder().setEndValueOpen(value));
      case LESS_OR_EQUAL:
        return createRowFilter(ValueRange.newBuilder().setEndValueClosed(value));
      case EQUAL:
        byte[] quotedBytes = ReaderExpressionHelper.quoteRegularExpression(comparator.getValue());
        return RowFilter.newBuilder()
            .setValueRegexFilter(ByteStringer.wrap(quotedBytes))
            .build();
      case NOT_EQUAL:
        // This strictly less than + strictly greater than:
        return RowFilter.newBuilder()
            .setInterleave(
                Interleave.newBuilder()
                    .addFilters(
                        createRowFilter(ValueRange.newBuilder().setEndValueOpen(value)))
                    .addFilters(
                        createRowFilter(ValueRange.newBuilder().setStartValueOpen(value))))
                .build();
      case GREATER_OR_EQUAL:
        return createRowFilter(ValueRange.newBuilder().setStartValueClosed(value));
      case GREATER:
        return createRowFilter(ValueRange.newBuilder().setStartValueOpen(value));
      case NO_OP:
        // No-op always passes. Instead of attempting to return null or default instance,
        // include an always-match filter.
        return FilterAdapterHelper.ACCEPT_ALL_FILTER;
      default:
        throw new IllegalStateException(
            String.format("Cannot handle unknown compare op %s", compareOp));
    }
  }

  private static RowFilter createRowFilter(ValueRange.Builder valueRange) {
    return RowFilter.newBuilder().setValueRangeFilter(valueRange).build();
  }

  private RowFilter adaptRegexStringComparator(
      CompareOp compareOp, RegexStringComparator comparator) {
    String pattern = FilterAdapterHelper.extractRegexPattern(comparator);
    switch (compareOp) {
      case EQUAL:
        return RowFilter.newBuilder()
            .setValueRegexFilter(ByteString.copyFromUtf8(pattern))
            .build();
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
}
