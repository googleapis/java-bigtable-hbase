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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Filters.ValueRangeFilter;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;

import java.io.IOException;

/**
 * Adapt a single HBase ValueFilter.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ValueFilterAdapter extends TypedFilterAdapterBase<ValueFilter> {

  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(FilterAdapterContext context, ValueFilter filter) throws IOException {
    return toFilter(context, filter).toProto();
  }

  public Filter toFilter(FilterAdapterContext context, ValueFilter filter) throws IOException {
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
  /** {@inheritDoc} */
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

  private Filter adaptBinaryComparator(
      CompareOp compareOp, BinaryComparator comparator) {
    ByteString value = ByteString.copyFrom(comparator.getValue());
    switch (compareOp) {
      case LESS:
        return range().endOpen(value);
      case LESS_OR_EQUAL:
        return range().endClosed(value);
      case EQUAL:
        if (comparator.getValue().length == 0) {
          // The empty case does needs to use valueRegexFilter, since "end value closed" of empty
          // is not allowed by the server.
          return FILTERS.value().regex(value);
        } else {
          return range().startClosed(value).endClosed(value);
        }
      case NOT_EQUAL:
        if(comparator.getValue().length == 0) {
          //Special case for NOT_EQUAL to EMPTY_STRING
          return FILTERS.value().regex(".+");
        } else {
          // This strictly less than + strictly greater than:
          return FILTERS.interleave().filter(range().endOpen(value)).filter(range().startOpen(value));
        }
      case GREATER_OR_EQUAL:
        return range().startClosed(value);
      case GREATER:
        return range().startOpen(value);
      case NO_OP:
        // No-op always passes. Instead of attempting to return null or default instance,
        // include an always-match filter.
        return FILTERS.pass();
      default:
        throw new IllegalStateException(
            String.format("Cannot handle unknown compare op %s", compareOp));
    }
  }

  private static ValueRangeFilter range() {
    return FILTERS.value().range();
  }

  private Filter adaptRegexStringComparator(
      CompareOp compareOp, RegexStringComparator comparator) {
    String pattern = FilterAdapterHelper.extractRegexPattern(comparator);
    switch (compareOp) {
      case EQUAL:
        return FILTERS.value().regex(pattern);
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
}
