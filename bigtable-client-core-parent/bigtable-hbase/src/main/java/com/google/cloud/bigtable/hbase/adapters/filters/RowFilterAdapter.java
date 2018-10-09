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

import java.io.IOException;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper;
import com.google.protobuf.ByteString;

/**
 * An adapter for row key filters using comparators and operators.
 * <p>
 * Note that regular expression comparators are passed as is. This means some
 * users may need to modify their queries to adhere to the RE2 syntax.
 * <p>
 * Currently only the regular expression operator with the EQUAL operator is
 * supported.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class RowFilterAdapter
    extends TypedFilterAdapterBase<org.apache.hadoop.hbase.filter.RowFilter> {

  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(FilterAdapterContext context,
      org.apache.hadoop.hbase.filter.RowFilter filter) throws IOException {
    CompareOp compareOp = filter.getOperator();
    if (compareOp != CompareFilter.CompareOp.EQUAL) {
      throw new IllegalStateException(String.format("Cannot adapt operator %s",
        compareOp == null ? null : compareOp.getClass().getCanonicalName()));
    }
    ByteArrayComparable comparator = filter.getComparator();
    ByteString regexValue;
    if (comparator == null) {
      throw new IllegalStateException("Comparator cannot be null"); 
    } else if (comparator instanceof RegexStringComparator) {
      regexValue = ByteString.copyFrom(comparator.getValue());
    } else if (comparator instanceof BinaryComparator) {
      regexValue =
          ReaderExpressionHelper.quoteRegularExpression(comparator.getValue());
    } else {
      throw new IllegalStateException(String.format("Cannot adapt comparator %s", comparator
          .getClass().getCanonicalName()));
    }
    return FILTERS.key().regex(regexValue).toProto();
  }
  
  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, 
      org.apache.hadoop.hbase.filter.RowFilter filter) {
    ByteArrayComparable comparator = filter.getComparator();
    if (!(comparator instanceof RegexStringComparator) && !(comparator instanceof BinaryComparator)) {
      return FilterSupportStatus.newNotSupported(comparator.getClass().getName()
          + " comparator is not supported");
    }
    if (filter.getOperator() != CompareFilter.CompareOp.EQUAL) {
      return FilterSupportStatus.newNotSupported(filter.getOperator()
          + " operator is not supported");
    }
    return FilterSupportStatus.SUPPORTED;
  }
}
