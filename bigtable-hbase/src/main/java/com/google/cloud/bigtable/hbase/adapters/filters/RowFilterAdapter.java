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

import java.io.IOException;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Builder;
import com.google.protobuf.ByteString;

/**
 * An adapter for row key filters using comparators and operators. 
 * <p>
 * Note that regular expression comparators are passed as is. This means some 
 * users may need to modify their queries to adhere to the RE2 syntax. 
 * <p>
 * Currently only the regular expression operator with the EQUAL operator is 
 * supported.
 */
public class RowFilterAdapter implements 
  TypedFilterAdapter<org.apache.hadoop.hbase.filter.RowFilter> {

  @Override
  public RowFilter adapt(FilterAdapterContext context, 
      org.apache.hadoop.hbase.filter.RowFilter filter)
      throws IOException {
    Builder builder = RowFilter.newBuilder();
    if (filter.getOperator() == CompareFilter.CompareOp.EQUAL && 
        filter.getComparator() != null && 
        filter.getComparator() instanceof RegexStringComparator) {
      ByteString rawValue = ByteString.copyFrom(
          filter.getComparator().getValue());
      builder.setRowKeyRegexFilter(rawValue);
    }
    return builder.build();
  }
  
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, 
      org.apache.hadoop.hbase.filter.RowFilter filter) {
    if (!(filter.getComparator() instanceof RegexStringComparator)) {
      return FilterSupportStatus.newNotSupported(
          filter.getComparator().getClass().getName() 
          + " comparators are not supported");
    }
    if (filter.getOperator() != CompareFilter.CompareOp.EQUAL) {
      return FilterSupportStatus.newNotSupported(
          filter.getOperator() + " operators are not supported");
    }
    return FilterSupportStatus.SUPPORTED;
  }
}
