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
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Adapter for a single {@link FamilyFilter} to a Cloud Bigtable RowFilter.
 */
public class FamilyFilterAdapter extends TypedFilterAdapterBase<FamilyFilter> {
  ReaderExpressionHelper readerExpressionHelper = new ReaderExpressionHelper();

  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(FilterAdapterContext context, FamilyFilter filter)
      throws IOException {
    CompareOp compareOp = filter.getOperator();
    if (compareOp != CompareFilter.CompareOp.EQUAL) {
      throw new IllegalStateException(String.format("Cannot adapt operator %s",
        compareOp == null ? null : compareOp.getClass().getCanonicalName()));
    }

    ByteArrayComparable comparator = filter.getComparator();
    String family = null;
    if (comparator == null) {
      throw new IllegalStateException("Comparator cannot be null"); 
    } else if (comparator instanceof RegexStringComparator) {
      family = Bytes.toString(comparator.getValue());
    } else if (comparator instanceof BinaryComparator) {
      ByteString quotedRegularExpression =
          ReaderExpressionHelper.quoteRegularExpression(comparator.getValue());
      family = quotedRegularExpression.toStringUtf8();
    } else {
      throw new IllegalStateException(
          "Cannot adapt comparator " + comparator.getClass().getCanonicalName());
    }
    return FILTERS.family().regex(family).toProto();
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      FamilyFilter filter) {
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
