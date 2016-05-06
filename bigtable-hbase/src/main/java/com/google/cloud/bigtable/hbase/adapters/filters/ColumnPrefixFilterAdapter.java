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

import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Adapter for a single ColumnPrefixFilter to a Cloud Bigtable RowFilter.
 */
public class ColumnPrefixFilterAdapter implements TypedFilterAdapter<ColumnPrefixFilter> {
  ReaderExpressionHelper readerExpressionHelper = new ReaderExpressionHelper();

  @Override
  public RowFilter adapt(FilterAdapterContext context, ColumnPrefixFilter filter)
      throws IOException {
    byte[] prefix = filter.getPrefix();

    // Quoting for RE2 can result in at most length * 2 characters written. Pre-allocate
    // that much space in the ByteArrayOutputStream to prevent reallocation later.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(prefix.length * 2);
    ReaderExpressionHelper.writeQuotedRegularExpression(outputStream, prefix);
    outputStream.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);

    return RowFilter.newBuilder()
        .setColumnQualifierRegexFilter(
            ByteString.copyFrom(
                outputStream.toByteArray()))
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      ColumnPrefixFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
