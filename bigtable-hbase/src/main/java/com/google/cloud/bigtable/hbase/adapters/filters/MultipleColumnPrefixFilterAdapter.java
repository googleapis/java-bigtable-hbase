/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * An adapter to transform an HBase MultipleColumnPrefixFilter into a
 * Bigtable RowFilter with each column prefix in an interleaved stream.
 */
public class MultipleColumnPrefixFilterAdapter
    implements TypedFilterAdapter<MultipleColumnPrefixFilter> {

  ReaderExpressionHelper readerExpressionHelper = new ReaderExpressionHelper();

  @Override
  public RowFilter adapt(
      FilterAdapterContext context,
      MultipleColumnPrefixFilter filter) throws IOException {
    Interleave.Builder interleaveBuilder = Interleave.newBuilder();
    ByteArrayOutputStream outputStream = null;
    for (byte[] prefix : filter.getPrefix()) {
      if (outputStream == null) {
        outputStream = new ByteArrayOutputStream(prefix.length * 2);
      }
      outputStream.reset();

      readerExpressionHelper.writeQuotedExpression(prefix, outputStream);
      outputStream.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);

      RowFilter.Builder singlePrefixBuilder = RowFilter.newBuilder();
      singlePrefixBuilder.setColumnQualifierRegexFilter(
          ByteString.copyFrom(
              outputStream.toByteArray()));

      interleaveBuilder.addFilters(singlePrefixBuilder);
    }
    return RowFilter.newBuilder()
        .setInterleave(interleaveBuilder)
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      MultipleColumnPrefixFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
