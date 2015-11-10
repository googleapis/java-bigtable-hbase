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
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Adapter for HBase {@link PrefixFilter} instances.
 */
public class PrefixFilterAdapter implements TypedFilterAdapter<PrefixFilter> {
  @Override
  public RowFilter adapt(FilterAdapterContext context, PrefixFilter filter)
      throws IOException {
    ByteArrayOutputStream baos =
        new ByteArrayOutputStream(filter.getPrefix().length * 2);
    ReaderExpressionHelper.writeQuotedRegularExpression(baos, filter.getPrefix());
    // Unquoted all bytes:
    baos.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);
    ByteString quotedValue = ByteStringer.wrap(baos.toByteArray());
    return RowFilter.newBuilder()
        .setRowKeyRegexFilter(quotedValue)
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, PrefixFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
