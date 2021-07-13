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
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;

/**
 * Adapter for a single ColumnPrefixFilter to a Cloud Bigtable RowFilter.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ColumnPrefixFilterAdapter extends TypedFilterAdapterBase<ColumnPrefixFilter> {

  /** {@inheritDoc} */
  @Override
  public Filter adapt(FilterAdapterContext context, ColumnPrefixFilter filter) throws IOException {
    byte[] prefix = filter.getPrefix();

    // Quoting for RE2 can result in at most length * 2 characters written. Pre-allocate
    // that much space in the ByteString.Output to prevent reallocation later.
    ByteString.Output outputStream = ByteString.newOutput(prefix.length * 2);
    ReaderExpressionHelper.writeQuotedExpression(outputStream, prefix);
    outputStream.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);

    return FILTERS.qualifier().regex(outputStream.toByteString());
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, ColumnPrefixFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
