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

import static com.google.cloud.bigtable.data.v2.wrappers.Filters.F;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.wrappers.Filters.InterleaveFilter;
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper;
import com.google.cloud.bigtable.util.ByteStringer;

import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * An adapter to transform an HBase MultipleColumnPrefixFilter into a
 * Bigtable RowFilter with each column prefix in an interleaved stream.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class MultipleColumnPrefixFilterAdapter
    extends TypedFilterAdapterBase<MultipleColumnPrefixFilter> {

  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(
      FilterAdapterContext context,
      MultipleColumnPrefixFilter filter) throws IOException {
    InterleaveFilter interleave = F.interleave();
    ByteArrayOutputStream outputStream = null;
    for (byte[] prefix : filter.getPrefix()) {
      if (outputStream == null) {
        outputStream = new ByteArrayOutputStream(prefix.length * 2);
      } else {
        outputStream.reset();
      }

      ReaderExpressionHelper.writeQuotedExpression(outputStream, prefix);
      outputStream.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);

      interleave.filter(F.qualifier().regex(ByteStringer.wrap(outputStream.toByteArray())));
    }
    return interleave.toProto();
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      MultipleColumnPrefixFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
