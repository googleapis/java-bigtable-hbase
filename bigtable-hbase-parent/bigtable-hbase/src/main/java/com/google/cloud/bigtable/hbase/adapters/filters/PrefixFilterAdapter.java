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
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.PrefixFilter;

/**
 * Adapter for HBase {@link org.apache.hadoop.hbase.filter.PrefixFilter} instances.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class PrefixFilterAdapter extends TypedFilterAdapterBase<PrefixFilter> {
  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(FilterAdapterContext context, PrefixFilter filter)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(filter.getPrefix().length * 2);
    ReaderExpressionHelper.writeQuotedRegularExpression(baos, filter.getPrefix());
    // Unquoted all bytes:
    baos.write(ReaderExpressionHelper.ALL_QUALIFIERS_BYTES);
    ByteString quotedValue = ByteStringer.wrap(baos.toByteArray());
    return RowFilter.newBuilder()
        .setRowKeyRegexFilter(quotedValue)
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, PrefixFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }

  /**
   * <p>When scanning for a prefix the scan should stop immediately after the the last row that
   * has the specified prefix. This method calculates the closest next rowKey immediately following
   * the given rowKeyPrefix.</p>
   * <p><b>IMPORTANT: This converts a rowKey<u>Prefix</u> into a rowKey</b>.</p>
   * <p>If the prefix is an 'ASCII' string put into a byte[] then this is easy because you can
   * simply increment the last byte of the array.
   * But if your application uses real binary rowids you may run into the scenario that your
   * prefix is something like:</p>
   * &nbsp;&nbsp;&nbsp;<b>{ 0x12, 0x23, 0xFF, 0xFF }</b><br/>
   * Then this stopRow needs to be fed into the actual scan<br/>
   * &nbsp;&nbsp;&nbsp;<b>{ 0x12, 0x24 }</b> (Notice that it is shorter now)<br/>
   * This method calculates the correct stop row value for this usecase.
   *
   * @param rowKeyPrefix the rowKey<u>Prefix</u>.
   * @return the closest next rowKey immediately following the given rowKeyPrefix.
   */
  public static byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
    // Essentially we are treating it like an 'unsigned very very long' and doing +1 manually.
    // Search for the place where the trailing 0xFFs start
    int offset = rowKeyPrefix.length;
    while (offset > 0) {
      if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
        break;
      }
      offset--;
    }

    if (offset == 0) {
      // We got an 0xFFFF... (only FFs) stopRow value which is
      // the last possible prefix before the end of the table.
      // So set it to stop at the 'end of the table'
      return HConstants.EMPTY_END_ROW;
    }

    // Copy the right length of the original
    byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
    // And increment the last one
    newStopRow[newStopRow.length - 1]++;
    return newStopRow;
  }
}
