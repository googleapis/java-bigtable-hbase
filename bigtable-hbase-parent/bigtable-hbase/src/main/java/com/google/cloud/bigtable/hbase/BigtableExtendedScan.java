/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import java.util.Arrays;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.annotations.VisibleForTesting;

/**
 * This is a Cloud Bigtable specific extension of {@link Scan}. The Cloud Bigtable ReadRows API
 * allows for an arbitrary set of ranges and row keys as part of a scan. Instance of
 * BigtableExtendedScan can be used in {@link Table#getScanner(Scan)}.
 */
public class BigtableExtendedScan extends Scan {


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
  @VisibleForTesting
  static byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
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

  private RowSet.Builder rowSet = RowSet.newBuilder();

  /**
   * @throws UnsupportedOperationException to avoid confusion.  Use {@link #addRange(byte[], byte[])} instead.
   */
  @Override
  public Scan setStartRow(byte[] startRow) {
    throw new UnsupportedOperationException("Pleaes use addRange(byte[], byte[]) instead.");
  }

  /**
   * @throws UnsupportedOperationException to avoid confusion.  Use {@link #addRange(byte[], byte[])} instead.
   */
  @Override
  public Scan setStopRow(byte[] stopRow) {
    throw new UnsupportedOperationException("Pleaes use addRange(byte[], byte[]) instead.");
  }

  /**
   * @throws UnsupportedOperationException to avoid confusion.  Use {@link #addRangeWithPrefix(byte[])} instead.
   */
  @Override
  public Scan setRowPrefixFilter(byte[] rowPrefix) {
    throw new UnsupportedOperationException("Pleaes use addRangeWithPrefix(byte[]) instead.");
  }

  /**
   * Creates a {@link RowRange} based on a prefix.  This is similar to {@link Scan#setRowPrefixFilter(byte[])}.
   * @param prefix
   */
  public void addRangeWithPrefix(byte[] prefix) {
    addRange(prefix, calculateTheClosestNextRowKeyForPrefix(prefix));
  }

  /**
   * Adds a range to scan. This is similar to calling a combination of
   * {@link Scan#setStartRow(byte[])} and {@link Scan#setStopRow(byte[])}. Other ranges can be
   * constructed by creating a {@link RowRange} and calling {@link #addRange(RowRange)}
   * @param startRow
   * @param stopRow
   */
  public void addRange(byte[] startRow, byte[] stopRow) {
    addRange(RowRange.newBuilder()
        .setStartKeyClosed(ByteStringer.wrap(startRow))
        .setEndKeyOpen(ByteStringer.wrap(stopRow))
        .build());
  }

  /**
   * Adds an arbitrary {@link RowRange} to the request. Ranges can have empty start keys or end
   * keys. Ranges can also be inclusive/closed or exclusive/open. The default range is inclusive
   * start and exclusive end.
   * @param range
   */
  public void addRange(RowRange range) {
    rowSet.addRowRanges(range);
  }

  /**
   * Add a single row key to the output. This can be called multiple times with random rowKeys.
   * Duplicate rowKeys will result in a single response in the scan results. Results of scans also
   * return rows in lexicographically sorted order, and not based on the order in which row keys
   * were added.
   * @param rowKey
   */
  public void addRowKey(byte[] rowKey) {
    rowSet.addRowKeys(ByteStringer.wrap(rowKey));
  }

  /**
   * @return the {@link RowSet} built until now, which includes lists of individual keys and row ranges.
   */
  public RowSet getRowSet() {
    return rowSet.build();
  }
}
