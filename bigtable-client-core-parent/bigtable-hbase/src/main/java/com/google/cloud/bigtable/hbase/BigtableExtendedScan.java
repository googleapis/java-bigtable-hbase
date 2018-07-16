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

import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.util.RowKeyUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

/**
 * This is a Cloud Bigtable specific extension of {@link Scan}. The Cloud Bigtable ReadRows API
 * allows for an arbitrary set of ranges and row keys as part of a scan. Instance of
 * BigtableExtendedScan can be used in {@link Table#getScanner(Scan)}.
 */
@SuppressWarnings("deprecation")
public class BigtableExtendedScan extends Scan {
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
    addRange(prefix, RowKeyUtil.calculateTheClosestNextRowKeyForPrefix(prefix));
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
