/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This is a stub of the MultiRowRangeFilter introduced in HBase 1.1. It's meant to be a shim
 * for Bigtable users that are using bigtable-hbase-1.0.
 *
 * Filter to support scan multiple row key ranges. It can construct the row key ranges from the
 * passed list which can be accessed by each region server.
 */
public class MultiRowRangeFilter extends FilterBase {

  private List<RowRange> rangeList;

 /**
   * @param list A list of <code>RowRange</code>
   * @throws IOException
   *           throw an exception if the range list is not in an natural order or any
   *           <code>RowRange</code> is invalid
   */
  public MultiRowRangeFilter(List<RowRange> list) throws IOException {
    this.rangeList = list;
  }

  public List<RowRange> getRowRanges() {
    return this.rangeList;
  }

  @Override
  public boolean filterAllRemaining() {
    throw new UnsupportedOperationException("filterAllRemaining");
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    throw new UnsupportedOperationException("filterRowKey");
  }

  @Override
  public ReturnCode filterKeyValue(Cell ignored) {
    throw new UnsupportedOperationException("filterKeyValue");
  }

  @Override
  public Cell getNextCellHint(Cell currentKV) {
    throw new UnsupportedOperationException("getNextCellHint");
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte[] toByteArray() {
    throw new UnsupportedOperationException("toByteArray");
  }

  /**
   * @param pbBytes A pb serialized instance
   * @return An instance of MultiRowRangeFilter
   * @throws DeserializationException
   */
  public static MultiRowRangeFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {

    throw new UnsupportedOperationException("parseFrom");
  }

  /**
   * sort the ranges and if the ranges with overlap, then merge them.
   *
   * @param ranges the list of ranges to sort and merge.
   * @return the ranges after sort and merge.
   */
  public static List<RowRange> sortAndMerge(List<RowRange> ranges) {
    throw new UnsupportedOperationException("sortAndMerge");
  }


  public static class RowRange implements Comparable<RowRange> {
    private byte[] startRow;
    private boolean startRowInclusive = true;
    private byte[] stopRow;
    private boolean stopRowInclusive = false;
    private int isScan = 0;

    public RowRange() {
    }
    /**
     * If the startRow is empty or null, set it to HConstants.EMPTY_BYTE_ARRAY, means begin at the
     * start row of the table. If the stopRow is empty or null, set it to
     * HConstants.EMPTY_BYTE_ARRAY, means end of the last row of table.
     */
    public RowRange(String startRow, boolean startRowInclusive, String stopRow,
        boolean stopRowInclusive) {
      this((startRow == null || startRow.isEmpty()) ? HConstants.EMPTY_BYTE_ARRAY :
        Bytes.toBytes(startRow), startRowInclusive,
        (stopRow == null || stopRow.isEmpty()) ? HConstants.EMPTY_BYTE_ARRAY :
        Bytes.toBytes(stopRow), stopRowInclusive);
    }

    public RowRange(byte[] startRow,  boolean startRowInclusive, byte[] stopRow,
        boolean stopRowInclusive) {
      this.startRow = (startRow == null) ? HConstants.EMPTY_BYTE_ARRAY : startRow;
      this.startRowInclusive = startRowInclusive;
      this.stopRow = (stopRow == null) ? HConstants.EMPTY_BYTE_ARRAY :stopRow;
      this.stopRowInclusive = stopRowInclusive;
      isScan = Bytes.equals(startRow, stopRow) ? 1 : 0;
    }

    public byte[] getStartRow() {
      return startRow;
    }

    public byte[] getStopRow() {
      return stopRow;
    }

    /**
     * @return if start row is inclusive.
     */
    public boolean isStartRowInclusive() {
      return startRowInclusive;
    }

    /**
     * @return if stop row is inclusive.
     */
    public boolean isStopRowInclusive() {
      return stopRowInclusive;
    }

    public boolean contains(byte[] row) {
      return contains(row, 0, row.length);
    }

    public boolean contains(byte[] buffer, int offset, int length) {
      if(startRowInclusive) {
        if(stopRowInclusive) {
          return Bytes.compareTo(buffer, offset, length, startRow, 0, startRow.length) >= 0
              && (Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY) ||
                  Bytes.compareTo(buffer, offset, length, stopRow, 0, stopRow.length) <= isScan);
        } else {
          return Bytes.compareTo(buffer, offset, length, startRow, 0, startRow.length) >= 0
              && (Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY) ||
                  Bytes.compareTo(buffer, offset, length, stopRow, 0, stopRow.length) < isScan);
        }
      } else {
        if(stopRowInclusive) {
          return Bytes.compareTo(buffer, offset, length, startRow, 0, startRow.length) > 0
              && (Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY) ||
                  Bytes.compareTo(buffer, offset, length, stopRow, 0, stopRow.length) <= isScan);
        } else {
          return Bytes.compareTo(buffer, offset, length, startRow, 0, startRow.length) > 0
              && (Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY) ||
                  Bytes.compareTo(buffer, offset, length, stopRow, 0, stopRow.length) < isScan);
        }
      }
    }

    @Override
    public int compareTo(RowRange other) {
      return Bytes.compareTo(this.startRow, other.startRow);
    }

    public boolean isValid() {
      return Bytes.equals(startRow, HConstants.EMPTY_BYTE_ARRAY)
          || Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY)
          || Bytes.compareTo(startRow, stopRow) < 0
          || (Bytes.compareTo(startRow, stopRow) == 0 && stopRowInclusive == true);
    }
  }
}
