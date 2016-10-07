/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflowimport.testing;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;

/**
 * Helpers for creating HBase {@link Cell}s in tests.
 */
public class HBaseCellUtils {
  // Dummy value for all data cells.
  private static final byte[] VALUE = new byte[] { '1' };

  // Delete Marker added as result of Delete(row).addColumn(family, qualifier) or
  // Delete(row).addColumn(family, qualifier, timestamp).
  public static Cell deleteMarkerForOneCellWithExactTimestamp(
      byte[] row, byte[] columnFamily, byte[] qualifier, long timestamp) {
    return new KeyValue(row, columnFamily, qualifier, timestamp, KeyValue.Type.Delete);
  }
  // Delete Marker added as result of Delete(row).addColumns(family, qualifier) or
  // Delete(row).addColumns(family, qualifier, timestamp).
  public static Cell deleteMarkerForCellsWithLowerOrEqualTimestamp(
      byte[] row, byte[] columnFamily, byte[] qualifier, long timestamp) {
    return new KeyValue(row, columnFamily, qualifier, timestamp, KeyValue.Type.DeleteColumn);
  }

  // Delete Marker added as result of Delete(row).addFamilyVersion(family, timestamp).
  public static Cell deleteMarkerForAllCellsInFamilyWithExactTimestamp(
      byte[] row, byte[] columnFamily, long timestamp) {
    return new KeyValue(row, columnFamily, null, timestamp, KeyValue.Type.DeleteFamilyVersion);
  }

  // Delete Marker added as result of Delete(row).addFamily(family) or
  // Delete(row).addFamily(family, timestamp).
  public static Cell deleteMarkerForAllCellsInFamilyWithLowerOrEqualTimestamp(
      byte[] row, byte[] columnFamily, long timestamp) {
    return new KeyValue(row, columnFamily, null, timestamp, Type.DeleteFamily);
  }

  public static Cell createDataCell(
      byte[] row, byte[] columnFamily, byte[] qualifier, long timestamp) {
    return new KeyValue(row, columnFamily, qualifier, timestamp, VALUE);
  }
}
