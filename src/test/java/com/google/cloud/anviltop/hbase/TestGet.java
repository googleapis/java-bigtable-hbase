/*
 * Copyright (c) 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.junit.Assert;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestGet extends AbstractTest {
  /**
   * Requirement 3.2 - If a column family is requested, but no qualifier, all columns in that family
   * are returned
   */
  @Test
  public void testNoQualifier() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = randomData("testrow-");
    int numValues = 3;
    byte[][] quals = randomData("qual-", numValues);
    byte[][] values = randomData("value-", numValues);

    // Insert some columns
    Put put = new Put(rowKey);
    for (int i = 0; i < numValues; ++i) {
      put.add(COLUMN_FAMILY, quals[i], values[i]);
    }
    table.put(put);

    // Get without a qualifer, and confirm all results are returned.
    Get get = new Get(rowKey);
    get.addFamily(COLUMN_FAMILY);
    Result result = table.get(get);
    Assert.assertEquals(numValues, result.size());
    for (int i = 0; i < numValues; ++i) {
      Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, quals[i]));
      Assert.assertArrayEquals(values[i],
          CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[i])));
    }

    // Cleanup
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    table.close();
  }

  /**
   * Requirement 3.3 - Multiple family:qualifiers can be requested for a single row.
   */
  @Test
  public void testMultipleQualifiers() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = randomData("testrow-");
    int numValues = 3;
    byte[][] quals = randomData("qual-", numValues);
    byte[][] values = randomData("value-", numValues);

    // Insert a few columns
    Put put = new Put(rowKey);
    for (int i = 0; i < numValues; ++i) {
      put.add(COLUMN_FAMILY, quals[i], values[i]);
    }
    table.put(put);

    // Select some, but not all columns, and confirm that's what's returned.
    Get get = new Get(rowKey);
    int[] colsToSelect = { 0, 2 };
    for (int i : colsToSelect) {
      get.addColumn(COLUMN_FAMILY, quals[i]);
    }
    Result result = table.get(get);
    Assert.assertEquals(colsToSelect.length, result.size());
    for (int i : colsToSelect) {
      Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, quals[i]));
      Assert.assertArrayEquals(values[i],
          CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[i])));
    }

    // Cleanup
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    table.close();
  }

  /**
   * Requirement 3.4 - A time range of minTimestamp (inclusive) - maxTimestamp (exclusive) can be
   * specified.
   */
  @Test
  public void testTimeRange() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = randomData("testrow-");
    byte[] qual = randomData("qual-");
    int numVersions = 5;
    int minVersion = 1;
    int maxVersion = 4;
    long timestamps[] = sequentialTimestamps(numVersions);
    byte[][] values = randomData("value-", numVersions);

    // Insert values with different timestamps at the same column.
    Put put = new Put(rowKey);
    for (int i = 0; i < numVersions; ++i) {
      put.add(COLUMN_FAMILY, qual, timestamps[i], values[i]);
    }
    table.put(put);

    // Get with a time range, and return the correct cells are returned.
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qual);
    get.setTimeRange(timestamps[minVersion], timestamps[maxVersion]);
    get.setMaxVersions(numVersions);
    Result result = table.get(get);
    Assert.assertEquals(maxVersion - minVersion, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertEquals(maxVersion - minVersion, cells.size());

    // Cells return in descending order.  Max is exclusive, min is inclusive.
    for (int i = maxVersion - 1, j = 0; i >= minVersion; --i, ++j) {
      Assert.assertEquals(timestamps[i], cells.get(j).getTimestamp());
      Assert.assertArrayEquals(values[i], CellUtil.cloneValue(cells.get(j)));
    }

    // Cleanup
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    table.close();
  }

  /**
   * Requirement 3.5 - A single timestamp can be specified.
   */
  @Test
  public void testSingleTimestamp() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = randomData("testrow-");
    byte[] qual = randomData("qual-");
    int numVersions = 5;
    int particularTimestamp = 3;
    long timestamps[] = sequentialTimestamps(numVersions);
    byte[][] values = randomData("value-", numVersions);

    // Insert several timestamps for a single row/column.
    Put put = new Put(rowKey);
    for (int i = 0; i < numVersions; ++i) {
      put.add(COLUMN_FAMILY, qual, timestamps[i], values[i]);
    }
    table.put(put);

    // Get a particular timestamp, and confirm it's returned.
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qual);
    get.setTimeStamp(timestamps[particularTimestamp]);
    get.setMaxVersions(numVersions);
    Result result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertEquals(1, cells.size());
    Assert.assertEquals(timestamps[particularTimestamp], cells.get(0).getTimestamp());
    Assert.assertArrayEquals(values[particularTimestamp], CellUtil.cloneValue(cells.get(0)));

    // Cleanup
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    table.close();
  }

  /**
   * Requirement 3.6 - Client can request a maximum # of most recent versions returned.
   */
  @Test
  public void testMaxVersions() throws IOException {
    // Initialize data
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = randomData("testrow-");
    byte[] qual = randomData("qual-");
    int totalVersions = 5;
    int maxVersions = 3;
    long timestamps[] = sequentialTimestamps(totalVersions);
    byte[][] values = randomData("value-", totalVersions);

    // Insert several versions into the same row/col
    Put put = new Put(rowKey);
    for (int i = 0; i < totalVersions; ++i) {
      put.add(COLUMN_FAMILY, qual, timestamps[i], values[i]);
    }
    table.put(put);

    // Get with maxVersions and confirm we get the last N versions.
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qual);
    get.setMaxVersions(maxVersions);
    Result result = table.get(get);
    Assert.assertEquals(maxVersions, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertEquals(maxVersions, cells.size());

    // Cells return in descending order
    for (int i = totalVersions - 1, j = 0; j < maxVersions; --i, ++j) {
      Assert.assertEquals(timestamps[i], cells.get(j).getTimestamp());
      Assert.assertArrayEquals(values[i], CellUtil.cloneValue(cells.get(j)));
    }

    // Cleanup
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    table.close();
  }

  /**
   * Requirement 3.7 - Specify maximum # of results to return per row, per column family.
   *
   * Requirement 3.8 - Can specify an offset to skip the first N qualifiers in a column family.
   */
  @Test
  public void testMaxResultsPerColumnFamily() throws IOException {
    // Initialize data
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = randomData("testrow-");
    int totalColumns = 10;
    int offsetColumn = 3;
    int maxColumns = 5;
    byte[][] quals = randomData("qual-", totalColumns);
    byte[][] values = randomData("value-", totalColumns);
    long[] timestamps = sequentialTimestamps(totalColumns);

    // Insert a bunch of columns.
    Put put = new Put(rowKey);
    List<QualifierValue> keyValues = new ArrayList<QualifierValue>();
    for (int i = 0; i < totalColumns; ++i) {
      put.add(COLUMN_FAMILY, quals[i], timestamps[i], values[i]);

      // Insert multiple timestamps per row/column to ensure only one cell per column is returned.
      put.add(COLUMN_FAMILY, quals[i], timestamps[i] - 1, values[i]);
      put.add(COLUMN_FAMILY, quals[i], timestamps[i] - 2, values[i]);

      keyValues.add(new QualifierValue(quals[i], values[i]));
    }
    table.put(put);

    // Get max columns with a particular offset.  Values should be ordered by qualifier.
    Get get = new Get(rowKey);
    get.addFamily(COLUMN_FAMILY);
    get.setMaxResultsPerColumnFamily(maxColumns);
    get.setRowOffsetPerColumnFamily(offsetColumn);
    Result result = table.get(get);
    Assert.assertEquals(maxColumns, result.size());
    Cell[] cells = result.rawCells();
    Collections.sort(keyValues);
    for (int i = 0; i < maxColumns; ++i) {
      QualifierValue keyValue = keyValues.get(offsetColumn + i);
      Assert.assertArrayEquals(keyValue.qualifier, CellUtil.cloneQualifier(cells[i]));
      Assert.assertArrayEquals(keyValue.value, CellUtil.cloneValue(cells[i]));
    }

    // Cleanup
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    table.close();
  }

  /**
   * Requirement 3.11 - Result can contain empty values. (zero-length byte[]).
   */
  @Test
  public void testEmptyValues() throws IOException {
    // Initialize data
    HTableInterface table = connection.getTable(TABLE_NAME);
    int numValues = 10;
    byte[] rowKey = randomData("testrow-");
    byte[][] quals = randomData("qual-", numValues);

    // Insert empty values.  Null and byte[0] are interchangeable for puts (but not gets).
    Put put = new Put(rowKey);
    for (int i = 0; i < numValues; ++i) {
      put.add(COLUMN_FAMILY, quals[i], i % 2 == 1 ? null : new byte[0]);
    }
    table.put(put);

    // Check values
    Get get = new Get(rowKey);
    get.addFamily(COLUMN_FAMILY);
    Result result = table.get(get);
    for (int i = 0; i < numValues; ++i) {
      Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, quals[i]));
      Assert.assertArrayEquals(new byte[0], result.getValue(COLUMN_FAMILY, quals[i]));
    }

    // Cleanup
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    table.close();
  }
}
