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
package com.google.cloud.bigtable.hbase;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public class TestGet extends AbstractTest {
  /**
   * Requirement 3.2 - If a column family is requested, but no qualifier, all columns in that family
   * are returned
   */
  @Test
  public void testNoQualifier() throws IOException {
    // Initialize variables
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    int numValues = 3;
    byte[][] quals = dataHelper.randomData("qual-", numValues);
    byte[][] values = dataHelper.randomData("value-", numValues);

    // Insert some columns
    Put put = new Put(rowKey);
    for (int i = 0; i < numValues; ++i) {
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i], values[i]);
    }
    table.put(put);

    // Get without a qualifer, and confirm all results are returned.
    Get get = new Get(rowKey);
    get.addFamily(SharedTestEnvRule.COLUMN_FAMILY);
    Result result = table.get(get);
    Assert.assertEquals(numValues, result.size());
    for (int i = 0; i < numValues; ++i) {
      Assert.assertTrue(result.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i]));
      Assert.assertArrayEquals(values[i],
          CellUtil.cloneValue(result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, quals[i])));
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
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    int numValues = 3;
    byte[][] quals = dataHelper.randomData("qual-", numValues);
    byte[][] values = dataHelper.randomData("value-", numValues);

    // Insert a few columns
    Put put = new Put(rowKey);
    for (int i = 0; i < numValues; ++i) {
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i], values[i]);
    }
    table.put(put);

    // Select some, but not all columns, and confirm that's what's returned.
    Get get = new Get(rowKey);
    int[] colsToSelect = { 0, 2 };
    for (int i : colsToSelect) {
      get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i]);
    }
    Result result = table.get(get);
    Assert.assertEquals(colsToSelect.length, result.size());
    for (int i : colsToSelect) {
      Assert.assertTrue(result.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i]));
      Assert.assertArrayEquals(values[i],
          CellUtil.cloneValue(result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, quals[i])));
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
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    int numVersions = 5;
    int minVersion = 1;
    int maxVersion = 4;
    long timestamps[] = dataHelper.sequentialTimestamps(numVersions);
    byte[][] values = dataHelper.randomData("value-", numVersions);

    // Insert values with different timestamps at the same column.
    Put put = new Put(rowKey);
    for (int i = 0; i < numVersions; ++i) {
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, timestamps[i], values[i]);
    }
    table.put(put);

    // Get with a time range, and return the correct cells are returned.
    Get get = new Get(rowKey);
    get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual);
    get.setTimeRange(timestamps[minVersion], timestamps[maxVersion]);
    get.readVersions(numVersions);
    Result result = table.get(get);
    Assert.assertEquals(maxVersion - minVersion, result.size());
    Assert.assertTrue(result.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, qual));
    List<Cell> cells = result.getColumnCells(SharedTestEnvRule.COLUMN_FAMILY, qual);
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
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    int numVersions = 5;
    int particularTimestamp = 3;
    long timestamps[] = dataHelper.sequentialTimestamps(numVersions);
    byte[][] values = dataHelper.randomData("value-", numVersions);

    // Insert several timestamps for a single row/column.
    Put put = new Put(rowKey);
    for (int i = 0; i < numVersions; ++i) {
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, timestamps[i], values[i]);
    }
    table.put(put);

    // Get a particular timestamp, and confirm it's returned.
    Get get = new Get(rowKey);
    get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual);
    get.setTimeStamp(timestamps[particularTimestamp]);
    get.readVersions(numVersions);
    Result result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, qual));
    List<Cell> cells = result.getColumnCells(SharedTestEnvRule.COLUMN_FAMILY, qual);
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
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    int totalVersions = 5;
    int maxVersions = 3;
    long timestamps[] = dataHelper.sequentialTimestamps(totalVersions);
    byte[][] values = dataHelper.randomData("value-", totalVersions);

    // Insert several versions into the same row/col
    Put put = new Put(rowKey);
    for (int i = 0; i < totalVersions; ++i) {
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, timestamps[i], values[i]);
    }
    table.put(put);

    // Get with maxVersions and confirm we get the last N versions.
    Get get = new Get(rowKey);
    get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual);
    get.readVersions(maxVersions);
    Result result = table.get(get);
    Assert.assertEquals(maxVersions, result.size());
    Assert.assertTrue(result.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, qual));
    List<Cell> cells = result.getColumnCells(SharedTestEnvRule.COLUMN_FAMILY, qual);
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
  @Category(KnownGap.class)
  public void testMaxResultsPerColumnFamily() throws IOException {
    // Initialize data
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("testrow-");
    int totalColumns = 10;
    int offsetColumn = 3;
    int maxColumns = 5;
    byte[][] quals = dataHelper.randomData("qual-", totalColumns);
    byte[][] values = dataHelper.randomData("value-", totalColumns);
    long[] timestamps = dataHelper.sequentialTimestamps(totalColumns);

    // Insert a bunch of columns.
    Put put = new Put(rowKey);
    List<QualifierValue> keyValues = new ArrayList<QualifierValue>();
    for (int i = 0; i < totalColumns; ++i) {
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i], timestamps[i], values[i]);

      // Insert multiple timestamps per row/column to ensure only one cell per column is returned.
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i], timestamps[i] - 1, values[i]);
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i], timestamps[i] - 2, values[i]);

      keyValues.add(new QualifierValue(quals[i], values[i]));
    }
    table.put(put);

    // Get max columns with a particular offset.  Values should be ordered by qualifier.
    Get get = new Get(rowKey);
    get.addFamily(SharedTestEnvRule.COLUMN_FAMILY);
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
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    int numValues = 10;
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[][] quals = dataHelper.randomData("qual-", numValues);

    // Insert empty values.  Null and byte[0] are interchangeable for puts (but not gets).
    Put put = new Put(rowKey);
    for (int i = 0; i < numValues; ++i) {
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i], i % 2 == 1 ? null : new byte[0]);
    }
    table.put(put);

    // Check values
    Get get = new Get(rowKey);
    get.addFamily(SharedTestEnvRule.COLUMN_FAMILY);
    Result result = table.get(get);
    for (int i = 0; i < numValues; ++i) {
      Assert.assertTrue(result.containsColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i]));
      Assert.assertArrayEquals(new byte[0], result.getValue(SharedTestEnvRule.COLUMN_FAMILY, quals[i]));
    }

    // Cleanup
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    table.close();
  }

  /**
   * Requirement 3.12 - Exists tests whether one or more of the row/columns exists, as specified in
   * the Get object.
   *
   * An OR operation. If multiple columns are in the Get, then only one has to exist.
   */
  @Test
  @Category(KnownGap.class)
  public void testExists() throws IOException {
    // Initialize data
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    int numValues = 10;
    byte[][] rowKeys = dataHelper.randomData("testrow-", numValues);
    byte[][] quals = dataHelper.randomData("qual-", numValues);
    byte[][] values = dataHelper.randomData("value-", numValues);

    // Insert a bunch of data
    List<Put> puts = new ArrayList<Put>(numValues);
    for (int i = 0; i < numValues; ++i) {
      Put put = new Put(rowKeys[i]);
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i], values[0]);
      puts.add(put);
    }
    table.put(puts);

    // Test just keys, both individually and as batch.
    List<Get> gets = new ArrayList<Get>(numValues);
    for(int i = 0; i < numValues; ++i) {
      Get get = new Get(rowKeys[i]);
      Assert.assertTrue(table.exists(get));
      gets.add(get);
    }
    boolean[] exists = table.exists(gets);
    Assert.assertEquals(numValues, exists.length);
    for (int i = 0; i < numValues; ++i) {
      Assert.assertTrue(exists[i]);
    }

    // Test keys and familes, both individually and as batch.
    gets.clear();
    for(int i = 0; i < numValues; ++i) {
      Get get = new Get(rowKeys[i]);
      get.addFamily(SharedTestEnvRule.COLUMN_FAMILY);
      Assert.assertTrue(table.exists(get));
      gets.add(get);
    }
    exists = table.exists(gets);
    Assert.assertEquals(numValues, exists.length);
    for (int i = 0; i < numValues; ++i) {
      Assert.assertTrue(exists[i]);
    }

    // Test keys and qualifiers, both individually and as batch.
    gets.clear();
    for(int i = 0; i < numValues; ++i) {
      Get get = new Get(rowKeys[i]);
      get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i]);
      Assert.assertTrue(table.exists(get));
      gets.add(get);
    }
    exists = table.exists(gets);
    Assert.assertEquals(numValues, exists.length);
    for (int i = 0; i < numValues; ++i) {
      Assert.assertTrue(exists[i]);
    }

    // Test bad rows, both individually and as batch
    gets.clear();
    for(int i = 0; i < numValues; ++i) {
      Get get = new Get(dataHelper.randomData("badRow-"));
      Assert.assertFalse(table.exists(get));
      gets.add(get);
    }
    exists = table.exists(gets);
    Assert.assertEquals(numValues, exists.length);
    for (int i = 0; i < numValues; ++i) {
      Assert.assertFalse(exists[i]);
    }

    // Test bad column family, both individually and as batch
    gets.clear();
    for(int i = 0; i < numValues; ++i) {
      Get get = new Get(rowKeys[i]);
      get.addFamily(dataHelper.randomData("badFamily-"));
      boolean throwsException = false;
      try {
        table.exists(get);
      } catch (NoSuchColumnFamilyException e) {
        throwsException = true;
      }
      Assert.assertTrue(throwsException);
      gets.add(get);
    }
    boolean throwsException = false;
    try {
      table.exists(gets);
    } catch (RetriesExhaustedWithDetailsException e) {
      throwsException = true;
      Assert.assertEquals(numValues, e.getNumExceptions());
    }
    Assert.assertTrue(throwsException);

    // Test bad qualifier, both individually and as batch
    gets.clear();
    for (int i = 0; i < numValues; ++i) {
      Get get = new Get(rowKeys[i]);
      get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, dataHelper.randomData("badColumn-"));
      Assert.assertFalse(table.exists(get));
      gets.add(get);
    }
    exists = table.exists(gets);
    Assert.assertEquals(numValues, exists.length);
    for (int i = 0; i < numValues; ++i) {
      Assert.assertFalse(exists[i]);
    }

    // Test correct OR behavior on a per-row basis.
    gets.clear();
    for (int i = 0; i < numValues; ++i) {
      Get get = new Get(rowKeys[i]);
      get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, quals[i]);
      get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, dataHelper.randomData("badColumn-"));
      Assert.assertTrue(table.exists(get));
      gets.add(get);
    }
    exists = table.exists(gets);
    Assert.assertEquals(numValues, exists.length);
    for (int i = 0; i < numValues; ++i) {
      Assert.assertTrue(exists[i]);
    }
  }

  /**
   * Requirement 3.16 - When submitting an array of Get operations, if one fails, they all fail.
   */
  @Test
  public void testOneBadApple() throws IOException {
    // Initialize data
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    int numValues = 10;

    // Run a control test.
    List<Get> gets = new ArrayList<Get>(numValues + 1);
    for (int i = 0; i < numValues; ++i) {
      Get get = new Get(dataHelper.randomData("key-"));
      get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, dataHelper.randomData("qual-"));
      gets.add(get);
    }
    table.get(gets);

    // Now add a poison get.
    Get get = new Get(dataHelper.randomData("testRow-"));
    get.addColumn(
        dataHelper.randomData("badFamily-"),
        dataHelper.randomData("qual-"));
    gets.add(get);

    boolean throwsException = false;
    try {
      table.get(gets);
    } catch (RetriesExhaustedWithDetailsException e) {
      throwsException = true;
      Assert.assertEquals(1, e.getNumExceptions());
    }
    Assert.assertTrue("Expected an exception", throwsException);
  }

  @Test
  public void testReadSpecialCharactersInColumnQualifiers() throws IOException {
    byte[][] qualifiers = new byte[][] {
        Bytes.toBytes("}"),
        Bytes.toBytes("{"),
        Bytes.toBytes("@"),
        Bytes.toBytes("}{"),
        Bytes.toBytes("@}}"),
        Bytes.toBytes("@@}}"),
        Bytes.toBytes("@{{"),
        Bytes.toBytes("@@{{")
    };
    byte[][] values = dataHelper.randomData("value-", qualifiers.length);
    byte[] rowKey = dataHelper.randomData("rowKey");
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    Put put = new Put(rowKey);

    for (int i = 0; i < qualifiers.length; i++) {
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifiers[i], values[i]);
    }
    table.put(put);

    Get get = new Get(rowKey);
    for (int i = 0; i < qualifiers.length; i++) {
      get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifiers[i]);
    }

    Result result = table.get(get);
    Assert.assertEquals(qualifiers.length, result.listCells().size());

    for (int i = 0; i < qualifiers.length; i++) {
      byte[] value = CellUtil.cloneValue(result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifiers[i]));
      Assert.assertArrayEquals(values[i], value);
    }
  }

  @Test
  public void testOrder() throws IOException {
    TableName tableName = sharedTestEnv.newTestTableName();
    List<String> randomFamilies = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      randomFamilies.add(UUID.randomUUID().toString());
    }
    try (Admin admin = getConnection().getAdmin()) {
      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      for (String family : randomFamilies) {
        tableDescriptor.addFamily(new HColumnDescriptor(family));
      }
      admin.createTable(tableDescriptor);
      try (Table table = getConnection().getTable(tableName)) {
        Collections.shuffle(randomFamilies);
        byte[] rowKey = Bytes.toBytes("rowKey");
        Put put = new Put(rowKey);
        for (String family : randomFamilies) {
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(UUID.randomUUID().toString()),
            Bytes.toBytes(UUID.randomUUID().toString()));
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(UUID.randomUUID().toString()),
            Bytes.toBytes(UUID.randomUUID().toString()));
        }
        table.put(put);
        Result result = table.get(new Get(rowKey));
        Cell[] rawCells = result.rawCells();
        Set<Cell> ordered = new TreeSet<>(KeyValue.COMPARATOR);
        ordered.addAll(Arrays.asList(rawCells));
        Cell[] orderedCells = ordered.toArray(new Cell[0]);
        Assert.assertArrayEquals(orderedCells, rawCells);
      } finally {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    }
  }
}
