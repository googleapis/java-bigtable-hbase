/*
 * Copyright 2018 Google LLC
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

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY2;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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

public abstract class AbstractTestPut extends AbstractTest {
  static final int NUM_CELLS = 100;
  static final int NUM_ROWS = 100;

  /**
   * Test inserting a row with multiple cells.
   *
   * @throws IOException
   */
  @Test
  public void testPutMultipleCellsOneRow() throws IOException {
    // Initialize variables
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[][] quals = dataHelper.randomData("testQualifier-", NUM_CELLS);
    byte[][] values = dataHelper.randomData("testValue-", NUM_CELLS);

    // Construct put with NUM_CELL random qualifier/value combos
    Put put = new Put(rowKey);
    List<QualifierValue> keyValues = new ArrayList<QualifierValue>(100);
    for (int i = 0; i < NUM_CELLS; ++i) {
      put.addColumn(COLUMN_FAMILY, quals[i], values[i]);
      keyValues.add(new QualifierValue(quals[i], values[i]));
    }
    table.put(put);

    // Get
    Get get = new Get(rowKey);
    Result result = table.get(get);
    List<Cell> cells = result.listCells();
    Assert.assertEquals(NUM_CELLS, cells.size());

    // Check results in sort order
    Collections.sort(keyValues);
    for (int i = 0; i < NUM_CELLS; ++i) {
      Assert.assertArrayEquals(keyValues.get(i).qualifier, CellUtil.cloneQualifier(cells.get(i)));
      Assert.assertArrayEquals(keyValues.get(i).value, CellUtil.cloneValue(cells.get(i)));
    }

    // Delete
    Delete delete = new Delete(rowKey);
    table.delete(delete);

    // Confirm gone
    Assert.assertFalse(table.exists(get));
    table.close();
  }

  /**
   * Test inserting multiple rows at one time.
   *
   * @throws IOException
   */
  public void testPutGetDeleteMultipleRows() throws IOException {
    // Initialize interface
    Table table = getDefaultTable();
    byte[][] rowKeys = dataHelper.randomData("testrow-", NUM_ROWS);
    byte[][] qualifiers = dataHelper.randomData("testQualifier-", NUM_ROWS);
    byte[][] values = dataHelper.randomData("testValue-", NUM_ROWS);

    // Do puts
    List<Put> puts = new ArrayList<Put>(NUM_ROWS);
    List<String> keys = new ArrayList<String>(NUM_ROWS);
    Map<String, QualifierValue> insertedKeyValues = new TreeMap<String, QualifierValue>();
    for (int i = 0; i < NUM_ROWS; ++i) {
      Put put = new Put(rowKeys[i]);
      put.addColumn(COLUMN_FAMILY, qualifiers[i], values[i]);
      puts.add(put);

      String key = Bytes.toString(rowKeys[i]);
      keys.add(key);
      insertedKeyValues.put(key, new QualifierValue(qualifiers[i], values[i]));
    }
    table.put(puts);

    // Get
    List<Get> gets = new ArrayList<Get>(NUM_ROWS);
    Collections.shuffle(keys); // Retrieve in random order
    for (String key : keys) {
      Get get = new Get(Bytes.toBytes(key));
      get.addColumn(COLUMN_FAMILY, insertedKeyValues.get(key).qualifier);
      gets.add(get);
    }
    Result[] result = table.get(gets);
    Assert.assertEquals(NUM_ROWS, result.length);
    for (int i = 0; i < NUM_ROWS; ++i) {
      String rowKey = keys.get(i);
      Assert.assertEquals(rowKey, Bytes.toString(result[i].getRow()));
      QualifierValue entry = insertedKeyValues.get(rowKey);
      String descriptor = "Row " + i + " (" + rowKey + ": ";
      Assert.assertEquals(descriptor, 1, result[i].size());
      Assert.assertTrue(
          descriptor, result[i].containsNonEmptyColumn(COLUMN_FAMILY, entry.qualifier));
      Assert.assertEquals(
          descriptor,
          entry.value,
          CellUtil.cloneValue(result[i].getColumnCells(COLUMN_FAMILY, entry.qualifier).get(0)));
    }

    // Delete
    List<Delete> deletes = new ArrayList<Delete>(NUM_ROWS);
    for (byte[] rowKey : rowKeys) {
      Delete delete = new Delete(rowKey);
      deletes.add(delete);
    }
    table.delete(deletes);

    // Confirm they are gone
    boolean[] checks = table.existsAll(gets);
    for (Boolean check : checks) {
      Assert.assertFalse(check);
    }
    table.close();
  }

  @Test
  public void testDefaultTimestamp() throws IOException {
    long now = System.currentTimeMillis();
    long oneMinute = 60 * 1000;
    long fifteenMinutes = 15 * 60 * 1000;

    Table table = getDefaultTable();
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, qualifier, value);
    table.put(put);
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qualifier);
    Result result = table.get(get);
    long timestamp1 = result.getColumnLatestCell(COLUMN_FAMILY, qualifier).getTimestamp();
    Assert.assertTrue(
        "Latest timestamp is off by > 15 minutes", Math.abs(timestamp1 - now) < fifteenMinutes);

    try {
      TimeUnit.MILLISECONDS.sleep(10); // Make sure the clock has a chance to move
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("sleep was interrupted", e);
    }
    table.put(put);
    get.addColumn(COLUMN_FAMILY, qualifier);
    result = table.get(get);
    long timestamp2 = result.getColumnLatestCell(COLUMN_FAMILY, qualifier).getTimestamp();
    Assert.assertTrue("Time increases strictly", timestamp2 > timestamp1);
    Assert.assertTrue("Time doesn't move too fast", (timestamp2 - timestamp1) < oneMinute);
    table.close();
  }

  @Test
  @Category(KnownGap.class)
  public void testMultiplePutsOneBadSameRow() throws Exception {
    final int numberOfGoodPuts = 100;
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[][] goodkeys = new byte[numberOfGoodPuts][];
    for (int i = 0; i < numberOfGoodPuts; ++i) {
      goodkeys[i] = rowKey;
    }
    multiplePutsOneBad(numberOfGoodPuts, goodkeys, rowKey);
    Get get = new Get(rowKey);
    Table table = getDefaultTable();
    Result whatsLeft = table.get(get);
    Assert.assertEquals("Same row, all other puts accepted", numberOfGoodPuts, whatsLeft.size());
    table.close();
  }

  @Test
  @Category(KnownGap.class)
  public void testMultiplePutsOneBadDiffRows() throws Exception {
    final int numberOfGoodPuts = 100;
    byte[] badkey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[][] goodkeys = new byte[numberOfGoodPuts][];
    for (int i = 0; i < numberOfGoodPuts; ++i) {
      goodkeys[i] = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
      assert !Arrays.equals(badkey, goodkeys[i]);
    }
    multiplePutsOneBad(numberOfGoodPuts, goodkeys, badkey);

    List<Get> gets = new ArrayList<Get>();
    for (int i = 0; i < numberOfGoodPuts; ++i) {
      Get get = new Get(goodkeys[i]);
      gets.add(get);
    }
    Table table = getDefaultTable();
    Result[] whatsLeft = table.get(gets);
    int cellCount = 0;
    for (Result result : whatsLeft) {
      cellCount += result.size();
    }
    Assert.assertEquals("Different row, all other puts accepted", numberOfGoodPuts, cellCount);
    table.close();
  }

  @Test
  public void testMultipleFamilies() throws IOException {
    // Initialize variables
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("multiFamRow-");
    byte[][] quals = dataHelper.randomData("testQualifier-", NUM_CELLS);
    byte[][] values = dataHelper.randomData("testValue-", NUM_CELLS * 2);

    // Construct put with NUM_CELL random qualifier/value combos
    Put put = new Put(rowKey);
    List<QualifierValue> family1KeyValues = new ArrayList<QualifierValue>(100);
    List<QualifierValue> family2KeyValues = new ArrayList<QualifierValue>(100);
    for (int i = 0; i < NUM_CELLS; i++) {
      put.addColumn(COLUMN_FAMILY, quals[i], values[i]);
      family1KeyValues.add(new QualifierValue(quals[i], values[i]));
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY2, quals[i], values[NUM_CELLS + i]);
      family2KeyValues.add(new QualifierValue(quals[i], values[NUM_CELLS + i]));
    }
    table.put(put);

    // Get
    Get get = new Get(rowKey);
    Result result = table.get(get);
    List<Cell> cells = result.listCells();
    Assert.assertEquals(NUM_CELLS * 2, cells.size());
    Assert.assertTrue(
        "CF 1 should sort before CF2", Bytes.compareTo(COLUMN_FAMILY, COLUMN_FAMILY2) < 0);
    // Check results in sort order
    Collections.sort(family1KeyValues);
    for (int i = 0; i < NUM_CELLS; ++i) {
      Assert.assertArrayEquals(COLUMN_FAMILY, CellUtil.cloneFamily(cells.get(i)));
      Assert.assertArrayEquals(
          family1KeyValues.get(i).qualifier, CellUtil.cloneQualifier(cells.get(i)));
      Assert.assertArrayEquals(family1KeyValues.get(i).value, CellUtil.cloneValue(cells.get(i)));
    }
    Collections.sort(family2KeyValues);
    for (int i = 0; i < NUM_CELLS; ++i) {
      int rowIndex = i + NUM_CELLS;
      Assert.assertArrayEquals(COLUMN_FAMILY2, CellUtil.cloneFamily(cells.get(rowIndex)));
      Assert.assertEquals(
          Bytes.toString(family2KeyValues.get(i).qualifier),
          Bytes.toString(CellUtil.cloneQualifier(cells.get(rowIndex))));
      Assert.assertEquals(
          Bytes.toString(family2KeyValues.get(i).value),
          Bytes.toString(CellUtil.cloneValue(cells.get(rowIndex))));
    }
    // Delete
    Delete delete = new Delete(rowKey);
    table.delete(delete);

    // Confirm gone
    Assert.assertFalse(table.exists(get));
    table.close();
  }

  private void multiplePutsOneBad(int numberOfGoodPuts, byte[][] goodkeys, byte[] badkey)
      throws IOException {
    Table table = getDefaultTable();
    List<Put> puts = new ArrayList<Put>();
    for (int i = 0; i < numberOfGoodPuts; ++i) {
      byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
      byte[] value = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
      Put put = new Put(goodkeys[i]);
      put.addColumn(COLUMN_FAMILY, qualifier, value);
      puts.add(put);
    }

    // Insert a bad put in the middle
    byte[] badfamily = Bytes.toBytes("badcolumnfamily-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(badkey);
    put.addColumn(badfamily, qualifier, value);
    puts.add(numberOfGoodPuts / 2, put);
    RetriesExhaustedWithDetailsException thrownException = null;
    try {
      table.put(puts);
    } catch (RetriesExhaustedWithDetailsException e) {
      thrownException = e;
    }
    Assert.assertNotNull("Exception should have been thrown", thrownException);
    Assert.assertEquals("Expecting one exception", 1, thrownException.getNumExceptions());
    Assert.assertArrayEquals("Row key", badkey, thrownException.getRow(0).getRow());
    Assert.assertTrue(
        "Cause: NoSuchColumnFamilyException",
        thrownException.getCause(0) instanceof NoSuchColumnFamilyException);
    table.close();
  }

  @Test
  public void testPutSameTimestamp() throws Exception {
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testqual-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value1 = Bytes.toBytes("testvalue-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value2 = Bytes.toBytes("testvalue-" + RandomStringUtils.randomAlphanumeric(8));
    long timestamp = System.currentTimeMillis();
    Table table = getDefaultTable();
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, qualifier, timestamp, value1);
    table.put(put);
    put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, qualifier, timestamp, value2);
    table.put(put);
    Get get = getGetAddColumnVersion(5, rowKey, qualifier);
    Result result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qualifier));
    Assert.assertEquals(
        timestamp, result.getColumnLatestCell(COLUMN_FAMILY, qualifier).getTimestamp());
    Assert.assertArrayEquals(
        value2, CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qualifier)));
    table.close();
  }

  @Test
  public void testPutWithNullValues() throws IOException {
    // Initialize
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    byte[] testValue = dataHelper.randomData("testValue-");

    try (Table table = getDefaultTable()) {
      Exception actualError = null;
      try {
        // Should throw exception with null Put
        table.put((Put) null);
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNotNull(actualError);
      actualError = null;

      try {
        // Should throw exception with null Put
        table.put((List<Put>) null);
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNotNull(actualError);
      actualError = null;

      try {
        byte[] rowKeyWithNullQual = dataHelper.randomData("testrow-");

        // Should add a row without any qualifier.
        table.put(new Put(rowKeyWithNullQual).addColumn(COLUMN_FAMILY, null, testValue));

        Result result = table.get(new Get(rowKeyWithNullQual));
        assertEquals(1, result.rawCells().length);
        Cell cell = result.getColumnLatestCell(COLUMN_FAMILY, null);
        assertArrayEquals(testValue, CellUtil.cloneValue(cell));

      } catch (Exception ex) {
        actualError = ex;
      }
      assertNull(actualError);

      try {
        byte[] rowKeyWithEmptyQual = dataHelper.randomData("testrow-");
        byte[] emptyQualifier = new byte[0];

        // should create a row without any qualifier
        table.put(new Put(rowKeyWithEmptyQual).addColumn(COLUMN_FAMILY, emptyQualifier, testValue));

        Result result = table.get(new Get(rowKeyWithEmptyQual));
        assertEquals(1, result.rawCells().length);
        Cell cell = result.getColumnLatestCell(COLUMN_FAMILY, emptyQualifier);
        assertArrayEquals(testValue, CellUtil.cloneValue(cell));
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNull(actualError);

      try {
        byte[] rowKeyEmptyValue = dataHelper.randomData("testrow-");

        // Should create a row with a single column
        table.put(new Put(rowKeyEmptyValue).addColumn(COLUMN_FAMILY, testQualifier, null));

        Result result = table.get(new Get(rowKeyEmptyValue));
        assertEquals(1, result.rawCells().length);
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNull(actualError);

      try {
        byte[] rowKeyEmptyValue = dataHelper.randomData("testrow-");

        // Should create a row with a single column
        table.put(new Put(rowKeyEmptyValue).addColumn(COLUMN_FAMILY, testQualifier, new byte[0]));

        Result result = table.get(new Get(rowKeyEmptyValue));
        assertEquals(1, result.rawCells().length);
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNull(actualError);
    }
  }

  protected abstract Get getGetAddColumnVersion(int version, byte[] rowKey, byte[] qualifier)
      throws IOException;
}
