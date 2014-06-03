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

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.junit.Assert;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.validation.constraints.NotNull;

public class TestPut extends AbstractTest {
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
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = randomData("testrow-");
    byte[][] quals = randomData("testQualifier-", NUM_CELLS);
    byte[][] values = randomData("testValue-", NUM_CELLS);

    // Construct put with NUM_CELL random qualifier/value combos
    Put put = new Put(rowKey);
    List<QualifierAndValue> keyValues = new ArrayList<QualifierAndValue>(100);
    for (int i = 0; i < NUM_CELLS; ++i) {
      put.add(COLUMN_FAMILY, quals[i], values[i]);
      keyValues.add(new QualifierAndValue(quals[i], values[i]));
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
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[][] rowKeys = randomData("testrow-", NUM_ROWS);
    byte[][] qualifiers = randomData("testQualifier-", NUM_ROWS);
    byte[][] values = randomData("testValue-", NUM_ROWS);

    // Do puts
    List<Put> puts = new ArrayList<Put>(NUM_ROWS);
    List<String> keys = new ArrayList<String>(NUM_ROWS);
    Map<String, QualifierAndValue> insertedKeyValues = new TreeMap<String, QualifierAndValue>();
    for (int i = 0; i < NUM_ROWS; ++i) {
      Put put = new Put(rowKeys[i]);
      put.add(COLUMN_FAMILY, qualifiers[i], values[i]);
      puts.add(put);

      String key = Bytes.toString(rowKeys[i]);
      keys.add(key);
      insertedKeyValues.put(key, new QualifierAndValue(qualifiers[i], values[i]));
    }
    table.put(puts);

    // Get
    List<Get> gets = new ArrayList<Get>(NUM_ROWS);
    Collections.shuffle(keys);  // Retrieve in random order
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
      QualifierAndValue entry = insertedKeyValues.get(rowKey);
      String descriptor = "Row " + i + " (" + rowKey + ": ";
      Assert.assertEquals(descriptor, 1, result[i].size());
      Assert.assertTrue(descriptor,
          result[i].containsNonEmptyColumn(COLUMN_FAMILY, entry.qualifier));
      Assert.assertEquals(descriptor, entry.value,
          CellUtil.cloneValue(result[i].getColumnCells(COLUMN_FAMILY, entry.qualifier).get(0))
      );
    }

    // Delete
    List<Delete> deletes = new ArrayList<Delete>(NUM_ROWS);
    for (byte[] rowKey : rowKeys) {
      Delete delete = new Delete(rowKey);
      deletes.add(delete);
    }
    table.delete(deletes);

    // Confirm they are gone
    Boolean[] checks = table.exists(gets);
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

    HTableInterface table = connection.getTable(TABLE_NAME);
    table.setAutoFlushTo(true);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qualifier, value);
    table.put(put);
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qualifier);
    Result result = table.get(get);
    long timestamp1 = result.getColumnLatestCell(COLUMN_FAMILY, qualifier).getTimestamp();
    Assert.assertTrue(Math.abs(timestamp1 - now) < fifteenMinutes);

    try {
      Thread.sleep(10);  // Make sure the clock has a chance to move
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    table.put(put);
    get.addColumn(COLUMN_FAMILY, qualifier);
    result = table.get(get);
    long timestamp2 = result.getColumnLatestCell(COLUMN_FAMILY, qualifier).getTimestamp();
    Assert.assertTrue("Time increases strictly", timestamp2 > timestamp1);
    Assert.assertTrue("Time doesn't move too fast", (timestamp2 - timestamp1) < oneMinute);
    table.close();
  }

  @Test(expected = RetriesExhaustedWithDetailsException.class)
  public void testIOExceptionOnFailedPut() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME);
    table.setAutoFlushTo(true);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] badfamily = Bytes.toBytes("badcolumnfamily-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(rowKey);
    put.add(badfamily, qualifier, value);
    table.put(put);
  }

  @Test
  public void testAtomicPut() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] goodQual = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] goodValue = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] badQual = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] badValue = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] badfamily = Bytes.toBytes("badcolumnfamily-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, goodQual, goodValue);
    put.add(badfamily, badQual, badValue);
    RetriesExhaustedWithDetailsException thrownException = null;
    try {
      table.put(put);
    } catch (RetriesExhaustedWithDetailsException e) {
      thrownException = e;
    }
    Assert.assertNotNull("Exception should have been thrown", thrownException);
    Assert.assertEquals("Expecting one exception", 1, thrownException.getNumExceptions());
    Assert.assertArrayEquals("Row key", rowKey, thrownException.getRow(0).getRow());
    Assert.assertTrue("Cause: NoSuchColumnFamilyException",
        thrownException.getCause(0) instanceof NoSuchColumnFamilyException);

    Get get = new Get(rowKey);
    Result result = table.get(get);
    Assert.assertEquals("Atomic behavior means there should be nothing here", 0, result.size());
    table.close();
  }

  /**
   * This tests particularly odd behavior, where if an error happens on the client-side validation
   * of a list of puts, the commits after the bad put fail.  (This is unlike a server-side error
   * where all the good puts are committed.)
   */
  @Test
  public void testClientSideValidationError() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME);
    table.setAutoFlushTo(false);
    byte[] rowKey1 = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qual1 = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value1 = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] rowKey2 = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    // No column.  This will cause an error during client-side validation.
    byte[] rowKey3 = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qual3 = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value3 = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));

    List<Put> puts = new ArrayList<Put>();
    Put put1 = new Put(rowKey1);
    put1.add(COLUMN_FAMILY, qual1, value1);
    puts.add(put1);
    Put put2 = new Put(rowKey2);
    puts.add(put2);
    Put put3 = new Put(rowKey3);
    put3.add(COLUMN_FAMILY, qual3, value3);
    puts.add(put3);
    boolean exceptionThrown = false;
    try {
      table.put(puts);
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue("Exception should have been thrown", exceptionThrown);
    Get get1 = new Get(rowKey1);
    Assert.assertFalse("Row 1 should not exist yet", table.exists(get1));
    table.flushCommits();

    Assert.assertTrue("Row 1 should exist", table.exists(get1));
    Get get2 = new Get(rowKey2);
    Assert.assertFalse("Row 2 should not exist", table.exists(get2));
    Get get3 = new Get(rowKey3);
    Assert.assertFalse("Row 3 should not exist", table.exists(get3));

    table.close();
  }

  @Test
  public void testMultiplePutsOneBadSameRow() throws Exception {
    final int numberOfGoodPuts = 100;
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[][] goodkeys = new byte[numberOfGoodPuts][];
    for (int i = 0; i < numberOfGoodPuts; ++i) {
      goodkeys[i] = rowKey;
    }
    multiplePutsOneBad(numberOfGoodPuts, goodkeys, rowKey);
    Get get = new Get(rowKey);
    HTableInterface table = connection.getTable(TABLE_NAME);
    Result whatsLeft = table.get(get);
    Assert.assertEquals("Same row, all other puts accepted", numberOfGoodPuts, whatsLeft.size());
    table.close();
  }

  @Test
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
    HTableInterface table = connection.getTable(TABLE_NAME);
    Result[] whatsLeft = table.get(gets);
    int cellCount = 0;
    for (Result result : whatsLeft) {
      cellCount += result.size();
    }
    Assert.assertEquals("Different row, all other puts accepted", numberOfGoodPuts, cellCount);
    table.close();
  }

  private void multiplePutsOneBad(int numberOfGoodPuts, byte[][] goodkeys, byte[] badkey)
      throws IOException {
    HTableInterface table = connection.getTable(TABLE_NAME);
    table.setAutoFlushTo(true);
    List<Put> puts = new ArrayList<Put>();
    for (int i = 0; i < numberOfGoodPuts; ++i) {
      byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
      byte[] value = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
      Put put = new Put(goodkeys[i]);
      put.add(COLUMN_FAMILY, qualifier, value);
      puts.add(put);
    }

    // Insert a bad put in the middle
    byte[] badfamily = Bytes.toBytes("badcolumnfamily-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(badkey);
    put.add(badfamily, qualifier, value);
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
    Assert.assertTrue("Cause: NoSuchColumnFamilyException",
        thrownException.getCause(0) instanceof NoSuchColumnFamilyException);
    table.close();
  }
}
