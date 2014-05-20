package com.google.cloud.anviltop.hbase;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.validation.constraints.NotNull;

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
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));

    // Construct put with NUM_CELL random qualifier/value combos
    Put put = new Put(rowKey);
    List<QualifierAndValue> insertedValues = new ArrayList<QualifierAndValue>(100);
    for (int i = 0; i < NUM_CELLS; ++i) {
      String qualifier = "testQualifier-" + RandomStringUtils.randomAlphanumeric(8);
      String value = "testValue-" + RandomStringUtils.randomAlphanumeric(8);
      put.add(COLUMN_FAMILY, Bytes.toBytes(qualifier), Bytes.toBytes(value));
      insertedValues.add(new QualifierAndValue(qualifier, value));
    }
    table.put(put);

    // Get
    Get get = new Get(rowKey);
    Result result = table.get(get);
    List<Cell> cells = result.listCells();
    Assert.assertEquals(NUM_CELLS, cells.size());

    // Check results in sort order
    Collections.sort(insertedValues);
    for (int i = 0; i < NUM_CELLS; ++i) {
      String qualifier = insertedValues.get(i).qualifier;
      String value = insertedValues.get(i).value;
      Assert.assertEquals(qualifier, Bytes.toString(CellUtil.cloneQualifier(cells.get(i))));
      Assert.assertEquals(value, Bytes.toString(CellUtil.cloneValue(cells.get(i))));
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

    // Do puts
    List<Put> puts = new ArrayList<Put>(NUM_ROWS);
    List<String> rowKeys = new ArrayList<String>(NUM_ROWS);
    Map<String, QualifierAndValue> insertedKeyValues = new TreeMap<String, QualifierAndValue>();
    for (int i = 0; i < NUM_ROWS; ++i) {
      String rowKey = "testrow-" + RandomStringUtils.randomAlphanumeric(8);
      String qualifier = "testQualifier-" + RandomStringUtils.randomAlphanumeric(8);
      String value = "testValue-" + RandomStringUtils.randomAlphanumeric(8);

      Put put = new Put(Bytes.toBytes(rowKey));
      put.add(COLUMN_FAMILY, Bytes.toBytes(qualifier), Bytes.toBytes(value));
      puts.add(put);

      insertedKeyValues.put(rowKey, new QualifierAndValue(qualifier, value));
    }
    table.put(puts);

    // Get
    List<Get> gets = new ArrayList<Get>(NUM_ROWS);
    Collections.shuffle(rowKeys);  // Retrieve in random order
    for (String rowKey : rowKeys) {
      Get get = new Get(Bytes.toBytes(rowKey));
      String qualifier = insertedKeyValues.get(rowKey).qualifier;
      get.addColumn(COLUMN_FAMILY, Bytes.toBytes(qualifier));
      gets.add(get);
    }
    Result[] result = table.get(gets);
    Assert.assertEquals(NUM_ROWS, result.length);
    for (int i = 0; i < NUM_ROWS; ++i) {
      String rowKey = rowKeys.get(i);
      Assert.assertEquals(rowKey, Bytes.toString(result[i].getRow()));
      QualifierAndValue entry = insertedKeyValues.get(rowKey);
      String descriptor = "Row " + i + " (" + rowKey + ": ";
      Assert.assertEquals(descriptor, 1, result[i].size());
      Assert.assertTrue(descriptor, result[i].containsNonEmptyColumn(COLUMN_FAMILY,
          Bytes.toBytes(entry.qualifier)));
      Assert.assertEquals(descriptor, entry.value,
          Bytes.toString(CellUtil.cloneValue(
              result[i].getColumnCells(COLUMN_FAMILY, Bytes.toBytes(entry.qualifier)).get(0)))
      );
    }

    // Delete
    List<Delete> deletes = new ArrayList<Delete>(NUM_ROWS);
    for (String rowKey : rowKeys) {
      Delete delete = new Delete(Bytes.toBytes(rowKey));
      String qualifier = insertedKeyValues.get(rowKey).qualifier;
      delete.deleteColumn(COLUMN_FAMILY, Bytes.toBytes(qualifier));
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

  @Test(expected = IOException.class)
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

  private static class QualifierAndValue implements Comparable<QualifierAndValue> {
    private final String qualifier;
    private final String value;

    public QualifierAndValue(@NotNull String qualifier, @NotNull String value) {
      this.qualifier = qualifier;
      this.value = value;
    }

    @Override
    public int compareTo(QualifierAndValue qualifierAndValue) {
      return qualifier.compareTo(qualifierAndValue.qualifier);
    }
  }
}
