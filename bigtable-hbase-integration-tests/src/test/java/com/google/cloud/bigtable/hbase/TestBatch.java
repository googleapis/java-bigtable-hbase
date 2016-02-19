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

import static com.google.cloud.bigtable.hbase.IntegrationTests.COLUMN_FAMILY;
import static com.google.cloud.bigtable.hbase.IntegrationTests.TABLE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class TestBatch extends AbstractTest {
  /**
   * Requirement 8.1 - Batch performs a collection of Deletes, Gets, Puts, Increments, and Appends
   * on multiple rows, returning results in the same order as the requested actions.
   *
   * Requirement 8.5 - A batch() should return an empty Result object for successful put/delete
   * operations and get operations with no matching column.
   *
   * Requirement 8.6 - Get operations with matching values should return populated Result object in
   * a batch() operation.
   */
  @Test
  public void testBatchPutGetAndDelete() throws IOException, InterruptedException {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey1 = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] rowKey2 = dataHelper.randomData("testrow-");
    byte[] qual2 = dataHelper.randomData("qual-");
    byte[] value2 = dataHelper.randomData("value-");
    byte[] emptyRowKey = dataHelper.randomData("testrow-");

    Put put1 = new Put(rowKey1).addColumn(COLUMN_FAMILY, qual1, value1);
    Put put2 = new Put(rowKey2).addColumn(COLUMN_FAMILY, qual2, value2);
    List<Row> batch = new ArrayList<Row>(2);
    batch.add(put1);
    batch.add(put2);
    Object[] results = new Object[batch.size()];
    table.batch(batch, results);
    Assert.assertTrue("Should be a Result", results[0] instanceof Result);
    Assert.assertTrue("Should be a Result", results[1] instanceof Result);
    Assert.assertTrue("Should be empty", ((Result) results[0]).isEmpty());
    Assert.assertTrue("Should be empty", ((Result) results[1]).isEmpty());
    Assert.assertEquals("Batch should not have been cleared", 2, batch.size());

    // Check values
    Get get1 = new Get(rowKey1);
    Get get2 = new Get(rowKey2);
    Get get3 = new Get(emptyRowKey);
    batch.clear();
    batch.add(get1);
    batch.add(get2);
    batch.add(get3);
    results = new Object[batch.size()];
    table.batch(batch, results);
    Assert.assertTrue("Should be Result", results[0] instanceof Result);
    Assert.assertTrue("Should be Result", results[1] instanceof Result);
    Assert.assertTrue("Should be Result", results[2] instanceof Result);
    Assert.assertEquals("Should be one value", 1, ((Result) results[0]).size());
    Assert.assertEquals("Should be one value", 1, ((Result) results[1]).size());
    Assert.assertEquals("Should be empty", 0, ((Result) results[2]).size());
    Assert.assertArrayEquals("Should be value1", value1,
      CellUtil.cloneValue(((Result) results[0]).getColumnLatestCell(COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals("Should be value2", value2,
      CellUtil.cloneValue(((Result) results[1]).getColumnLatestCell(COLUMN_FAMILY, qual2)));

    // Delete values
    Delete delete1 = new Delete(rowKey1);
    Delete delete2 = new Delete(rowKey2);
    batch.clear();
    batch.add(delete1);
    batch.add(delete2);
    results = new Object[batch.size()];
    table.batch(batch, results);
    Assert.assertTrue("Should be a Result", results[0] instanceof Result);
    Assert.assertTrue("Should be a Result", results[1] instanceof Result);
    Assert.assertTrue("Should be empty", ((Result) results[0]).isEmpty());
    Assert.assertTrue("Should be empty", ((Result) results[1]).isEmpty());

    // Check that delete succeeded
    batch.clear();
    batch.add(get1);
    batch.add(get2);
    results = new Object[batch.size()];
    table.batch(batch, results);
    Assert.assertTrue("Should be empty", ((Result) results[0]).isEmpty());
    Assert.assertTrue("Should be empty", ((Result) results[1]).isEmpty());

    table.close();
  }

  /**
   * Requirement 8.1
   */
  @Test
  public void testBatchIncrement() throws IOException, InterruptedException {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey1 = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    Random random = new Random();
    long value1 = random.nextLong();
    byte[] rowKey2 = dataHelper.randomData("testrow-");
    byte[] qual2 = dataHelper.randomData("qual-");
    long value2 = random.nextLong();

    // Put
    Put put1 = new Put(rowKey1).addColumn(COLUMN_FAMILY, qual1, Bytes.toBytes(value1));
    Put put2 = new Put(rowKey2).addColumn(COLUMN_FAMILY, qual2, Bytes.toBytes(value2));
    List<Row> batch = new ArrayList<Row>(2);
    batch.add(put1);
    batch.add(put2);
    table.batch(batch, null);

    // Increment
    Increment increment1 = new Increment(rowKey1).addColumn(COLUMN_FAMILY, qual1, 1L);
    Increment increment2 = new Increment(rowKey2).addColumn(COLUMN_FAMILY, qual2, 1L);
    batch.clear();
    batch.add(increment1);
    batch.add(increment2);
    Object[] results = new Object[2];
    table.batch(batch, results);
    Assert.assertEquals("Should be value1 + 1", value1 + 1, Bytes.toLong(
      CellUtil.cloneValue(((Result) results[0]).getColumnLatestCell(COLUMN_FAMILY, qual1))));
    Assert.assertEquals("Should be value2 + 1", value2 + 1, Bytes.toLong(
      CellUtil.cloneValue(((Result) results[1]).getColumnLatestCell(COLUMN_FAMILY, qual2))));

    table.close();
  }

  /**
   * Requirement 8.1
   */
  @Test
  public void testBatchAppend() throws IOException, InterruptedException {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey1 = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    byte[] value1_1 = dataHelper.randomData("value-");
    byte[] value1_2 = dataHelper.randomData("value-");
    byte[] rowKey2 = dataHelper.randomData("testrow-");
    byte[] qual2 = dataHelper.randomData("qual-");
    byte[] value2_1 = dataHelper.randomData("value-");
    byte[] value2_2 = dataHelper.randomData("value-");

    // Put
    Put put1 = new Put(rowKey1).addColumn(COLUMN_FAMILY, qual1, value1_1);
    Put put2 = new Put(rowKey2).addColumn(COLUMN_FAMILY, qual2, value2_1);
    List<Row> batch = new ArrayList<Row>(2);
    batch.add(put1);
    batch.add(put2);
    table.batch(batch, null);

    // Increment
    Append append1 = new Append(rowKey1).add(COLUMN_FAMILY, qual1, value1_2);
    Append append2 = new Append(rowKey2).add(COLUMN_FAMILY, qual2, value2_2);
    batch.clear();
    batch.add(append1);
    batch.add(append2);
    Object[] results = new Object[2];
    table.batch(batch, results);
    Assert.assertArrayEquals("Should be value1_1 + value1_2", ArrayUtils.addAll(value1_1, value1_2),
      CellUtil.cloneValue(((Result) results[0]).getColumnLatestCell(COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals("Should be value1_1 + value1_2", ArrayUtils.addAll(value2_1, value2_2),
      CellUtil.cloneValue(((Result) results[1]).getColumnLatestCell(COLUMN_FAMILY, qual2)));

    table.close();
  }

  /**
   * Requirement 8.2 - Batch throws an exception if any of the calls failed.  Any successful
   * mutations that did not throw an exception will succeed.
   *
   * Requirement 8.7 - Server errors should populate those return elements in a batch() call with
   * corresponding Throwables.
   */
  @Test
  @Category(KnownGap.class)
  public void testBatchWithException() throws IOException, InterruptedException {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    byte[][] rowKeys = dataHelper.randomData("testrow-", 5);
    byte[][] quals = dataHelper.randomData("qual-", 5);
    byte[][] values = dataHelper.randomData("value-", 5);

    Put put0 = new Put(rowKeys[0]).addColumn(COLUMN_FAMILY, quals[0], values[0]);
    Put put1 = new Put(rowKeys[1]).addColumn(Bytes.toBytes("NO SUCH FAMILY"), quals[1], values[1]);
    Put put2 = new Put(rowKeys[2]).addColumn(COLUMN_FAMILY, quals[2], values[2]);
    Put put3 = new Put(rowKeys[3]).addColumn(Bytes.toBytes("NO SUCH FAMILY"), quals[3], values[3]);
    Put put4 = new Put(rowKeys[4]).addColumn(COLUMN_FAMILY, quals[4], values[4]);
    List<Row> batch = new ArrayList<Row>(5);
    Object[] results = new Object[5];
    batch.add(put0);
    batch.add(put1);  // This one is bad
    batch.add(put2);
    batch.add(put3);  // So's this one
    batch.add(put4);
    RetriesExhaustedWithDetailsException exception = null;
    try {
      table.batch(batch, results);
    } catch (RetriesExhaustedWithDetailsException e) {
      exception = e;
    }
    Assert.assertNotNull("Exception should have been thrown", exception);
    Assert.assertEquals("There should have been two exceptions", 2, exception.getNumExceptions());
    Assert.assertTrue("Cause should be NoSuchColumnFamilyException",
      exception.getCause(0) instanceof NoSuchColumnFamilyException);
    Assert.assertArrayEquals("Row key should be #1", rowKeys[1], exception.getRow(0).getRow());
    Assert.assertTrue("Cause should be NoSuchColumnFamilyException",
      exception.getCause(1) instanceof NoSuchColumnFamilyException);
    Assert.assertArrayEquals("Row key should be #3", rowKeys[3], exception.getRow(1).getRow());
    Assert.assertTrue("#0 should be a Result", results[0] instanceof Result);
    Assert.assertTrue("#1 should be the exception cause",
      results[1] instanceof NoSuchColumnFamilyException);
    Assert.assertTrue("#2 should be a Result", results[2] instanceof Result);
    Assert.assertTrue("#3 should be the exception cause",
      results[3] instanceof NoSuchColumnFamilyException);
    Assert.assertTrue("#4 should be a Result", results[4] instanceof Result);

    // Check values.  The good puts should have worked.
    List<Get> gets = new ArrayList<Get>(5);
    for (int i = 0; i < 5; ++i) {
      gets.add(new Get(rowKeys[i]));
    }
    Result[] getResults = table.get(gets);
    Assert.assertArrayEquals("Row #0 should have value #0", values[0],
      CellUtil.cloneValue(getResults[0].getColumnLatestCell(COLUMN_FAMILY, quals[0])));
    Assert.assertTrue("Row #1 should be empty", getResults[1].isEmpty());
    Assert.assertArrayEquals("Row #2 should have value #2", values[2],
      CellUtil.cloneValue(getResults[2].getColumnLatestCell(COLUMN_FAMILY, quals[2])));
    Assert.assertTrue("Row #3 should be empty", getResults[3].isEmpty());
    Assert.assertArrayEquals("Row #4 should have value #4", values[4],
      CellUtil.cloneValue(getResults[4].getColumnLatestCell(COLUMN_FAMILY, quals[4])));

    table.close();
  }

  /**
   * Requirement 8.3 - MutateRow performs a combination of Put and Delete operations for a single
   * row.
   */
  @Test
  @Category(KnownGap.class)
  public void testRowMutations() throws IOException {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[][] quals = dataHelper.randomData("qual-", 3);
    byte[][] values = dataHelper.randomData("value-", 3);

    // Put a couple of values
    Put put0 = new Put(rowKey).addColumn(COLUMN_FAMILY, quals[0], values[0]);
    Put put1 = new Put(rowKey).addColumn(COLUMN_FAMILY, quals[1], values[1]);
    RowMutations rm = new RowMutations(rowKey);
    rm.add(put0);
    rm.add(put1);
    table.mutateRow(rm);

    // Check
    Result result = table.get(new Get(rowKey));
    Assert.assertEquals("Should have two values", 2, result.size());
    Assert.assertArrayEquals("Value #0 should exist", values[0],
      CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[0])));
    Assert.assertArrayEquals("Value #1 should exist", values[1],
      CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[1])));

    // Now delete the value #1 and insert value #3
    Delete delete = new Delete(rowKey).addColumn(COLUMN_FAMILY, quals[1]);
    Put put2 = new Put(rowKey).addColumn(COLUMN_FAMILY, quals[2], values[2]);
    rm = new RowMutations(rowKey);
    rm.add(delete);
    rm.add(put2);
    table.mutateRow(rm);

    // Check
    result = table.get(new Get(rowKey));
    Assert.assertEquals("Should have two values", 2, result.size());
    Assert.assertArrayEquals("Value #0 should exist", values[0],
      CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[0])));
    Assert.assertArrayEquals("Value #2 should exist", values[2],
      CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[2])));

    table.close();
  }
  
  @Test
  public void testBatchGets() throws Exception {
    // Initialize data
    Table table = getConnection().getTable(TABLE_NAME);
    byte[] rowKey1 = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] rowKey2 = dataHelper.randomData("testrow-");
    byte[] qual2 = dataHelper.randomData("qual-");
    byte[] value2 = dataHelper.randomData("value-");
    byte[] emptyRowKey = dataHelper.randomData("testrow-");

    Put put1 = new Put(rowKey1).addColumn(COLUMN_FAMILY, qual1, value1);
    Put put2 = new Put(rowKey2).addColumn(COLUMN_FAMILY, qual2, value2);
    List<Row> batch = new ArrayList<Row>(2);
    batch.add(put1);
    batch.add(put2);
    Object[] results = new Object[batch.size()];
    table.batch(batch, results);
    Assert.assertTrue("Should be a Result", results[0] instanceof Result);
    Assert.assertTrue("Should be a Result", results[1] instanceof Result);
    Assert.assertTrue("Should be empty", ((Result) results[0]).isEmpty());
    Assert.assertTrue("Should be empty", ((Result) results[1]).isEmpty());
    Assert.assertEquals("Batch should not have been cleared", 2, batch.size());

    // Check values
    Get get1 = new Get(rowKey1);
    Get get2 = new Get(rowKey2);
    Get get3 = new Get(emptyRowKey);
    batch.clear();
    batch.add(get1);
    batch.add(get2);
    batch.add(get3);
    results = new Object[batch.size()];
    table.batch(batch, results);
    Assert.assertTrue("Should be Result", results[0] instanceof Result);
    Assert.assertTrue("Should be Result", results[1] instanceof Result);
    Assert.assertTrue("Should be Result", results[2] instanceof Result);
    Assert.assertEquals("Should be one value", 1, ((Result) results[0]).size());
    Assert.assertEquals("Should be one value", 1, ((Result) results[1]).size());
    Assert.assertEquals("Should be empty", 0, ((Result) results[2]).size());
    Assert.assertArrayEquals("Should be value1", value1,
      CellUtil.cloneValue(((Result) results[0]).getColumnLatestCell(COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals("Should be value2", value2,
      CellUtil.cloneValue(((Result) results[1]).getColumnLatestCell(COLUMN_FAMILY, qual2)));

    table.close();
  }

  @Test
  public void testBatchDoesntHang() throws Exception {
    Connection closedConnection = ConnectionFactory.createConnection(IntegrationTests.getConfiguration());
    Table table = closedConnection.getTable(TABLE_NAME);
    closedConnection.close();
    try {
      table.batch(Arrays.asList(new Get(Bytes.toBytes("key"))), new Object[1]);
      Assert.fail("Expected an exception");
    } catch(RetriesExhaustedWithDetailsException e) {
      Assert.assertEquals(1, e.getCauses().size());
      Assert.assertTrue(e.getCause(0).getMessage().toLowerCase().contains("closed"));
    }
  }
}
