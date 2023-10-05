/*
 * Copyright 2015 Google LLC
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
import static com.google.common.truth.Truth.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class TestScan extends AbstractTest {

  public static byte[] rowFollowing(byte[] rowKey) {
    // result will be padded with trailing 0s:
    return Arrays.copyOf(rowKey, rowKey.length + 1);
  }

  public static byte[] rowFollowingSameLength(byte[] rowKey) {
    byte[] result = Arrays.copyOf(rowKey, rowKey.length);
    for (int index = rowKey.length - 1; index >= 0; index--) {
      result[index] = (byte) (rowKey[index] + 1);

      if (result[index] != 0) {
        break;
      }
    }

    return result;
  }

  @Test
  public void testRowFollowing() {
    Assert.assertArrayEquals(
        new byte[] {0x00, 0x01, 0x02}, rowFollowingSameLength(new byte[] {0x00, 0x01, 0x01}));

    Assert.assertArrayEquals(
        new byte[] {0x00, 0x02, 0x00},
        rowFollowingSameLength(new byte[] {0x00, 0x01, (byte) 0xFF}));

    Assert.assertArrayEquals(
        new byte[] {0x00, 0x01, 0x02, 0x00}, rowFollowing(new byte[] {0x00, 0x01, 0x02}));
  }

  @Test
  public void testNextAfterClose() throws IOException {
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");

    table.put(new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value));

    Scan scan = new Scan().withStartRow(rowKey).withStopRow(rowKey, true);
    ResultScanner resultScanner = table.getScanner(scan);

    Assert.assertNotNull(resultScanner.next());

    // The scanner should close here.
    Assert.assertNull(resultScanner.next());

    // This shouldn't throw an exception
    Assert.assertNull(resultScanner.next());

    resultScanner.close();

    // This shouldn't throw an exception
    Assert.assertNull(resultScanner.next());
  }

  @Test
  public void testGetScannerBeforeTimestamp() throws IOException {
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("qual-");
    byte[][] values = dataHelper.randomData("value-", 2);

    long ts1 = 100000l;
    long ts2 = 200000l;

    table.put(
        new Put(rowKey)
            .addColumn(COLUMN_FAMILY, qual, ts1, values[0])
            .addColumn(COLUMN_FAMILY, qual, ts2, values[1]));

    Scan scan1 =
        new Scan().withStartRow(rowKey).withStopRow(rowFollowing(rowKey)).setTimeRange(0, ts1);
    try (ResultScanner resultScanner = table.getScanner(scan1)) {
      Assert.assertNull(resultScanner.next());
    }

    Scan scan2 =
        new Scan().withStartRow(rowKey).withStopRow(rowFollowing(rowKey)).setTimeRange(0, ts2 + 1);
    try (ResultScanner resultScanner = table.getScanner(scan2)) {
      Result result = resultScanner.next();
      Assert.assertNotNull(result);
      Assert.assertArrayEquals(
          values[1], CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual)));
    }

    Scan scan3 =
        new Scan().withStartRow(rowKey).withStopRow(rowFollowing(rowKey)).setTimeRange(0, ts1 + 1);
    try (ResultScanner resultScanner = table.getScanner(scan3)) {
      Result result = resultScanner.next();
      Assert.assertNotNull(result);
      Assert.assertArrayEquals(
          values[0], CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual)));
    }
  }

  @Test
  public void testGetScannerNoQualifiers() throws IOException {
    // Initialize variables
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("testrow-");
    int numValues = 3;
    byte[][] quals = dataHelper.randomData("qual-", numValues);
    byte[][] values = dataHelper.randomData("value-", numValues);

    // Insert some columns
    Put put = new Put(rowKey);
    for (int i = 0; i < numValues; ++i) {
      put.addColumn(COLUMN_FAMILY, quals[i], values[i]);
    }
    table.put(put);

    // Get without a qualifer, and confirm all results are returned.
    Scan scan =
        new Scan().withStartRow(rowKey).withStopRow(rowFollowing(rowKey)).addFamily(COLUMN_FAMILY);

    ResultScanner resultScanner = table.getScanner(scan);
    Result result = resultScanner.next();
    resultScanner.close();

    Assert.assertEquals(numValues, result.size());
    for (int i = 0; i < numValues; ++i) {
      Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, quals[i]));
      Assert.assertArrayEquals(
          values[i], CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[i])));
    }

    // Cleanup
    Delete delete = new Delete(rowKey);
    table.delete(delete);
    table.close();
  }

  @Test
  public void testManyResultsInScanner_lessThanPageSize() throws IOException {
    testManyResultsInScanner(95);
  }

  @Test
  public void testManyResultsInScanner_equalToPageSize() throws IOException {
    testManyResultsInScanner(100);
  }

  @Test
  public void testManyResultsInScanner_greaterThanPageSize() throws IOException {
    testManyResultsInScanner(105);
  }

  private void testManyResultsInScanner(int rowsToWrite) throws IOException {
    String prefix = "scan_row_";

    // Initialize variables
    Table table = getDefaultTable();

    byte[][] rowKeys = new byte[rowsToWrite][];
    rowKeys[0] = dataHelper.randomData(prefix);
    for (int i = 1; i < rowsToWrite; i++) {
      rowKeys[i] = rowFollowingSameLength(rowKeys[i - 1]);
    }

    int numValuesPerRow = 3;
    byte[][] quals = dataHelper.randomData("qual-", numValuesPerRow);
    byte[][] values = dataHelper.randomData("value-", numValuesPerRow);

    ArrayList<Put> puts = new ArrayList<>(rowsToWrite);

    // Insert some columns
    for (int rowIndex = 0; rowIndex < rowsToWrite; rowIndex++) {
      Put put = new Put(rowKeys[rowIndex]);
      for (int qualifierIndex = 0; qualifierIndex < numValuesPerRow; qualifierIndex++) {
        put.addColumn(COLUMN_FAMILY, quals[qualifierIndex], values[qualifierIndex]);
      }
      puts.add(put);
    }
    table.put(puts);

    Scan scan = new Scan();
    scan.withStartRow(rowKeys[0])
        .withStopRow(rowFollowingSameLength(rowKeys[rowsToWrite - 1]))
        .addFamily(COLUMN_FAMILY);

    try (ResultScanner resultScanner = table.getScanner(scan)) {
      for (int rowIndex = 0; rowIndex < rowsToWrite; rowIndex++) {
        Result result = resultScanner.next();

        Assert.assertNotNull(String.format("Didn't expect row %s to be null", rowIndex), result);

        Assert.assertEquals(numValuesPerRow, result.size());
        for (int i = 0; i < numValuesPerRow; ++i) {
          Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, quals[i]));
          Assert.assertArrayEquals(
              values[i], CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[i])));
        }
      }

      // Verify that there are no more rows:
      Assert.assertNull(
          "There should not be any more results in the scanner.", resultScanner.next());
    }

    // Cleanup
    ArrayList<Delete> deletes = new ArrayList<>(rowsToWrite);
    for (int rowIndex = 0; rowIndex < rowsToWrite; rowIndex++) {
      deletes.add(new Delete(rowKeys[rowIndex]));
    }
    table.delete(deletes);
    table.close();
  }

  @Test
  /**
   * Cloud Bigtable had problems with operations that happened during a scan. Create 100 rows, scan
   * for them, and delete each resulting row. Make sure that this problem doesn't reoccur.
   */
  public void testScanDelete() throws IOException {
    String prefix = "scan_delete_";
    int rowsToWrite = 100;

    // Initialize variables
    Table table = getDefaultTable();

    byte[][] rowKeys = new byte[rowsToWrite][];
    rowKeys[0] = dataHelper.randomData(prefix);
    for (int i = 1; i < rowsToWrite; i++) {
      rowKeys[i] = rowFollowingSameLength(rowKeys[i - 1]);
    }

    int numValuesPerRow = 3;
    byte[][] quals = dataHelper.randomData("qual-", numValuesPerRow);
    byte[][] values = dataHelper.randomData("value-", numValuesPerRow);

    ArrayList<Put> puts = new ArrayList<>(rowsToWrite);

    // Insert some columns
    for (int rowIndex = 0; rowIndex < rowsToWrite; rowIndex++) {
      Put put = new Put(rowKeys[rowIndex]);
      for (int qualifierIndex = 0; qualifierIndex < numValuesPerRow; qualifierIndex++) {
        put.addColumn(COLUMN_FAMILY, quals[qualifierIndex], values[qualifierIndex]);
      }
      puts.add(put);
    }
    table.put(puts);

    Scan scan = new Scan();
    scan.withStartRow(rowKeys[0])
        .withStopRow(rowFollowingSameLength(rowKeys[rowsToWrite - 1]))
        .addFamily(COLUMN_FAMILY);
    int deleteCount = 0;
    try (ResultScanner resultScanner = table.getScanner(scan)) {
      for (Result result : resultScanner) {
        table.delete(new Delete(result.getRow()));
        deleteCount++;
      }
    }
    Assert.assertEquals(rowsToWrite, deleteCount);
    try (ResultScanner resultScanner = table.getScanner(scan)) {
      Assert.assertNull(resultScanner.next());
    }
  }

  @Test
  public void testStartEndEquals() throws IOException {
    // Initialize variables
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("start_end_equals");
    byte[] qualifier = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");

    table.put(new Put(rowKey).addColumn(COLUMN_FAMILY, qualifier, value));
    Scan scan = new Scan().withStartRow(rowKey).withStopRow(rowKey, true);
    try (ResultScanner resultScanner = table.getScanner(scan)) {
      Result result = resultScanner.next();
      Assert.assertNotNull(result);
      Cell cell = result.getColumnCells(COLUMN_FAMILY, qualifier).get(0);
      Assert.assertArrayEquals(value, CellUtil.cloneValue(cell));
    }
  }

  @Test
  @Category(KnownEmulatorGap.class)
  public void testBasicReverseScan() throws IOException {
    String prefix = "reverse_basic";
    int rowsToWrite = 10;

    // Initialize variables
    Table table = getDefaultTable();

    byte[][] rowKeys = new byte[rowsToWrite][];
    rowKeys[0] = dataHelper.randomData(prefix);
    for (int i = 1; i < rowsToWrite; i++) {
      rowKeys[i] = rowFollowingSameLength(rowKeys[i - 1]);
    }

    byte[] qualifier = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");

    ArrayList<Put> puts = new ArrayList<>(rowsToWrite);

    // Insert some columns
    for (int rowIndex = 0; rowIndex < rowsToWrite; rowIndex++) {
      Put put = new Put(rowKeys[rowIndex]).addColumn(COLUMN_FAMILY, qualifier, value);
      puts.add(put);
    }
    table.put(puts);

    Scan scan = new Scan().setReversed(true).withStartRow(rowKeys[6]).withStopRow(rowKeys[2]);

    List<String> actualRowKeys =
        StreamSupport.stream(table.getScanner(scan).spliterator(), false)
            .map(Result::getRow)
            .map(String::new)
            .collect(Collectors.toList());

    List<String> expectedRowKeys =
        ImmutableList.of(
            new String(rowKeys[6]),
            new String(rowKeys[5]),
            new String(rowKeys[4]),
            new String(rowKeys[3]));

    assertThat(actualRowKeys).containsExactlyElementsIn(expectedRowKeys).inOrder();
  }

  @Test
  @Category(KnownEmulatorGap.class)
  public void testReverseScanWithFilter() throws IOException {
    String prefix = "reverse_filter";
    int rowsToWrite = 10;

    // Initialize variables
    Table table = getDefaultTable();

    byte[][] rowKeys = new byte[rowsToWrite][];
    rowKeys[0] = dataHelper.randomData(prefix);
    for (int i = 1; i < rowsToWrite; i++) {
      rowKeys[i] = rowFollowingSameLength(rowKeys[i - 1]);
    }

    byte[] qualifier = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");

    ArrayList<Put> puts = new ArrayList<>(rowsToWrite);

    // Insert some columns
    for (int rowIndex = 0; rowIndex < rowsToWrite; rowIndex++) {
      Put put = new Put(rowKeys[rowIndex]).addColumn(COLUMN_FAMILY, qualifier, value);
      puts.add(put);
    }
    table.put(puts);

    Scan scan =
        new Scan()
            .setReversed(true)
            .setFilter(
                new MultiRowRangeFilter(
                    Lists.newArrayList(
                        new RowRange(rowKeys[3], false, rowKeys[4], true),
                        new RowRange(rowKeys[6], true, rowKeys[8], false))));

    List<String> actualRowKeys =
        StreamSupport.stream(table.getScanner(scan).spliterator(), false)
            .map(Result::getRow)
            .map(String::new)
            .collect(Collectors.toList());

    List<String> expectedRowKeys =
        ImmutableList.of(new String(rowKeys[7]), new String(rowKeys[6]), new String(rowKeys[4]));

    assertThat(actualRowKeys).containsExactlyElementsIn(expectedRowKeys).inOrder();
  }

  @Test
  @Category(KnownEmulatorGap.class)
  public void testReverseScanWithFilterAndRange() throws IOException {
    String prefix = "reverse_filter";
    int rowsToWrite = 10;

    // Initialize variables
    Table table = getDefaultTable();

    byte[][] rowKeys = new byte[rowsToWrite][];
    rowKeys[0] = dataHelper.randomData(prefix);
    for (int i = 1; i < rowsToWrite; i++) {
      rowKeys[i] = rowFollowingSameLength(rowKeys[i - 1]);
    }

    byte[] qualifier = dataHelper.randomData("qual-");
    byte[] value = dataHelper.randomData("value-");

    ArrayList<Put> puts = new ArrayList<>(rowsToWrite);

    // Insert some columns
    for (int rowIndex = 0; rowIndex < rowsToWrite; rowIndex++) {
      Put put = new Put(rowKeys[rowIndex]).addColumn(COLUMN_FAMILY, qualifier, value);
      puts.add(put);
    }
    table.put(puts);

    Scan scan =
        new Scan()
            .setReversed(true)
            .withStartRow(rowKeys[6])
            .withStopRow(rowKeys[1])
            .setFilter(
                new MultiRowRangeFilter(
                    Lists.newArrayList(
                        new RowRange(rowKeys[3], true, rowKeys[4], true),
                        new RowRange(rowKeys[6], true, rowKeys[8], false))));

    List<String> actualRowKeys =
        StreamSupport.stream(table.getScanner(scan).spliterator(), false)
            .map(Result::getRow)
            .map(String::new)
            .collect(Collectors.toList());

    List<String> expectedRowKeys =
        ImmutableList.of(new String(rowKeys[6]), new String(rowKeys[4]), new String(rowKeys[3]));

    assertThat(actualRowKeys).containsExactlyElementsIn(expectedRowKeys).inOrder();
  }

  @Test
  public void testColFamilyTimeRange() throws IOException {
    TableName tableName = sharedTestEnv.newTestTableName();
    sharedTestEnv.createTable(tableName);
    Table table = getConnection().getTable(tableName);
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    byte[] qual2 = dataHelper.randomData("qual-");
    byte[][] values = dataHelper.randomData("value-", 3);

    long ts1 = 100000L;
    long ts2 = 200000L;

    table.put(
        new Put(rowKey)
            .addColumn(COLUMN_FAMILY, qual1, ts1, values[1])
            .addColumn(COLUMN_FAMILY, qual2, ts2, values[2]));

    Scan scan1 = new Scan().setColumnFamilyTimeRange(COLUMN_FAMILY, 0, ts1);
    try (ResultScanner resultScanner = table.getScanner(scan1)) {
      Assert.assertNull(resultScanner.next());
    }

    Scan scan2 = new Scan().setColumnFamilyTimeRange(COLUMN_FAMILY, 0, ts1 + 1);
    try (ResultScanner resultScanner = table.getScanner(scan2)) {
      Result result = resultScanner.next();
      Assert.assertNotNull(result);
      Assert.assertEquals(1, result.rawCells().length);
      Assert.assertEquals(ts1, result.rawCells()[0].getTimestamp());
    }

    Scan scan3 = new Scan().setColumnFamilyTimeRange(COLUMN_FAMILY, ts1, ts2 + 1);
    try (ResultScanner resultScanner = table.getScanner(scan3)) {
      Result result = resultScanner.next();
      Assert.assertNotNull(result);
      Assert.assertEquals(2, result.rawCells().length);
      Assert.assertEquals(ts1, result.getColumnLatestCell(COLUMN_FAMILY, qual1).getTimestamp());
      Assert.assertEquals(ts2, result.getColumnLatestCell(COLUMN_FAMILY, qual2).getTimestamp());
    }
  }

  @Test
  public void testColFamilyTimeRangeWithMultiRows() throws IOException {
    TableName tableName = sharedTestEnv.newTestTableName();
    sharedTestEnv.createTable(tableName);
    Table table = getConnection().getTable(tableName);
    byte[][] rowKeys = dataHelper.randomData("testrow-", 3);
    byte[] qual = dataHelper.randomData("qual-");
    byte[][] values = dataHelper.randomData("value-", 3);

    long ts1 = 600000L;
    long ts2 = 800000L;
    long ts3 = 900000L;

    List<Put> puts =
        Arrays.asList(
            new Put(rowKeys[0])
                .addColumn(COLUMN_FAMILY, qual, ts1, values[1])
                .addColumn(COLUMN_FAMILY2, qual, ts2, values[1]),
            new Put(rowKeys[1])
                .addColumn(COLUMN_FAMILY, qual, ts2, values[1])
                .addColumn(COLUMN_FAMILY2, qual, ts3, values[1]),
            new Put(rowKeys[2]).addColumn(COLUMN_FAMILY, qual, values[2]));
    table.put(puts);

    Scan scan1 =
        new Scan()
            .setColumnFamilyTimeRange(COLUMN_FAMILY, ts1 + 1, ts3)
            .setColumnFamilyTimeRange(COLUMN_FAMILY2, ts2, ts3);

    try (ResultScanner resultScanner = table.getScanner(scan1)) {
      Result result = resultScanner.next();
      Assert.assertNotNull(result);
      Cell[] cells = result.rawCells();
      Assert.assertEquals(1, cells.length);
      Assert.assertEquals(ts2, result.rawCells()[0].getTimestamp());

      result = resultScanner.next();
      cells = result.rawCells();
      Assert.assertEquals(1, cells.length);
      Assert.assertEquals(ts2, result.rawCells()[0].getTimestamp());

      Assert.assertNull(
          "There should not be any more results in the scanner.", resultScanner.next());
    }
  }
}
