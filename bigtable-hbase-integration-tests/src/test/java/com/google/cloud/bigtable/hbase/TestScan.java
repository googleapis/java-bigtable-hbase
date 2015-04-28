/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
        new byte[]{0x00, 0x01, 0x02}, rowFollowingSameLength(new byte[]{0x00, 0x01, 0x01}));

    Assert.assertArrayEquals(
        new byte[]{0x00, 0x02, 0x00}, rowFollowingSameLength(new byte[]{0x00, 0x01, (byte)0xFF}));

    Assert.assertArrayEquals(
        new byte[]{0x00, 0x01, 0x02, 0x00}, rowFollowing(new byte[]{0x00, 0x01, 0x02}));
  }

  @Test
  public void testGetScannerNoQualifiers() throws IOException {
    // Initialize variables
    Table table = getConnection().getTable(TABLE_NAME);
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
    Scan scan = new Scan();
    scan.setStartRow(rowKey)
        .setStopRow(rowFollowing(rowKey))
        .addFamily(COLUMN_FAMILY);

    ResultScanner resultScanner = table.getScanner(scan);
    Result result = resultScanner.next();
    resultScanner.close();

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

  @Test
  @Category(KnownGap.class)
  public void test100ResultsInScanner() throws IOException {
    int rowsToWrite = 100;

    // Initialize variables
    Table table = getConnection().getTable(TABLE_NAME);

    byte[][] rowKeys = new byte[rowsToWrite][];
    rowKeys[0] = dataHelper.randomData("scan_row_");
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
    scan.setStartRow(rowKeys[0])
        .setStopRow(rowFollowing(rowKeys[rowsToWrite - 1]))
        .addFamily(COLUMN_FAMILY);

    ResultScanner resultScanner = table.getScanner(scan);

    for (int rowIndex = 0; rowIndex < rowsToWrite; rowIndex++) {
      Result result = resultScanner.next();

      Assert.assertNotNull(String.format("Didn't expect row %s to be null", rowIndex), result);

      Assert.assertEquals(numValuesPerRow, result.size());
      for (int i = 0; i < numValuesPerRow; ++i) {
        Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, quals[i]));
        Assert.assertArrayEquals(values[i],
            CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[i])));
      }
    }

    // Verify that there are no more rows:
    Assert.assertNull("There should not be any more results in the scanner.", resultScanner.next());

    resultScanner.close();

    // Cleanup
    ArrayList<Delete> deletes = new ArrayList<>(rowsToWrite);
    for (int rowIndex = 0; rowIndex < rowsToWrite; rowIndex++) {
      deletes.add(new Delete(rowKeys[rowIndex]));
    }
    table.delete(deletes);
    table.close();
  }
}
