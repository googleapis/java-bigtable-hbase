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
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestAppend extends AbstractTest {
  /**
   * Requirement 5.1 - Append values to one or more columns within a single row.
   */
  @Test
  public void testAppend() throws Exception {
    // Initialize
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value1-");
    byte[] value1And2 = ArrayUtils.addAll(value1, value2);

    // Put then append
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifier, value1);
    table.put(put);
    Append append = new Append(rowKey).add(SharedTestEnvRule.COLUMN_FAMILY, qualifier, value2);
    Result result = table.append(append);
    Cell cell = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect concatenated byte array", value1And2,
      CellUtil.cloneValue(cell));

    // Test result
    Get get = new Get(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    get.setMaxVersions(5);
    result = table.get(get);
    List<Cell> cells = result.getColumnCells(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect concatenated byte array", value1And2,
      CellUtil.cloneValue(cells.get(0)));
    if (result.size() == 2) {
      // TODO: This isn't always true with CBT.  Why is that?
      Assert.assertEquals("There should be two versions now", 2, result.size());
      Assert.assertArrayEquals("Expect original value still there", value1,
        CellUtil.cloneValue(cells.get(1)));
    }
  }

  @Test
  public void testAppendToEmptyCell() throws Exception {
    // Initialize
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier = dataHelper.randomData("qualifier-");
    byte[] value = dataHelper.randomData("value1-");

    // Put then append
    Append append = new Append(rowKey).add(SharedTestEnvRule.COLUMN_FAMILY, qualifier, value);
    table.append(append);

    // Test result
    Get get = new Get(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("There should be one version now", 1, result.size());
    Cell cell = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect append value is entire value", value, CellUtil.cloneValue(cell));
  }

  @Test
  public void testAppendNoResult() throws Exception {
    // Initialize
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Put then append
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value1);
    table.put(put);
    Append append = new Append(rowKey).add(SharedTestEnvRule.COLUMN_FAMILY, qual, value2);
    append.setReturnResults(false);
    Result result = table.append(append);
    Assert.assertNull("Should not return result", result);
  }

  @Test
  public void testAppendToMultipleColumns() throws Exception {
    // Initialize
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier1 = dataHelper.randomData("qualifier1-");
    byte[] qualifier2 = dataHelper.randomData("qualifier2-");
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value2-");

    // Put then append
    Append append =
        new Append(rowKey)
            .add(SharedTestEnvRule.COLUMN_FAMILY, qualifier1, value1)
            .add(SharedTestEnvRule.COLUMN_FAMILY, qualifier2, value2);
    table.append(append);

    // Test result
    Get get = new Get(rowKey).addFamily(SharedTestEnvRule.COLUMN_FAMILY);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("There should be two cells", 2, result.size());
    Cell cell1 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier1);
    Assert.assertEquals(
        Bytes.toString(value1),
        Bytes.toString(CellUtil.cloneValue(cell1)));
    Cell cell2 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier2);
    Assert.assertEquals(
        Bytes.toString(value2),
        Bytes.toString(CellUtil.cloneValue(cell2)));
  }


  @Test
  public void testAppendToMultipleFamilies() throws Exception {
    // Initialize
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier1 = dataHelper.randomData("qualifier1-");
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value2-");

    // Put then append
    Append append =
        new Append(rowKey)
            .add(SharedTestEnvRule.COLUMN_FAMILY, qualifier1, value1)
            .add(SharedTestEnvRule.COLUMN_FAMILY2, qualifier1, value2);
    table.append(append);

    // Test result
    Get get = new Get(rowKey)
        .addFamily(SharedTestEnvRule.COLUMN_FAMILY)
        .addFamily(SharedTestEnvRule.COLUMN_FAMILY2);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("There should be two cells", 2, result.size());
    Cell cell1 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier1);
    Assert.assertNotNull(cell1);
    Assert.assertEquals(
        Bytes.toString(value1),
        Bytes.toString(CellUtil.cloneValue(cell1)));
    Cell cell2 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY2, qualifier1);
    Assert.assertNotNull(cell2);
    Assert.assertEquals(
        Bytes.toString(value2),
        Bytes.toString(CellUtil.cloneValue(cell2)));
  }

  @Test
  public void testAppendMultiFamilyEmptyQualifier() throws Exception {
    // Initialize
    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier1 = new byte[0];
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value2-");

    // Put then append
    Append append =
        new Append(rowKey)
            .add(SharedTestEnvRule.COLUMN_FAMILY, qualifier1, value1)
            .add(SharedTestEnvRule.COLUMN_FAMILY2, qualifier1, value2);
    table.append(append);

    // Test result
    Get get = new Get(rowKey).addFamily(SharedTestEnvRule.COLUMN_FAMILY).addFamily(SharedTestEnvRule.COLUMN_FAMILY2);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("There should be two cells", 2, result.size());
    Cell cell1 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier1);
    Assert.assertEquals(
        Bytes.toString(value1),
        Bytes.toString(CellUtil.cloneValue(cell1)));
    Cell cell2 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY2, qualifier1);
    Assert.assertEquals(
        Bytes.toString(value2),
        Bytes.toString(CellUtil.cloneValue(cell2)));
  }
}
