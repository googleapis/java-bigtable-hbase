/*
 * Copyright (c) 2014 Google Inc.
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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.IntegrationTests.*;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
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
    Table table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value1-");
    byte[] value1And2 = ArrayUtils.addAll(value1, value2);

    // Put then append
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qualifier, value1);
    table.put(put);
    Append append = new Append(rowKey).add(COLUMN_FAMILY, qualifier, value2);
    Result result = table.append(append);
    Cell cell = result.getColumnLatestCell(COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect concatenated byte array", value1And2,
      CellUtil.cloneValue(cell));

    // Test result
    Get get = new Get(rowKey).addColumn(COLUMN_FAMILY, qualifier);
    get.setMaxVersions(5);
    result = table.get(get);
    Assert.assertEquals("There should be two versions now", 2, result.size());
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect concatenated byte array", value1And2,
      CellUtil.cloneValue(cells.get(0)));
    Assert.assertArrayEquals("Expect original value still there", value1,
      CellUtil.cloneValue(cells.get(1)));
  }

  @Test
  public void testAppendToEmptyCell() throws Exception {
    // Initialize
    Table table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier = dataHelper.randomData("qualifier-");
    byte[] value = dataHelper.randomData("value1-");

    // Put then append
    Append append = new Append(rowKey).add(COLUMN_FAMILY, qualifier, value);
    table.append(append);

    // Test result
    Get get = new Get(rowKey).addColumn(COLUMN_FAMILY, qualifier);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("There should be one version now", 1, result.size());
    Cell cell = result.getColumnLatestCell(COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect append value is entire value", value, CellUtil.cloneValue(cell));
  }

  @Test
  public void testAppendNoResult() throws Exception {
    // Initialize
    Table table = connection.getTable(TABLE_NAME);
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Put then append
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value1);
    table.put(put);
    Append append = new Append(rowKey).add(COLUMN_FAMILY, qual, value2);
    append.setReturnResults(false);
    Result result = table.append(append);
    Assert.assertNull("Should not return result", result);
  }
}
