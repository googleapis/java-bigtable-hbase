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

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;

public class TestBatch extends AbstractTestBatch {
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
 

  /**
   * Requirement 8.1
   */
  @Test
  public void testBatchAppend() throws IOException, InterruptedException {
    // Initialize data
    Table table = getDefaultTable();
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
}
