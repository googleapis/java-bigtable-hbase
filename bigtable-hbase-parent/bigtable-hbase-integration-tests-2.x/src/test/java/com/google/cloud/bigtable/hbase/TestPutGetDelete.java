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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A simple put-get-delete test.
 */
public class TestPutGetDelete extends AbstractTest {
  @Test
  public void testPutGetDelete() throws IOException {
    // Initialize
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    byte[] testValue = dataHelper.randomData("testValue-");

    Table table = getConnection().getTable(sharedTestEnv.getDefaultTableName());

    // Put
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, testQualifier, testValue);
    table.put(put);

    // Get
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, testQualifier);
    Result result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(1, cells.size());
    Assert.assertTrue(Arrays.equals(testValue, CellUtil.cloneValue(cells.get(0))));

    // Delete
    Delete delete = new Delete(rowKey);
    delete.addColumns(COLUMN_FAMILY, testQualifier);
    table.delete(delete);

    // Confirm deleted
    Assert.assertFalse(table.exists(get));
    table.close();
  }
}
