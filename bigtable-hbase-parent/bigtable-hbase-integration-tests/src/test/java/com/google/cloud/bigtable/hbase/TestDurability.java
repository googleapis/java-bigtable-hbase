/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import static com.google.cloud.bigtable.hbase.IntegrationTests.*;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestDurability extends AbstractTest {
  /**
   * Requirement 1.5 - A client can specify the durability guarantee of any mutation.
   *
   * Bigtable can do without durability hints.  Its recovery and write-throughput is fast enough
   * that the benefits are not that great.  We do however need to not break if a user provides a
   * durability hint.  We'll always do a durable write regardless of the hint.
   */
  @Test
  public void testDurability() throws IOException {
    for (Durability durability : Durability.values()) {
      testDurability(durability);
    }
  }

  private void testDurability(Durability durability) throws IOException {
    // Initialize
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    byte[] testValue = dataHelper.randomData("testValue-");

    // Put
    Put put = new Put(rowKey);
    put.setDurability(durability);
    put.addColumn(COLUMN_FAMILY, testQualifier, testValue);
    Table table = getConnection().getTable(TABLE_NAME);
    table.put(put);

    // Get
    Get get = new Get(rowKey).addColumn(COLUMN_FAMILY, testQualifier);
    Result result = table.get(get);
    Assert.assertTrue("Durability=" + durability,
        result.containsColumn(COLUMN_FAMILY, testQualifier));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals("Durability=" + durability, 1, cells.size());
    Assert.assertArrayEquals("Durability=" + durability, testValue,
        CellUtil.cloneValue(cells.get(0)));

    // Delete
    Delete delete = new Delete(rowKey);
    delete.addColumns(COLUMN_FAMILY, testQualifier);
    table.delete(delete);

    // Confirm deleted
    Assert.assertFalse("Durability=" + durability, table.exists(get));
    table.close();
  }
}
