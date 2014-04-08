package com.google.cloud.anviltop.hbase;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

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
public class TestDurability extends AbstractTest {
  final String TABLE_NAME = "test";
  final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");

  /**
   * Bigtable doesn't need durability hints.  Its recover and write-throughput is fast enough that
   * the benefits are negligible.  We do however need to not break if a user provides a durability
   * hint.
   *
   * @throws IOException
   */
  @Ignore // TODO(carterpage) - enable once implemented
  @Test
  public void testDurability() throws IOException {
    for (Durability durability : Durability.values()) {
      testDurability(durability);
    }
  }

  private void testDurability(Durability durability) throws IOException {
    // Initialize
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] testQualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.random(8));
    byte[] testValue = Bytes.toBytes("testValue-" + RandomStringUtils.random(8));

    // Put
    Put put = new Put(rowKey);
    put.setDurability(durability);
    put.add(COLUMN_FAMILY, testQualifier, testValue);
    HTableInterface table = connection.getTable(TABLE_NAME);
    table.put(put);

    // Get
    Get get = new Get(rowKey).addColumn(COLUMN_FAMILY, testQualifier);
    Result result = table.get(get);
    Assert.assertTrue("Durability=" + durability,
        result.containsColumn(COLUMN_FAMILY, testQualifier));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals("Durability=" + durability, 1, cells.size());
    Assert.assertArrayEquals("Durability=" + durability, testValue, cells.get(0).getValueArray());

    // Delete
    Delete delete = new Delete(rowKey);
    delete.deleteColumn(COLUMN_FAMILY, testQualifier);
    table.delete(delete);

    // Confirm deleted
    Assert.assertFalse("Durability=" + durability, table.exists(get));
  }
}
