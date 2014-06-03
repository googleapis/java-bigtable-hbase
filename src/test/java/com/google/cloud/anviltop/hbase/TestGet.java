package com.google.cloud.anviltop.hbase;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.junit.Assert;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

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
public class TestGet extends AbstractTest {
  /**
   * Requirement 3.2 - If a column family is requested, but no qualifier, all columns in that family
   * are returned
   */
  @Test
  public void testNoQualifier() throws IOException {
    // Initialize variables
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.random(8));
    byte[] qual1 = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    byte[] value1 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] qual2 = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    byte[] value2 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    byte[] qual3 = Bytes.toBytes("qual-" + RandomStringUtils.random(8));
    byte[] value3 = Bytes.toBytes("value-" + RandomStringUtils.random(8));
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qual1, value1);
    put.add(COLUMN_FAMILY, qual2, value2);
    put.add(COLUMN_FAMILY, qual3, value3);
    table.put(put);

    Get get = new Get(rowKey);
    get.addFamily(COLUMN_FAMILY);
    Result result = table.get(get);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual1));
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual2));
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, qual3));
    Assert.assertArrayEquals(value1,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1)));
    Assert.assertArrayEquals(value2,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2)));
    Assert.assertArrayEquals(value3,
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual3)));

    Delete delete = new Delete(rowKey);
    table.delete(delete);

    table.close();
  }
}
