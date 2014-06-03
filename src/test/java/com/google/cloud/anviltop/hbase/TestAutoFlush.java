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
package com.google.cloud.anviltop.hbase;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Requirement 1.1 - Writes are buffered in the client by default (can be disabled).  Buffer size
 * can be defined programmatically or configuring the hbase.client.write.buffer property.
 *
 * TODO - Test buffer size definitions
 */
public class TestAutoFlush extends AbstractTest {
  @Test
  public void testAutoFlushDefault() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME);
    Assert.assertTrue("Auto-flush on", table.isAutoFlush());
  }

  @Test
  public void testAutoFlushOff() throws Exception {
    HTableInterface tableForWrite = connection.getTable(TABLE_NAME);
    tableForWrite.setAutoFlushTo(false);
    HTableInterface tableForRead = createNewConnection().getTable(TABLE_NAME);
    Get get = quickPutThenGet(tableForWrite);

    Result result = tableForRead.get(get);

    Assert.assertEquals("Expecting no results", 0, result.size());
    tableForWrite.flushCommits();
    result = tableForRead.get(get);
    Assert.assertEquals("Expecting one result", 1, result.size());
  }

  @Test
  public void testAutoFlushOn() throws Exception {
    HTableInterface tableForWrite = connection.getTable(TABLE_NAME);
    tableForWrite.setAutoFlushTo(true);
    HTableInterface tableForRead = createNewConnection().getTable(TABLE_NAME);
    Get get = quickPutThenGet(tableForWrite);

    Result result = tableForRead.get(get);
    Assert.assertEquals("Expecting one result", 1, result.size());
  }

  private Get quickPutThenGet(HTableInterface tableForWrite) throws IOException {
    // Set up the tiny write and read
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qualifier, value);
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qualifier);

    // Write and read in quick succession.  If this gets race-y, the test will be flaky and will
    // need to be rethought.
    tableForWrite.put(put);
    return get;
  }
}
