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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.IntegrationTests.COLUMN_FAMILY;
import static com.google.cloud.bigtable.hbase.IntegrationTests.TABLE_NAME;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Requirement 1.1 - Writes are buffered in the client by default (can be disabled).  Buffer size
 * can be defined programmatically or configuring the hbase.client.write.buffer property.
 *
 * TODO - Test buffer size definitions
 */
public class TestBufferedMutator extends AbstractTest {

  @Test
  public void testAutoFlushOff() throws Exception {
    try (BufferedMutator mutator = connection.getBufferedMutator(TABLE_NAME);
        Table tableForRead = createNewConnection().getTable(TABLE_NAME);) {
      // Set up the tiny write and read
      mutator.mutate(getPut());
      Get get = getGet();

      Assert.assertEquals("Expecting no results", 0, tableForRead.get(get).size());
      mutator.flush();
      Assert.assertEquals("Expecting one result", 1, tableForRead.get(get).size());
    }
  }

  @Test
  public void testAutoFlushOn() throws Exception {
    try (Table mutator = connection.getTable(TABLE_NAME);
        Table tableForRead = createNewConnection().getTable(TABLE_NAME);) {
      mutator.put(getPut());
      Assert.assertEquals("Expecting one result", 1, tableForRead.get(getGet()).size());
    }
  }

  @Test
  public void testBufferSizeFlush() throws Exception {
    int maxSize = 32 * 1024;
    try (BufferedMutator mutator =
        connection.getBufferedMutator(new BufferedMutatorParams(TABLE_NAME)
            .writeBufferSize(maxSize))) {
      Put put = getPut();
      mutator.mutate(put);
      Assert.assertEquals(put.heapSize(), mutator.getWriteBufferSize());

      Put largePut = new Put(dataHelper.randomData("testrow-"));
      put.add(COLUMN_FAMILY, qualifier,
        Bytes.toBytes(RandomStringUtils.randomAlphanumeric(maxSize)));
      mutator.mutate(largePut);
      Assert.assertEquals(0, mutator.getWriteBufferSize());
    }
  }

  final byte[] qualifier = dataHelper.randomData("testQualifier-");
  final byte[] rowKey = dataHelper.randomData("testrow-");
  final byte[] value = dataHelper.randomData("testValue-");

  private Get getGet() {
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, qualifier);
    return get;
  }

  private Put getPut() {
    Put put = new Put(rowKey);
    put.add(COLUMN_FAMILY, qualifier, value);
    return put;
  }
}
