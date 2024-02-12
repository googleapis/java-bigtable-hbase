/*
 * Copyright 2015 Google LLC
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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Requirement 1.1 - Writes are buffered in the client by default (can be disabled). Buffer size can
 * be defined programmatically or configuring the hbase.client.write.buffer property.
 *
 * <p>TODO - Test buffer size definitions
 */
public class TestBufferedMutator extends AbstractTest {

  @Test
  public void testAutoFlushOff() throws Exception {
    try (BufferedMutator mutator =
            getConnection().getBufferedMutator(sharedTestEnv.getDefaultTableName());
        Connection c = createNewConnection();
        Table tableForRead = c.getTable(sharedTestEnv.getDefaultTableName()); ) {
      // Set up the tiny write and read
      mutator.mutate(getPut());
      Get get = getGet();

      // Bigtable pushes the change right away.  This test would be flaky.
      //      Assert.assertEquals("Expecting no results", 0, tableForRead.get(get).size());
      mutator.flush();
      Assert.assertEquals("Expecting one result", 1, tableForRead.get(get).size());
    }
  }

  @Test
  public void testAutoFlushOn() throws Exception {
    try (Table mutator = getDefaultTable();
        Connection c = createNewConnection();
        Table tableForRead = c.getTable(sharedTestEnv.getDefaultTableName()); ) {
      mutator.put(getPut());
      Assert.assertEquals("Expecting one result", 1, tableForRead.get(getGet()).size());
    }
  }

  @Test
  @Ignore(value = "We need a better test now that BigtableBufferedMutator has different logic")
  public void testBufferSizeFlush() throws Exception {
    int maxSize = 1024;
    BufferedMutatorParams params =
        new BufferedMutatorParams(sharedTestEnv.getDefaultTableName()).writeBufferSize(maxSize);
    try (BufferedMutator mutator = getConnection().getBufferedMutator(params)) {
      // HBase 1.0.0 has a bug in it. It returns maxSize instead of the buffer size for
      // getWriteBufferSize.  https://issues.apache.org/jira/browse/HBASE-13113
      Assert.assertTrue(
          0 == mutator.getWriteBufferSize() || maxSize == mutator.getWriteBufferSize());

      Put put = getPut();
      mutator.mutate(put);
      Assert.assertTrue(mutator.getWriteBufferSize() > 0);

      Put largePut = new Put(dataHelper.randomData("testrow-"));
      largePut.addColumn(
          COLUMN_FAMILY,
          qualifier,
          Bytes.toBytes(RandomStringUtils.randomAlphanumeric(maxSize * 2)));
      long heapSize = largePut.heapSize();
      Assert.assertTrue("largePut heapsize is : " + heapSize, heapSize > maxSize);
      mutator.mutate(largePut);

      // HBase 1.0.0 has a bug in it. It returns maxSize instead of the buffer size for
      // getWriteBufferSize.  https://issues.apache.org/jira/browse/HBASE-13113
      Assert.assertTrue(
          0 == mutator.getWriteBufferSize() || maxSize == mutator.getWriteBufferSize());
    }
  }

  @Test
  public void testBufferedMutatorWithNullAndEmptyValues() throws Exception {
    BufferedMutatorParams params = new BufferedMutatorParams(sharedTestEnv.getDefaultTableName());
    try (BufferedMutator bm = getConnection().getBufferedMutator(params)) {
      Exception actualError = null;
      try {
        bm.mutate((List<? extends Mutation>) null);
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNotNull(actualError);
      actualError = null;

      try {
        bm.mutate(Collections.emptyList());
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNull(actualError);
      actualError = null;

      try {
        bm.mutate(new Put(new byte[0]));
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNotNull(actualError);
      actualError = null;

      try {
        byte[] rowKey = dataHelper.randomData("test-row");
        bm.mutate(new Put(rowKey).addColumn(COLUMN_FAMILY, null, value));
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNull(actualError);

      try {
        byte[] rowKey = dataHelper.randomData("test-row");
        bm.mutate(new Put(rowKey).addColumn(COLUMN_FAMILY, new byte[0], value));
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNull(actualError);

      try {
        byte[] rowKey = dataHelper.randomData("test-row");
        bm.mutate(new Put(rowKey).addColumn(COLUMN_FAMILY, qualifier, null));
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNull(actualError);

      try {
        byte[] rowKey = dataHelper.randomData("test-row");
        bm.mutate(new Put(rowKey).addColumn(COLUMN_FAMILY, qualifier, new byte[0]));
      } catch (Exception ex) {
        actualError = ex;
      }
      assertNull(actualError);
    }
  }

  @Test
  public void testBulkMutation() throws Exception {
    final String rowKeyPrefix = RandomStringUtils.randomAlphanumeric(10);
    try (BufferedMutator mutator =
            getConnection().getBufferedMutator(sharedTestEnv.getDefaultTableName());
        Table tableForRead = getConnection().getTable(sharedTestEnv.getDefaultTableName())) {

      List<Put> mutations = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        mutations.add(
            new Put(Bytes.toBytes(rowKeyPrefix + i))
                .addColumn(COLUMN_FAMILY, qualifier, 10_001L, value));
      }

      mutator.mutate(mutations);
      // force bufferedMutator to apply mutation
      mutator.flush();

      Scan scan = new Scan();
      scan.setRowPrefixFilter(Bytes.toBytes(rowKeyPrefix));
      ResultScanner resultScanner = tableForRead.getScanner(scan);
      Result[] results = resultScanner.next(20);

      assertEquals("mutations should have been applied now", mutations.size(), results.length);
      for (int i = 0; i < results.length; i++) {
        assertArrayEquals(mutations.get(i).getRow(), results[i].getRow());
      }
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
    put.addColumn(COLUMN_FAMILY, qualifier, value);
    return put;
  }
}
