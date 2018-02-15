/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.async;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncBufferedMutator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;

/**
 * Requirement 1.1 - Writes are buffered in the client by default (can be disabled).  Buffer size
 * can be defined programmatically or configuring the hbase.client.write.buffer property.
 */
public class TestAsyncBufferedMutator extends AbstractAsyncTest {

  @Test
  public void testBufferdMutator() throws Exception {
    TableName defaultTableName = sharedTestEnv.getDefaultTableName();

    final byte[] rowKey = dataHelper.randomData("TestAsyncBufferedMutator-");
    final byte[] qualifier = dataHelper.randomData("testQualifier-");
    final byte[] value = dataHelper.randomData("testValue-");

    try (AsyncBufferedMutator mutator = getAsyncConnection().getBufferedMutator(defaultTableName);
        Table tableForRead = getDefaultTable();) {
      Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qualifier, value);
      Get get = new Get(rowKey).addColumn(COLUMN_FAMILY, qualifier);
      CompletableFuture<Void> future = mutator.mutate(put);
      mutator.flush();
      future.get(30, TimeUnit.SECONDS);
      Assert.assertEquals("Expecting one result", 1, tableForRead.get(get).size());
    }
  }

  @Test
  public void testBufferdMutatorMulti() throws Exception {
    final byte[] rowKey1 = dataHelper.randomData("TestAsyncBufferedMutator-");
    final byte[] rowKey2 = dataHelper.randomData("TestAsyncBufferedMutator-");
    final byte[] qualifier = dataHelper.randomData("testQualifier-");
    final byte[] value = dataHelper.randomData("testValue-");

    Put put1 = new Put(rowKey1).addColumn(COLUMN_FAMILY, qualifier, value);
    Put put2 = new Put(rowKey2).addColumn(COLUMN_FAMILY, qualifier, value);
    Get get1 = new Get(rowKey1);
    Get get2 = new Get(rowKey2);

    
    TableName defaultTableName = sharedTestEnv.getDefaultTableName();
    try (AsyncBufferedMutator mutator = getAsyncConnection().getBufferedMutator(defaultTableName);
        Table tableForRead = getDefaultTable();) {
      List<CompletableFuture<Void>> futures = mutator.mutate(Arrays.asList(put1, put2));
      mutator.flush();
      for (CompletableFuture<Void> f : futures) {
        f.get(30, TimeUnit.SECONDS);
      }
      Result[] results = tableForRead.get(Arrays.asList(get1, get2));
      Assert.assertEquals("Expecting one result", 2, results.length);
      Assert.assertTrue(CellUtil.matchingValue(results[0].rawCells()[0], value));
      Assert.assertTrue(CellUtil.matchingValue(results[1].rawCells()[0], value));
    }
  }
}
