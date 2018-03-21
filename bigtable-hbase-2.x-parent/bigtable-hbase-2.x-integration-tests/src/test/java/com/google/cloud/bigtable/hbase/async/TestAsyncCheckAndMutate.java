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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTable.CheckAndMutateBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.hbase.AbstractTestCheckAndMutate;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@RunWith(JUnit4.class)
@SuppressWarnings("deprecation")
public class TestAsyncCheckAndMutate extends AbstractTestCheckAndMutate {
  private static ExecutorService executor;
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setup() {
    executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TestAsyncCheckAndMutate").build());
  }

  @AfterClass
  public static void teardown() {
    executor.shutdownNow();
  }

  @Override
  protected boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws Exception {
    try {
      return getBuilder(row, family, qualifier, CompareOp.EQUAL, value).thenPut(put).get();
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp op,
      byte[] value, Put put) throws Exception {
    try {
      return getBuilder(row, family, qualifier, op, value).thenPut(put).get();
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws Exception {
    try {
      return getBuilder(row, family, qualifier, CompareOp.EQUAL, value).thenDelete(delete).get();
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp op,
      byte[] value, RowMutations rm) throws Exception {
    try {
      return getBuilder(row, family, qualifier, op, value).thenMutate(rm).get();
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  private CheckAndMutateBuilder getBuilder(byte[] row, byte[] family, byte[] qualifier,
      CompareOp op, byte[] value) throws InterruptedException, ExecutionException {
    CheckAndMutateBuilder builder = getDefaultAsyncTable().checkAndMutate(row,family)
        .qualifier(qualifier);
    if (value == null && op != CompareOp.NOT_EQUAL) {
      return builder.ifNotExists();
    } else {
      return builder.ifMatches(CompareOperator.valueOf(op.name()), value);
    }
  }

  protected AsyncTable getDefaultAsyncTable() throws InterruptedException, ExecutionException {
    return AbstractAsyncTest.getAsyncConnection().getTable(sharedTestEnv.getDefaultTableName(),
      executor);
  }

}
