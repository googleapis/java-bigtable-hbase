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
package com.google.cloud.bigtable.hbase;

import org.apache.hadoop.hbase.client.Table.CheckAndMutateBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.ExecutionException;

@RunWith(JUnit4.class)
@SuppressWarnings("deprecation")
public class TestCheckAndMutateHBase2Builder extends AbstractTestCheckAndMutate {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Override
  protected boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws Exception {
    try {
      return getBuilder(row, family, qualifier, CompareOp.EQUAL, value).thenPut(put);
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp op,
      byte[] value, Put put) throws Exception {
    try {
      return getBuilder(row, family, qualifier, op, value).thenPut(put);
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws Exception {
    try {
      return getBuilder(row, family, qualifier, CompareOp.EQUAL, value).thenDelete(delete);
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp op,
      byte[] value, RowMutations rm) throws Exception {
    try {
      return getBuilder(row, family, qualifier, op, value).thenMutate(rm);
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  private CheckAndMutateBuilder getBuilder(byte[] row, byte[] family, byte[] qualifier,
      CompareOp op, byte[] value) throws Exception {
    CheckAndMutateBuilder builder = getDefaultTable().checkAndMutate(row,family)
        .qualifier(qualifier);
    if (value == null && op != CompareOp.NOT_EQUAL) {
      return builder.ifNotExists();
    } else {
      return builder.ifMatches(TestCheckAndMutateHBase2.translate(op), value);
    }
  }
}
