/*
 * Copyright 2018 Google LLC
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

import java.io.IOException;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings("deprecation")
public class TestCheckAndMutateHBase2 extends AbstractTestCheckAndMutate {

  @Override
  protected boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    return getDefaultTable().checkAndPut(row, family, qualifier, value, put);
  }

  @Override
  protected boolean checkAndPut(
      byte[] row, byte[] family, byte[] qualifier, CompareOp op, byte[] value, Put put)
      throws IOException {
    return getDefaultTable()
        .checkAndPut(row, family, qualifier, CompareOperator.valueOf(op.name()), value, put);
  }

  @Override
  protected boolean checkAndDelete(
      byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
    return getDefaultTable().checkAndDelete(row, family, qualifier, value, delete);
  }

  @Override
  protected boolean checkAndMutate(
      byte[] row, byte[] family, byte[] qualifier, CompareOp op, byte[] value, RowMutations rm)
      throws IOException {
    return getDefaultTable()
        .checkAndMutate(row, family, qualifier, CompareOperator.valueOf(op.name()), value, rm);
  }
}
