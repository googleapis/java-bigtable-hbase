/*
 * Copyright (c) 2014 Google Inc.
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
package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopServiceMessages.IncrementRowRequest;
import com.google.bigtable.anviltop.AnviltopData.RowIncrement;
import com.google.cloud.anviltop.hbase.DataGenerationHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestIncrementAdapter {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected IncrementAdapter incrementAdapter = new IncrementAdapter();
  protected DataGenerationHelper dataHelper = new DataGenerationHelper();

  @Test
  public void testBasicRowKeyIncrement() throws IOException {
    byte[] rowKey = dataHelper.randomData("rk1-");
    Increment incr = new Increment(rowKey);
    IncrementRowRequest.Builder requestBuilder = incrementAdapter.adapt(incr);
    ByteString adaptedRowKey = requestBuilder.getIncrement().getRowKey();
    Assert.assertArrayEquals(rowKey, adaptedRowKey.toByteArray());
  }

  @Test
  public void testSingleIncrement() throws IOException {
    byte[] rowKey = dataHelper.randomData("rk1-");
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("qualifier");
    long amount = 1234;

    Increment incr = new Increment(rowKey);
    incr.addColumn(family, qualifier, amount);

    IncrementRowRequest.Builder requestBuilder = incrementAdapter.adapt(incr);
    Assert.assertEquals(1, requestBuilder.getIncrement().getIncrsCount());
    RowIncrement.Increment incrProto = requestBuilder.getIncrement().getIncrs(0);
    Assert.assertEquals("family:qualifier", incrProto.getColumnName().toStringUtf8());
    Assert.assertEquals(amount, incrProto.getAmount());
  }

  @Test
  public void testMultipleIncrement() throws IOException {
    byte[] rowKey = dataHelper.randomData("rk1-");

    byte[] family1 = Bytes.toBytes("family1");
    byte[] qualifier1 = Bytes.toBytes("qualifier1");
    long amount1 = 1234;

    byte[] family2 = Bytes.toBytes("family2");
    byte[] qualifier2 = Bytes.toBytes("qualifier2");
    long amount2 = 4321;


    Increment incr = new Increment(rowKey);
    incr.addColumn(family1, qualifier1, amount1);
    incr.addColumn(family2, qualifier2, amount2);

    IncrementRowRequest.Builder requestBuilder = incrementAdapter.adapt(incr);
    Assert.assertEquals(2, requestBuilder.getIncrement().getIncrsCount());

    RowIncrement.Increment incrProto = requestBuilder.getIncrement().getIncrs(0);
    Assert.assertEquals("family1:qualifier1", incrProto.getColumnName().toStringUtf8());
    Assert.assertEquals(amount1, incrProto.getAmount());

    incrProto = requestBuilder.getIncrement().getIncrs(1);
    Assert.assertEquals("family2:qualifier2", incrProto.getColumnName().toStringUtf8());
    Assert.assertEquals(amount2, incrProto.getAmount());
  }

  @Test
  public void testIncrementTimeRange() throws IOException {
    byte[] rowKey = dataHelper.randomData("rk1-");
    Increment incr = new Increment(rowKey);
    incr.setTimeRange(0, 10);
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Setting the time range in an Increment is not implemented");

    IncrementRowRequest.Builder requestBuilder = incrementAdapter.adapt(incr);
  }
}
