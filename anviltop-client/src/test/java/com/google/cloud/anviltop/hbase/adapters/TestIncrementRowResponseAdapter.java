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

import com.google.bigtable.anviltop.AnviltopData;
import com.google.bigtable.anviltop.AnviltopServices.IncrementRowResponse;
import com.google.cloud.anviltop.hbase.DataGenerationHelper;
import com.google.protobuf.ByteString;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestIncrementRowResponseAdapter {
  protected IncrementRowResponseAdapter adapter = new IncrementRowResponseAdapter();
  protected QualifierTestHelper qualifierTestHelper = new QualifierTestHelper();
  protected DataGenerationHelper dataHelper = new DataGenerationHelper();

  @Test
  public void testEmptyResponse() {
    IncrementRowResponse.Builder responseBuilder = IncrementRowResponse.newBuilder();

    Result result = adapter.adaptResponse(responseBuilder.build());
    Assert.assertNull(result.getRow());
  }

  @Test
  public void testSingleCellResult() {
    byte[] rowKey = dataHelper.randomData("rk1-");
    byte[] columnFamily = Bytes.toBytes("cf");
    byte[] qualfier = dataHelper.randomData("col1");
    byte[] value = dataHelper.randomData("val1");
    long anviltopTimestamp = 10000L;
    byte[] fullQualifier = qualifierTestHelper.makeFullQualifier(columnFamily, qualfier);

    IncrementRowResponse.Builder responseBuilder = IncrementRowResponse.newBuilder();
    AnviltopData.Row.Builder rowBuilder = responseBuilder.getRowBuilder();
    rowBuilder.setRowKey(ByteString.copyFrom(rowKey));

    AnviltopData.Column.Builder columnBuilder = rowBuilder.addColumnsBuilder();
    columnBuilder.setColumnName(ByteString.copyFrom(fullQualifier));

    AnviltopData.Cell.Builder cellBuilder = columnBuilder.addCellsBuilder();
    cellBuilder.setValue(ByteString.copyFrom(value));
    cellBuilder.setTimestampMicros(anviltopTimestamp);

    Result result = adapter.adaptResponse(responseBuilder.build());
    Assert.assertEquals(1, result.size());

    Cell cell = result.listCells().get(0);
    Assert.assertArrayEquals(value, CellUtil.cloneValue(cell));
    Assert.assertArrayEquals(columnFamily, CellUtil.cloneFamily(cell));
    Assert.assertArrayEquals(qualfier, CellUtil.cloneQualifier(cell));
    Assert.assertArrayEquals(rowKey, CellUtil.cloneRow(cell));
    Assert.assertEquals(anviltopTimestamp / 1000L, cell.getTimestamp());
  }
}
