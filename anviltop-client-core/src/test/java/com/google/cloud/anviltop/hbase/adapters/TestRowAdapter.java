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
import com.google.bigtable.anviltop.AnviltopData.Row;
import com.google.cloud.anviltop.hbase.DataGenerationHelper;
import com.google.cloud.hadoop.hbase.repackaged.protobuf.ByteString;

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
public class TestRowAdapter {
  protected RowAdapter adapter = new RowAdapter();
  protected QualifierTestHelper qualifierTestHelper = new QualifierTestHelper();
  protected DataGenerationHelper dataHelper = new DataGenerationHelper();

  @Test
  public void testRowKeyNotPresentWhenNoCellsReturned() {
    byte[] rowKey = dataHelper.randomData("rk1-");
    Row.Builder rowBuilder = Row.newBuilder();
    rowBuilder.setRowKey(ByteString.copyFrom(rowKey));


    Result result = adapter.adaptResponse(rowBuilder.build());
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

    Row.Builder rowBuilder = Row.newBuilder();
    rowBuilder.setRowKey(ByteString.copyFrom(rowKey));

    AnviltopData.Column.Builder columnBuilder = rowBuilder.addColumnsBuilder();
    columnBuilder.setColumnName(ByteString.copyFrom(fullQualifier));

    AnviltopData.Cell.Builder cellBuilder = columnBuilder.addCellsBuilder();
    cellBuilder.setValue(ByteString.copyFrom(value));
    cellBuilder.setTimestampMicros(anviltopTimestamp);

    Result result = adapter.adaptResponse(rowBuilder.build());
    Assert.assertEquals(1, result.size());

    Cell cell = result.listCells().get(0);
    Assert.assertArrayEquals(value, CellUtil.cloneValue(cell));
    Assert.assertArrayEquals(columnFamily, CellUtil.cloneFamily(cell));
    Assert.assertArrayEquals(qualfier, CellUtil.cloneQualifier(cell));
    Assert.assertArrayEquals(rowKey, CellUtil.cloneRow(cell));
    Assert.assertEquals(anviltopTimestamp / 1000L, cell.getTimestamp());
  }

  @Test
  public void testMultipleCellsInSingleColumnResult() {
    byte[] rowKey = dataHelper.randomData("rk1-");
    byte[] columnFamily = Bytes.toBytes("cf");
    byte[] qualfier = dataHelper.randomData("col1");
    byte[] value1 = dataHelper.randomData("val1");
    long anviltopTimestamp1 = 20000L;
    byte[] value2 = dataHelper.randomData("val2");
    long anviltopTimestamp2 = 10000L;
    byte[] fullQualifier = qualifierTestHelper.makeFullQualifier(columnFamily, qualfier);

    Row.Builder rowBuilder = Row.newBuilder();
    rowBuilder.setRowKey(ByteString.copyFrom(rowKey));

    AnviltopData.Column.Builder columnBuilder = rowBuilder.addColumnsBuilder();
    columnBuilder.setColumnName(ByteString.copyFrom(fullQualifier));

    AnviltopData.Cell.Builder cellBuilder = columnBuilder.addCellsBuilder();
    cellBuilder.setValue(ByteString.copyFrom(value1));
    cellBuilder.setTimestampMicros(anviltopTimestamp1);

    cellBuilder = columnBuilder.addCellsBuilder();
    cellBuilder.setValue(ByteString.copyFrom(value2));
    cellBuilder.setTimestampMicros(anviltopTimestamp2);

    Result result = adapter.adaptResponse(rowBuilder.build());
    Assert.assertEquals(2, result.size());

    Cell cell1 = result.listCells().get(0);
    Assert.assertArrayEquals(columnFamily, CellUtil.cloneFamily(cell1));
    Assert.assertArrayEquals(qualfier, CellUtil.cloneQualifier(cell1));
    Assert.assertArrayEquals(value1, CellUtil.cloneValue(cell1));
    Assert.assertEquals(anviltopTimestamp1 / 1000L, cell1.getTimestamp());

    Cell cell2 = result.listCells().get(1);
    Assert.assertArrayEquals(columnFamily, CellUtil.cloneFamily(cell2));
    Assert.assertArrayEquals(qualfier, CellUtil.cloneQualifier(cell2));
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(cell2));
    Assert.assertEquals(anviltopTimestamp2 / 1000L, cell2.getTimestamp());
  }

  @Test
  public void testMultipleCellsAreSortedByColumnName() {
    byte[] rowKey = dataHelper.randomData("rk1-");
    byte[] columnFamily = Bytes.toBytes("cf");
    byte[] qualfier1 = dataHelper.randomData("col1");
    byte[] qualfier2 = dataHelper.randomData("col2");
    byte[] value1 = dataHelper.randomData("val1");
    long anviltopTimestamp1 = 10000L;
    byte[] value2 = dataHelper.randomData("val2");
    long anviltopTimestamp2 = 10000L;
    byte[] fullQualifier1 = qualifierTestHelper.makeFullQualifier(columnFamily, qualfier1);
    byte[] fullQualifier2 = qualifierTestHelper.makeFullQualifier(columnFamily, qualfier2);

    Row.Builder rowBuilder = Row.newBuilder();
    rowBuilder.setRowKey(ByteString.copyFrom(rowKey));

    AnviltopData.Column.Builder columnBuilder = rowBuilder.addColumnsBuilder();
    columnBuilder.setColumnName(ByteString.copyFrom(fullQualifier1));

    AnviltopData.Cell.Builder cellBuilder = columnBuilder.addCellsBuilder();
    cellBuilder.setValue(ByteString.copyFrom(value1));
    cellBuilder.setTimestampMicros(anviltopTimestamp1);

    columnBuilder = rowBuilder.addColumnsBuilder();
    columnBuilder.setColumnName(ByteString.copyFrom(fullQualifier2));

    cellBuilder = columnBuilder.addCellsBuilder();
    cellBuilder.setValue(ByteString.copyFrom(value2));
    cellBuilder.setTimestampMicros(anviltopTimestamp2);

    Result result = adapter.adaptResponse(rowBuilder.build());
    Assert.assertEquals(2, result.size());

    Cell cell1 = result.listCells().get(0);
    Assert.assertArrayEquals(columnFamily, CellUtil.cloneFamily(cell1));
    Assert.assertArrayEquals(qualfier1, CellUtil.cloneQualifier(cell1));
    Assert.assertArrayEquals(value1, CellUtil.cloneValue(cell1));
    Assert.assertEquals(anviltopTimestamp1 / 1000L, cell1.getTimestamp());

    Cell cell2 = result.listCells().get(1);
    Assert.assertArrayEquals(columnFamily, CellUtil.cloneFamily(cell2));
    Assert.assertArrayEquals(qualfier2, CellUtil.cloneQualifier(cell2));
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(cell2));
    Assert.assertEquals(anviltopTimestamp2 / 1000L, cell2.getTimestamp());
  }
}
