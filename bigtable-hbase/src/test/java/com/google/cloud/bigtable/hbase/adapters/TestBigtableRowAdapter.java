package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for BigtableRowAdapter.
 */
@RunWith(JUnit4.class)
public class TestBigtableRowAdapter {
  protected BigtableRowAdapter adapter = new BigtableRowAdapter();
  protected DataGenerationHelper dataHelper = new DataGenerationHelper();

  @Test
  public void testRowKeyNotPresentWhenNoCellsReturned() {
    byte[] rowKey = dataHelper.randomData("rk1-");
    Row.Builder rowBuilder = Row.newBuilder();
    rowBuilder.setKey(ByteString.copyFrom(rowKey));

    Result result = adapter.adaptResponse(rowBuilder.build());
    Assert.assertNull(result.getRow());
  }

  @Test
  public void testNullIsAnEmptyResult() {
    Row row = null;
    Result result = adapter.adaptResponse(row);
    Assert.assertNull(result.getRow());
  }

  @Test
  public void testSingleCellResult() {
    byte[] rowKey = dataHelper.randomData("rk1-");
    String columnFamily = "cf";
    byte[] qualfier = dataHelper.randomData("col1");
    byte[] value = dataHelper.randomData("val1");
    long anviltopTimestamp = 10000L;

    Row.Builder rowBuilder = Row.newBuilder();
    rowBuilder.setKey(ByteString.copyFrom(rowKey));

    Family.Builder familyBuilder = rowBuilder.addFamiliesBuilder();
    familyBuilder.setName(columnFamily);

    Column.Builder columnBuilder = familyBuilder.addColumnsBuilder();
    columnBuilder.setQualifier(ByteString.copyFrom(qualfier));

    Cell.Builder cellBuilder = columnBuilder.addCellsBuilder();
    cellBuilder.setValue(ByteString.copyFrom(value));
    cellBuilder.setTimestampMicros(anviltopTimestamp);

    Result result = adapter.adaptResponse(rowBuilder.build());
    Assert.assertEquals(1, result.size());

    org.apache.hadoop.hbase.Cell cell = result.listCells().get(0);
    Assert.assertArrayEquals(value, CellUtil.cloneValue(cell));
    Assert.assertArrayEquals(Bytes.toBytes(columnFamily), CellUtil.cloneFamily(cell));
    Assert.assertArrayEquals(qualfier, CellUtil.cloneQualifier(cell));
    Assert.assertArrayEquals(rowKey, CellUtil.cloneRow(cell));
    Assert.assertEquals(anviltopTimestamp / 1000L, cell.getTimestamp());
  }

  @Test
  public void testMultipleCellsInSingleColumnResult() {
    byte[] rowKey = dataHelper.randomData("rk1-");
    String columnFamily = "cf";
    byte[] qualfier = dataHelper.randomData("col1");
    byte[] value1 = dataHelper.randomData("val1");
    long anviltopTimestamp1 = 20000L;
    byte[] value2 = dataHelper.randomData("val2");
    long anviltopTimestamp2 = 10000L;

    Row.Builder rowBuilder = Row.newBuilder();
    rowBuilder.setKey(ByteString.copyFrom(rowKey));

    Family.Builder familyBuilder = rowBuilder.addFamiliesBuilder();
    familyBuilder.setName(columnFamily);

    Column.Builder columnBuilder = familyBuilder.addColumnsBuilder();
    columnBuilder.setQualifier(ByteString.copyFrom(qualfier));

    Cell.Builder cellBuilder = columnBuilder.addCellsBuilder();
    cellBuilder.setValue(ByteString.copyFrom(value1));
    cellBuilder.setTimestampMicros(anviltopTimestamp1);

    Cell.Builder cellBuilder2 = columnBuilder.addCellsBuilder();
    cellBuilder2.setValue(ByteString.copyFrom(value2));
    cellBuilder2.setTimestampMicros(anviltopTimestamp2);

    Result result = adapter.adaptResponse(rowBuilder.build());
    Assert.assertEquals(2, result.size());

    org.apache.hadoop.hbase.Cell cell1 = result.listCells().get(0);
    Assert.assertArrayEquals(Bytes.toBytes(columnFamily), CellUtil.cloneFamily(cell1));
    Assert.assertArrayEquals(qualfier, CellUtil.cloneQualifier(cell1));
    Assert.assertArrayEquals(value1, CellUtil.cloneValue(cell1));
    Assert.assertEquals(anviltopTimestamp1 / 1000L, cell1.getTimestamp());

    org.apache.hadoop.hbase.Cell cell2 = result.listCells().get(1);
    Assert.assertArrayEquals(Bytes.toBytes(columnFamily), CellUtil.cloneFamily(cell2));
    Assert.assertArrayEquals(qualfier, CellUtil.cloneQualifier(cell2));
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(cell2));
    Assert.assertEquals(anviltopTimestamp2 / 1000L, cell2.getTimestamp());
  }

  @Test
  public void testMultipleCellsAreSortedByColumnName() {
    byte[] rowKey = dataHelper.randomData("rk1-");
    String columnFamily = "cf";
    byte[] qualfierA = dataHelper.randomData("colA");
    byte[] qualfierB = dataHelper.randomData("colB");
    byte[] valueA = dataHelper.randomData("valA");
    long anviltopTimestampA = 10000L;
    byte[] valueB = dataHelper.randomData("valB");
    long anviltopTimestampB = 10000L;

    Row.Builder rowBuilder = Row.newBuilder();
    rowBuilder.setKey(ByteString.copyFrom(rowKey));

    Family.Builder familyBuilder = rowBuilder.addFamiliesBuilder();
    familyBuilder.setName(columnFamily);

    Column.Builder columnBuilderB = familyBuilder.addColumnsBuilder();
    columnBuilderB.setQualifier(ByteString.copyFrom(qualfierB));

    Cell.Builder cellBuilderB = columnBuilderB.addCellsBuilder();
    cellBuilderB.setValue(ByteString.copyFrom(valueB));
    cellBuilderB.setTimestampMicros(anviltopTimestampB);

    Column.Builder columnBuilderA = familyBuilder.addColumnsBuilder();
    columnBuilderA.setQualifier(ByteString.copyFrom(qualfierA));

    Cell.Builder cellBuilderA = columnBuilderA.addCellsBuilder();
    cellBuilderA.setValue(ByteString.copyFrom(valueA));
    cellBuilderA.setTimestampMicros(anviltopTimestampA);

    Result result = adapter.adaptResponse(rowBuilder.build());
    Assert.assertEquals(2, result.size());

    org.apache.hadoop.hbase.Cell cellA = result.listCells().get(0);
    Assert.assertArrayEquals(Bytes.toBytes(columnFamily), CellUtil.cloneFamily(cellA));
    Assert.assertArrayEquals(qualfierA, CellUtil.cloneQualifier(cellA));
    Assert.assertArrayEquals(valueA, CellUtil.cloneValue(cellA));
    Assert.assertEquals(anviltopTimestampA / 1000L, cellA.getTimestamp());

    org.apache.hadoop.hbase.Cell cellB = result.listCells().get(1);
    Assert.assertArrayEquals(Bytes.toBytes(columnFamily), CellUtil.cloneFamily(cellB));
    Assert.assertArrayEquals(qualfierB, CellUtil.cloneQualifier(cellB));
    Assert.assertArrayEquals(valueB, CellUtil.cloneValue(cellB));
    Assert.assertEquals(anviltopTimestampB / 1000L, cellB.getTimestamp());
  }
}
