package com.google.cloud.bigtable.hbase.adapters;

import static org.junit.Assert.*;

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.Row;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Unit tests for the {@link RowAdapter}.
 */
@RunWith(JUnit4.class)
public class TestRowAdapter {
  
  private RowAdapter instance = new RowAdapter();

  @Test
  public void adaptResponse_null() {
    assertNull(instance.adaptResponse(null).rawCells());
  }

  @Test
  public void adaptResponse_emptyRow() {
    Row row = Row.newBuilder()
        .setKey(ByteString.copyFromUtf8("key"))
        .build();
    assertEquals(0, instance.adaptResponse(row).rawCells().length);
  }

  @Test
  public void adaptResponse_oneRow() {
    String family1 = "family1";
    String family2 = "family2";
    byte[] qualifier1 = "qualifier1".getBytes();
    byte[] qualifier2 = "qualifier2".getBytes();
    byte[] value1 = "value1".getBytes();
    byte[] value2 = "value2".getBytes();
    byte[] value3 = "value3".getBytes();
    byte[] value4 = "value4".getBytes();
    byte[] value5 = "value5".getBytes();
 
    Row row = Row.newBuilder()
        .setKey(ByteString.copyFromUtf8("key"))
        .addFamilies(Family.newBuilder()
            .setName(family1)
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFrom(qualifier1))
                .addCells(Cell.newBuilder() // First cell.
                    .setTimestampMicros(54321L)
                    .setValue(ByteString.copyFrom(value1)))
                .addCells(Cell.newBuilder() // Duplicate cell.
                    .setTimestampMicros(54321L)
                    .setValue(ByteString.copyFrom(value1)))
                .addCells(Cell.newBuilder() // Same family, same column, but different timestamps.
                  .setTimestampMicros(12345L)
                  .setValue(ByteString.copyFrom(value2)))
                .addCells(Cell.newBuilder() // With label
                    .setValue(ByteString.copyFromUtf8("withLabel"))
                    .addLabels("label")))
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFrom(qualifier2))
                .addCells(Cell.newBuilder() // Same family, same timestamp, but different column.
                    .setTimestampMicros(54321L)
                    .setValue(ByteString.copyFrom(value3)))))
        .addFamilies(Family.newBuilder()
            .setName(family2)
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFrom(qualifier1))
                .addCells(Cell.newBuilder() // Same column, same timestamp, but different family.
                    .setTimestampMicros(54321L)
                    .setValue(ByteString.copyFrom(value4))))
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFrom(qualifier2))
                .addCells(Cell.newBuilder() // Same timestamp, but different family and column.
                    .setTimestampMicros(54321L)
                    .setValue(ByteString.copyFrom(value5)))))
        .build();

    Result result = instance.adaptResponse(row);
    assertEquals(5, result.rawCells().length);

    List<org.apache.hadoop.hbase.Cell> cells1 =
        result.getColumnCells(family1.getBytes(), qualifier1);
    assertEquals(2, cells1.size());
    assertEquals(Bytes.toString(value1), Bytes.toString(CellUtil.cloneValue(cells1.get(0))));
    assertEquals(Bytes.toString(value2), Bytes.toString(CellUtil.cloneValue(cells1.get(1))));

    List<org.apache.hadoop.hbase.Cell> cells2 =
        result.getColumnCells(family1.getBytes(), qualifier2);
    assertEquals(1, cells2.size());
    assertEquals(Bytes.toString(value3), Bytes.toString(CellUtil.cloneValue(cells2.get(0))));

    List<org.apache.hadoop.hbase.Cell> cells3 =
        result.getColumnCells(family2.getBytes(), qualifier1);
    assertEquals(1, cells3.size());
    assertEquals(Bytes.toString(value4), Bytes.toString(CellUtil.cloneValue(cells3.get(0))));

    List<org.apache.hadoop.hbase.Cell> cells4 =
        result.getColumnCells(family2.getBytes(), qualifier2);
    assertEquals(1, cells4.size());
    assertEquals(Bytes.toString(value5), Bytes.toString(CellUtil.cloneValue(cells4.get(0))));
  }
}

