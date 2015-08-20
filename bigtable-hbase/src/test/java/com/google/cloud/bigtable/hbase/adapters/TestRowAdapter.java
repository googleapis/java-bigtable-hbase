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
    String family = "family";
    byte[] qualifier = "qualifier".getBytes();
    byte[] value = "noLabel".getBytes();
 
    Row row = Row.newBuilder()
        .setKey(ByteString.copyFromUtf8("key"))
        .addFamilies(Family.newBuilder()
            .setName(family)
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFrom(qualifier))
                .addCells(Cell.newBuilder()
                    .setValue(ByteString.copyFrom(value)))
                .addCells(Cell.newBuilder()
                    .setValue(ByteString.copyFromUtf8("withLabel"))
                    .addLabels("label"))))
        .build();
    Result result = instance.adaptResponse(row);
    assertEquals(1, result.rawCells().length);
    List<org.apache.hadoop.hbase.Cell> cells = result.getColumnCells(family.getBytes(), qualifier);
    assertEquals(1, cells.size());
    assertEquals(Bytes.toString(value), Bytes.toString(CellUtil.cloneValue(cells.get(0))));
  }
}

