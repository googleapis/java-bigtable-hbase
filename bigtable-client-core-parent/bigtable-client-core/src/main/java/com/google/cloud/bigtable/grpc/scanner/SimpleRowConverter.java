package com.google.cloud.bigtable.grpc.scanner;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.grpc.SimpleRow;
import com.google.cloud.bigtable.grpc.SimpleRow.SimpleColumn;
import com.google.common.collect.ImmutableList;

public class SimpleRowConverter {	
  public Row.Builder buildRow(SimpleRow row, Row.Builder rowBuilder) {
    String prevKey = "";
    Family.Builder familyBuilder = null;
    ImmutableList<SimpleColumn> list = row.getList();
    for (int i = 0; i < list.size(); i++) {
      SimpleColumn column = list.get(i);
      if (!prevKey.equals(column.getFamily())) {
        familyBuilder = rowBuilder.addFamiliesBuilder().setName(column.getFamily());
	  }
      Column.Builder columnBuilder = Column.newBuilder().setQualifier(column.getQualifier());
      columnBuilder.addCells(Cell.newBuilder()
              .setTimestampMicros(column.getTimestamp())
              .addAllLabels(column.getLabels())
              .setValue(column.getValue())
              .build());
	  familyBuilder.addColumns(columnBuilder);
	  prevKey = column.getFamily();
    }
    return rowBuilder;
  }  
}
