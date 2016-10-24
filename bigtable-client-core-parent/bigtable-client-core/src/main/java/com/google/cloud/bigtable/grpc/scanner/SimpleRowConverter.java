/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.scanner;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.grpc.SimpleRow;
import com.google.cloud.bigtable.grpc.SimpleRow.SimpleColumn;
import com.google.common.collect.ImmutableList;

/**
 * <p>This class converts an instance of {@link com.google.cloud.bigtable.grpc.SimpleRow} to
 * {@link com.google.bigtable.v2.Row}.</p>
 *
 * @author tyagihas
 * @version $Id: $Id
 */
public class SimpleRowConverter {	
  public Row.Builder buildRow(SimpleRow row) {
    Row.Builder rowBuilder = Row.newBuilder();
    String prevKey = "";
    Family.Builder familyBuilder = null;
    ImmutableList<SimpleColumn> list =
      SimpleRow.FamilyColumnOrdering.DEFAULT_ORDERING.immutableSortedCopy(row.getList());

    for (int i = 0; i < list.size(); i++) {
      SimpleColumn column = list.get(i);
      if (!prevKey.equals(column.getFamily())) {
        familyBuilder = rowBuilder.addFamiliesBuilder().setName(column.getFamily());
	  }
      System.out.println(column.getFamily() + ", " + new String(column.getQualifier().toByteArray()));
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
