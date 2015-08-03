/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.admin.table.v1.ColumnFamily;
import com.google.bigtable.admin.table.v1.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.naming.BigtableClusterName;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.util.Map;
import java.util.Map.Entry;

public class TableAdapter {

  private final BigtableClusterName bigtableClusterName;
  private final ColumnDescriptorAdapter columnDescriptorAdapter;

  public TableAdapter(BigtableOptions options, ColumnDescriptorAdapter columnDescriptorAdapter) {
    this.bigtableClusterName = options.getClusterName();
    this.columnDescriptorAdapter = columnDescriptorAdapter;
  }

  public Table adapt(HTableDescriptor desc) {
    Table.Builder tableBuilder = Table.newBuilder();
    Map<String, ColumnFamily> columnFamilies = tableBuilder.getMutableColumnFamilies();
    for (HColumnDescriptor column : desc.getColumnFamilies()) {
      ColumnFamily columnFamily = columnDescriptorAdapter.adapt(column).build();
      String columnName = column.getNameAsString();
      columnFamilies.put(columnName, columnFamily);
    }
    return tableBuilder.build();
  }

  public HTableDescriptor adapt(Table table) {
    String tableId = bigtableClusterName.toTableId(table.getName());
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableId));
    for (Entry<String, ColumnFamily> entry : table.getColumnFamilies().entrySet()) {
      tableDescriptor.addFamily(columnDescriptorAdapter.adapt(entry.getKey(), entry.getValue()));
    }
    return tableDescriptor;
  }
}
