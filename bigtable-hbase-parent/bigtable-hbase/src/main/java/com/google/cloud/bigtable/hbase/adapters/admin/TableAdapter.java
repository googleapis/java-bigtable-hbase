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
package com.google.cloud.bigtable.hbase.adapters.admin;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.util.Map;
import java.util.Map.Entry;

/**
 * <p>TableAdapter class.</p>
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class TableAdapter {
  private final BigtableInstanceName bigtableInstanceName;
  private final ColumnDescriptorAdapter columnDescriptorAdapter;

  /**
   * <p>Constructor for TableAdapter.</p>
   *
   * @param options a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   * @param columnDescriptorAdapter a {@link com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter} object.
   */
  public TableAdapter(BigtableOptions options, ColumnDescriptorAdapter columnDescriptorAdapter) {
    this.bigtableInstanceName = options.getInstanceName();
    this.columnDescriptorAdapter = columnDescriptorAdapter;
  }

  /**
   * <p>adapt.</p>
   *
   * @param desc a {@link org.apache.hadoop.hbase.HTableDescriptor} object.
   * @return a {@link com.google.bigtable.admin.v2.Table} object.
   */
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

  /**
   * <p>adapt.</p>
   *
   * @param table a {@link com.google.bigtable.admin.v2.Table} object.
   * @return a {@link org.apache.hadoop.hbase.HTableDescriptor} object.
   */
  public HTableDescriptor adapt(Table table) {
    String tableId = bigtableInstanceName.toTableId(table.getName());
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableId));
    for (Entry<String, ColumnFamily> entry : table.getColumnFamilies().entrySet()) {
      tableDescriptor.addFamily(columnDescriptorAdapter.adapt(entry.getKey(), entry.getValue()));
    }
    return tableDescriptor;
  }
}
