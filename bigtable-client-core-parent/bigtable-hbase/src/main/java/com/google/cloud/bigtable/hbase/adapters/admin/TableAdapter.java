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
package com.google.cloud.bigtable.hbase.adapters.admin;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * <p>TableAdapter class.</p>
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class TableAdapter {
  protected final BigtableInstanceName bigtableInstanceName;
  protected final ColumnDescriptorAdapter columnDescriptorAdapter;

  /**
   * <p>Constructor for TableAdapter.</p>
   *
   * @param bigtableInstanceName a {@link BigtableInstanceName} object.
   * @param columnDescriptorAdapter a {@link ColumnDescriptorAdapter} object.
   */
  public TableAdapter(BigtableInstanceName bigtableInstanceName,
      ColumnDescriptorAdapter columnDescriptorAdapter) {
    this.bigtableInstanceName = Preconditions
        .checkNotNull(bigtableInstanceName, "bigtableInstanceName cannot be null.");
    this.columnDescriptorAdapter = columnDescriptorAdapter;
  }

  /**
   * <p>adapt.</p>
   *
   * @param desc a {@link org.apache.hadoop.hbase.HTableDescriptor} object.
   * @return a {@link com.google.bigtable.admin.v2.Table} object.
   */
  protected Table adapt(HTableDescriptor desc) {
    Map<String, ColumnFamily> columnFamilies = new HashMap<>();
    for (HColumnDescriptor column : desc.getColumnFamilies()) {
      String columnName = column.getNameAsString();
      ColumnFamily columnFamily = columnDescriptorAdapter.adapt(column).build();
      columnFamilies.put(columnName, columnFamily);
    }
    return Table.newBuilder().putAllColumnFamilies(columnFamilies).build();
  }


  public CreateTableRequest.Builder adapt(HTableDescriptor desc, byte[][] splitKeys) {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setTableId(desc.getTableName().getQualifierAsString());
    builder.setTable(adapt(desc));
    addSplitKeys(builder, splitKeys);
    return builder;
  }

  public static void addSplitKeys(CreateTableRequest.Builder builder, byte[][] splitKeys) {
    if (splitKeys != null) {
      for (byte[] splitKey : splitKeys) {
        builder.addInitialSplits(
            CreateTableRequest.Split.newBuilder().setKey(ByteString.copyFrom(splitKey)).build());
      }
    }
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
    for (Entry<String, ColumnFamily> entry : table.getColumnFamiliesMap().entrySet()) {
      tableDescriptor.addFamily(columnDescriptorAdapter.adapt(entry.getKey(), entry.getValue()));
    }
    return tableDescriptor;
  }
}
