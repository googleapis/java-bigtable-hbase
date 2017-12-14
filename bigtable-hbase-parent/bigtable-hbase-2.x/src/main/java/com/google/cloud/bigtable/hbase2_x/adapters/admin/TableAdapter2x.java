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
package com.google.cloud.bigtable.hbase2_x.adapters.admin;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.adapters.admin.TableAdapter;

/**
 * Temporary copy to get things going. Cannot use existing adopter due to binary incompatibility.
 * Needs a better re-factoring 
 * 
 * @author spollapally
 */
public class TableAdapter2x extends TableAdapter {
  private final ColumnDescriptorAdapter2x columnDescriptorAdapter2x;

  public TableAdapter2x(BigtableOptions options,
      ColumnDescriptorAdapter2x columnDescriptorAdapter2x) {
    super(options, columnDescriptorAdapter2x);
    this.columnDescriptorAdapter2x = columnDescriptorAdapter2x;
  }

  public Table adapt(TableDescriptor desc) {
    Map<String, ColumnFamily> columnFamilies = new HashMap<>();
    for (ColumnFamilyDescriptor column : desc.getColumnFamilies()) {
      String columnName = column.getNameAsString();
      ColumnFamily columnFamily = columnDescriptorAdapter2x.adapt(column).build();
      columnFamilies.put(columnName, columnFamily);
    }
    return Table.newBuilder().putAllColumnFamilies(columnFamilies).build();
  }
}
