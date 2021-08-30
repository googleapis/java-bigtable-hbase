/*
 * Copyright 2015 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;
import com.google.cloud.bigtable.hbase.adapters.admin.TableAdapter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * Need this extended class as {@link TableAdapter#adapt(HTableDescriptor, CreateTableRequest)} is
 * not binary compatible with {@link TableAdapter2x#adapt(TableDescriptor, byte[][])}
 *
 * <p>Similarly, {@link ColumnDescriptorAdapter#adapt(HColumnDescriptor)} is not binary compatible
 * with {@link ColumnFamilyDescriptor}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class TableAdapter2x {

  public static CreateTableRequest adapt(TableDescriptor desc, byte[][] splitKeys) {
    return TableAdapter.adapt(new HTableDescriptor(desc), splitKeys);
  }

  public static HColumnDescriptor toHColumnDescriptor(ColumnFamilyDescriptor column) {
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(TableName.valueOf("N_A")).setColumnFamily(column).build();
    return new HTableDescriptor(desc).getColumnFamilies()[0];
  }

  private TableAdapter2x() {}

  /**
   * adapt.
   *
   * @param table a {@link Table} object.
   * @return a {@link TableDescriptor} object.
   */
  public static TableDescriptor adapt(Table table) {
    return TableAdapter.adapt(table);
  }
}
