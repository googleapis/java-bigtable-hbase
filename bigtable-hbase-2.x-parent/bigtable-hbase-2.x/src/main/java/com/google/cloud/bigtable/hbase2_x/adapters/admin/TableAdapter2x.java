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

import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;
import com.google.cloud.bigtable.hbase.adapters.admin.TableAdapter;

/**
 * Need this extended class as {@link TableAdapter#adapt(CreateTableRequest , org.apache.hadoop.hbase.HTableDescriptor)}
 * is not binary compatible with {@link TableAdapter2x#adapt(TableDescriptor)}
 * 
 * Similarly, {@link ColumnDescriptorAdapter#adapt(HColumnDescriptor)} is not binary compatible with
 * {@link ColumnFamilyDescriptor}.
 * 
 * @author spollapally
 */
public class TableAdapter2x {
  protected static final ColumnDescriptorAdapter columnDescriptorAdapter = new ColumnDescriptorAdapter();

  public static CreateTableRequest adapt(TableDescriptor desc, byte[][] splitKeys) {
    return TableAdapter.adapt(new HTableDescriptor(desc), splitKeys);
  }

  public static CreateTableRequest adapt(TableDescriptor desc) {
    return adapt(new HTableDescriptor(desc), null);
  }

  public static HColumnDescriptor toHColumnDescriptor(ColumnFamilyDescriptor column) {
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(TableName.valueOf("N_A")).setColumnFamily(column)
            .build();
    return new HTableDescriptor(desc).getColumnFamilies()[0];
  }

  protected final BigtableInstanceName bigtableInstanceName;

  public TableAdapter2x(BigtableOptions options) {
    bigtableInstanceName = options.getInstanceName();
  }

  /**
   * <p>adapt.</p>
   *
   * @param table a {@link com.google.bigtable.admin.v2.Table} object.
   * @return a {@link TableDescriptor} object.
   */
  public TableDescriptor adapt(Table table) {
    return new TableAdapter(bigtableInstanceName).adapt(table);
  }
}
