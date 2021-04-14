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

import static com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter.buildGarbageCollectionRule;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules.GCRule;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

/**
 * TableAdapter class.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class TableAdapter {
  private static final ColumnDescriptorAdapter columnDescriptorAdapter =
      ColumnDescriptorAdapter.INSTANCE;

  /**
   * adapt. This method adapts ColumnFamily to CreateTableRequest.
   *
   * @param desc a {@link HTableDescriptor} object.
   * @param request a {@link CreateTableRequest}
   */
  protected static void adapt(HTableDescriptor desc, CreateTableRequest request) {
    if (request != null) {
      for (HColumnDescriptor column : desc.getColumnFamilies()) {
        String columnName = column.getNameAsString();
        GCRule gcRule = buildGarbageCollectionRule(column);
        if (gcRule == null) {
          request.addFamily(columnName);
        } else {
          request.addFamily(columnName, gcRule);
        }
      }
    }
  }

  public static CreateTableRequest adapt(HTableDescriptor desc, byte[][] splitKeys) {
    CreateTableRequest request = CreateTableRequest.of(desc.getTableName().getNameAsString());
    adapt(desc, request);
    addSplitKeys(splitKeys, request);
    return request;
  }

  public static void addSplitKeys(byte[][] splitKeys, CreateTableRequest request) {
    if (splitKeys != null) {
      for (byte[] splitKey : splitKeys) {
        request.addSplit(ByteString.copyFrom(splitKey));
      }
    }
  }

  private TableAdapter() {}

  /**
   * adapt.
   *
   * @param table a {@link Table} object.
   * @return a {@link HTableDescriptor} object.
   */
  public static HTableDescriptor adapt(Table table) {
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(table.getId()));
    for (ColumnFamily columnFamily : table.getColumnFamilies()) {
      tableDescriptor.addFamily(columnDescriptorAdapter.adapt(columnFamily));
    }
    return tableDescriptor;
  }
}
