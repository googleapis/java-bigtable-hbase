/*
 * Copyright 2018 Google LLC All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;

/** Tests creation and deletion of column families. */
public class TestColumnFamilyAdminAsync extends AbstractTestColumnFamilyAdmin {
  @Override
  protected HTableDescriptor getTableDescriptor(TableName tableName) throws Exception {
    return new HTableDescriptor(admin.getDescriptor(tableName));
  }

  @Override
  protected void addColumn(byte[] columnName, int version) throws Exception {
    admin
        .addColumnFamilyAsync(tableName, createFamilyDescriptor(columnName, version))
        .get(1, TimeUnit.MINUTES);
  }

  @Override
  protected void modifyColumn(byte[] columnName, int version) throws Exception {
    admin
        .modifyColumnFamilyAsync(tableName, createFamilyDescriptor(columnName, version))
        .get(1, TimeUnit.MINUTES);
  }

  @Override
  protected void deleteColumn(byte[] columnName) throws Exception {
    admin.deleteColumnFamilyAsync(tableName, DELETE_COLUMN_FAMILY).get(1, TimeUnit.MINUTES);
  }

  private ColumnFamilyDescriptor createFamilyDescriptor(byte[] columnName, int version) {
    return ColumnFamilyDescriptorBuilder.newBuilder(columnName).setMaxVersions(version).build();
  }
}
