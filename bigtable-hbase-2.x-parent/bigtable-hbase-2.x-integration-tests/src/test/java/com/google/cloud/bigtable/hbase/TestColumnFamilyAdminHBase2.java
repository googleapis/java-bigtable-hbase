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

import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;

/**
 * Tests creation and deletion of column families.
 */
public class TestColumnFamilyAdminHBase2 extends AbstractTestColumnFamilyAdmin {

  @Override
  protected void addColumn(byte[] columnName, int version) throws Exception {
    ColumnFamilyDescriptor descriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(columnName).setMaxVersions(version)
            .build();
    admin.addColumnFamily(tableName, descriptor);
  }

  @Override
  protected void modifyColumn(byte[] columnName, int version) throws Exception {
    ColumnFamilyDescriptor descriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(columnName).setMaxVersions(version)
            .build();
    admin.modifyColumnFamily(tableName, descriptor);
  }

  @Override
  protected void deleteColumn(byte[] columnName) throws Exception {
    admin.deleteColumnFamily(tableName, columnName);
  }
}
