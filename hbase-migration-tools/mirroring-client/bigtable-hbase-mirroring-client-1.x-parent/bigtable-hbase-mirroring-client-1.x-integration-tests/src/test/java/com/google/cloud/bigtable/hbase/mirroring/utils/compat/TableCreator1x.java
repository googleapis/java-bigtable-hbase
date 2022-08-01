/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring.utils.compat;

import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

public class TableCreator1x implements TableCreator {
  @Override
  public void createTable(Connection connection, String tableName, byte[]... columnFamilies)
      throws IOException {
    Admin admin = connection.getAdmin();

    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
    for (byte[] columnFamilyName : columnFamilies) {
      descriptor.addFamily(new HColumnDescriptor(columnFamilyName));
    }
    admin.createTable(descriptor);
  }
}
