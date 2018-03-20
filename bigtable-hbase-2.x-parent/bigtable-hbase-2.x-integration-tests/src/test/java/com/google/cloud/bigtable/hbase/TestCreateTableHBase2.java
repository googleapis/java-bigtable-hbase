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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

@SuppressWarnings("deprecation")
public class TestCreateTableHBase2 extends AbstractTestCreateTable {

  @Override
  protected void createTable(TableName tableName) throws IOException {
    getConnection().getAdmin().createTable(createDescriptor(tableName));
  }

  @Override
  protected void createTable(TableName tableName, byte[] start, byte[] end,
      int splitCount)
      throws IOException {
    getConnection().getAdmin().createTable(createDescriptor(tableName), start, end, splitCount);
  }

  @Override
  protected void createTable(TableName tableName, byte[][] ranges) throws IOException {
    getConnection().getAdmin().createTable(createDescriptor(tableName), ranges);
  }

  private TableDescriptor createDescriptor(TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build())
        .build();
  }

}
