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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

public class TestListTables extends AbstractTestListTables {

  @Override
  protected void checkColumnFamilies(Admin admin, TableName tableName) 
      throws TableNotFoundException,IOException {
    HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
    HColumnDescriptor[] columnFamilies = descriptor.getColumnFamilies();
    Assert.assertEquals(2, columnFamilies.length);
    Assert.assertEquals(Bytes.toString(COLUMN_FAMILY), columnFamilies[0].getNameAsString());
  }
  
  @Override
  protected void createTable(Admin admin, TableName tableName) throws IOException {
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    admin.createTable(descriptor);
  }
  
  @Override
  protected void checkTableDescriptor(Admin admin, TableName tableName) 
      throws TableNotFoundException, IOException {
    admin.getTableDescriptor(tableName);
  }

  @Override
  protected List<TableName> listTableNamesUsingDescriptors(Admin admin, Pattern pattern) throws IOException {
    return toTableNames(admin.listTables(pattern));
  }

  @Override
  protected List<TableName> listTableNamesUsingDescriptors(Admin admin, List<TableName> tableNames) throws IOException {
    return toTableNames(admin.getTableDescriptorsByTableName(tableNames));
  }
  
  private List<TableName> toTableNames(HTableDescriptor[] descriptors)
  {
    List<TableName> tableList = new ArrayList<TableName>();
    for (HTableDescriptor descriptor : descriptors) {
      tableList.add(descriptor.getTableName());
    }
    return tableList;
  }
}
