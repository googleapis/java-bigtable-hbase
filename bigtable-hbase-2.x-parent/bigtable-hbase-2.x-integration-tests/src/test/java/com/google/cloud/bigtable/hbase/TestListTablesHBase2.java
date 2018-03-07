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
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestListTablesHBase2 extends AbstractTestListTables {

  private boolean enableAsyncDelete = false;
  
  @Before
  public void setup()
  {
    enableAsyncDelete = false;
  }

  @Test
  public void testDeleteTableAsync() throws Exception
  {
    enableAsyncDelete = true;
    testDeleteTable();
  }
  
  @Override
  protected void checkColumnFamilies(Admin admin, TableName tableName) 
      throws TableNotFoundException,IOException {
    HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
    HColumnDescriptor[] columnFamilies = descriptor.getColumnFamilies();
    Assert.assertEquals(2, columnFamilies.length);
    Assert.assertEquals(Bytes.toString(COLUMN_FAMILY), columnFamilies[0].getNameAsString());
  }
  
  @Override
  protected void deleteTable(Admin admin, TableName tableName) throws Exception {
    if (enableAsyncDelete && sharedTestEnv.isBigtable()) {
      admin.disableTableAsync(tableName).get();
      admin.deleteTableAsync(tableName).get();
    } else {
      super.deleteTable(admin, tableName);
    }
  }


  @Override
  protected List<TableName> listTableNamesUsingDescriptors(Admin admin, Pattern pattern) 
      throws IOException {
    return toTableNames(admin.listTableDescriptors(pattern));
  }

  @Override
  protected List<TableName> listTableNamesUsingDescriptors(Admin admin, List<TableName> tableNames) 
      throws IOException {
    return toTableNames(admin.listTableDescriptors(tableNames));
  }

  @Override
  protected void checkTableDescriptor(Admin admin, TableName tableName)
      throws TableNotFoundException, IOException {
    admin.getDescriptor(tableName);
  }
  
  private List<TableName> toTableNames(List<TableDescriptor> descriptors) {
    List<TableName> tableNames = new ArrayList<TableName>();
    for (TableDescriptor descriptor : descriptors) {
      tableNames.add(descriptor.getTableName());
    }
    return tableNames;
  }
}
