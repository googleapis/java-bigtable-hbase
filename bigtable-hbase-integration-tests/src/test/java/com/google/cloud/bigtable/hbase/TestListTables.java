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

import static com.google.cloud.bigtable.hbase.IntegrationTests.COLUMN_FAMILY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestListTables extends AbstractTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private List<TableName> tablesToDelete = new ArrayList<>();

  @After
  public void deleteTables() throws IOException{
    try (Admin admin = getConnection().getAdmin()) {
      for (TableName tableName : tablesToDelete) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    }
  }

  /**
   * @throws IOException
   */
  @Test
  public void testTableNames() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      TableName tableName1 = TableName.valueOf("list_table1-" + UUID.randomUUID().toString());
      TableName tableName2 = TableName.valueOf("list_table2-" + UUID.randomUUID().toString());

      Assert.assertFalse(admin.tableExists(tableName1));
      Assert.assertFalse(ArrayUtils.contains(admin.listTableNames(), tableName1));

      createTable(admin, tableName1);

      {
        Assert.assertTrue(admin.tableExists(tableName1));
        Assert.assertFalse(admin.tableExists(tableName2));
        TableName[] tableList = admin.listTableNames();
        Assert.assertTrue(ArrayUtils.contains(tableList, tableName1));
        Assert.assertFalse(ArrayUtils.contains(tableList, tableName2));
      }

      checkColumnFamilies(admin, tableName1);
      createTable(admin, tableName2);

      {
        Assert.assertTrue(admin.tableExists(tableName1));
        Assert.assertTrue(admin.tableExists(tableName2));
        TableName[] tableList = admin.listTableNames();
        Assert.assertTrue(ArrayUtils.contains(tableList, tableName1));
        Assert.assertTrue(ArrayUtils.contains(tableList, tableName2));
      }

      checkColumnFamilies(admin, tableName2);
    }
  }

  @Test
  public void testNotFound() throws IOException {
    thrown.expect(TableNotFoundException.class);
    try (Admin admin = getConnection().getAdmin()) {
      TableName nonExistantTableName =
          TableName.valueOf("list_table2-" + UUID.randomUUID().toString());
      admin.getTableDescriptor(nonExistantTableName);
    }
  }

  private void checkColumnFamilies(Admin admin, TableName tableName) throws TableNotFoundException,
      IOException {
    HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
    HColumnDescriptor[] columnFamilies = descriptor.getColumnFamilies();
    Assert.assertEquals(1, columnFamilies.length);
    Assert.assertEquals(Bytes.toString(COLUMN_FAMILY), columnFamilies[0].getNameAsString());
  }

  private void createTable(Admin admin, TableName tableName) throws IOException {
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    tablesToDelete.add(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    admin.createTable(descriptor);
  }
}
