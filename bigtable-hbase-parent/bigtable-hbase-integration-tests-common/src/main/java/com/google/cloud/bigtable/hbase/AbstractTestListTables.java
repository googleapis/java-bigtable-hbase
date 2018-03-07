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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public abstract class AbstractTestListTables extends AbstractTest {

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
  
  private void addTable(TableName tableName) {
    tablesToDelete.add(tableName);
  }

  private void removeTable(TableName tableName) {
    tablesToDelete.remove(tableName);
  }

  /**
   * @throws IOException
   */
  @Test
  public void testTableNames() throws Exception {
    try (Admin admin = getConnection().getAdmin()) {
      TableName tableName1 = TableName.valueOf("list_table1-" + UUID.randomUUID().toString());
      TableName tableName2 = TableName.valueOf("list_table2-" + UUID.randomUUID().toString());

      Assert.assertFalse(admin.tableExists(tableName1));
      Assert.assertFalse(listTableNames(admin).contains(tableName1));

      sharedTestEnv.createTable(tableName1);
      addTable(tableName1);
      checkColumnFamilies(admin, tableName1);

      {
        Assert.assertTrue(admin.tableExists(tableName1));
        Assert.assertFalse(admin.tableExists(tableName2));
        List<TableName> tableList = listTableNames(admin);
        Assert.assertTrue(tableList.contains(tableName1));
        Assert.assertFalse(tableList.contains(tableName2));
      }

      sharedTestEnv.createTable(tableName2);
      addTable(tableName2);
      checkColumnFamilies(admin, tableName2);

      {
        Assert.assertTrue(admin.tableExists(tableName1));
        Assert.assertTrue(admin.tableExists(tableName2));
        List<TableName> tableList = listTableNames(admin);
        Assert.assertTrue(tableList.contains(tableName1));
        Assert.assertTrue(tableList.contains(tableName2));
      }

      {
        List<TableName> tableList = listTableNames(admin, Pattern.compile("list_table1-.*"));
        Assert.assertTrue(tableList.contains(tableName1));
        Assert.assertFalse(tableList.contains(tableName2));
      }
      
      {
        List<TableName> tableList = 
            listTableNamesUsingDescriptors(admin, Pattern.compile("list_table1-.*"));
        Assert.assertTrue(tableList.contains(tableName1));
        Assert.assertFalse(tableList.contains(tableName2));
        Assert.assertEquals(1, tableList.size());
      }

      {
        List<TableName> tableList = 
            listTableNamesUsingDescriptors(admin, Collections.singletonList(tableName2));
        Assert.assertFalse(tableList.contains(tableName1));
        Assert.assertTrue(tableList.contains(tableName2));
        Assert.assertEquals(1, tableList.size());
      }
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    try (Admin admin = getConnection().getAdmin()) {
      TableName tableName1 = TableName.valueOf("del_table1-" + UUID.randomUUID().toString());
      TableName tableName2 = TableName.valueOf("del_table2-" + UUID.randomUUID().toString());

      Assert.assertFalse(admin.tableExists(tableName1));
      Assert.assertFalse(listTableNames(admin).contains(tableName1));
      sharedTestEnv.createTable(tableName1);
      addTable(tableName1);
      Assert.assertTrue(admin.tableExists(tableName1));
      
      Assert.assertFalse(admin.tableExists(tableName2));
      Assert.assertFalse(listTableNames(admin).contains(tableName2));
      sharedTestEnv.createTable(tableName2);
      addTable(tableName2);
      Assert.assertTrue(admin.tableExists(tableName2));
      
      {
        List<TableName> tableList = listTableNames(admin);
        Assert.assertTrue(tableList.contains(tableName1));
        Assert.assertTrue(tableList.contains(tableName2));
      }
     
      deleteTable(admin,tableName2);
      removeTable(tableName2);
      {
        Assert.assertTrue(admin.tableExists(tableName1));
        Assert.assertFalse(admin.tableExists(tableName2));
        List<TableName> tableList = listTableNames(admin);
        Assert.assertTrue(tableList.contains(tableName1));
        Assert.assertFalse(tableList.contains(tableName2));
      }

      deleteTable(admin,tableName1);
      removeTable(tableName1);
      {
        Assert.assertFalse(admin.tableExists(tableName1));
        Assert.assertFalse(admin.tableExists(tableName2));
        List<TableName> tableList = listTableNames(admin);
        Assert.assertFalse(tableList.contains(tableName1));
        Assert.assertFalse(tableList.contains(tableName2));
      }
    }
  }

  @Test
  public void testNotFound() throws IOException {
    thrown.expect(TableNotFoundException.class);
    try (Admin admin = getConnection().getAdmin()) {
      TableName nonExistantTableName =
          TableName.valueOf("NA_table2-" + UUID.randomUUID().toString());
      checkTableDescriptor(admin, nonExistantTableName);
    }
  }
  
  protected abstract void checkColumnFamilies(Admin admin, TableName tableName) 
      throws TableNotFoundException,IOException;

  protected void deleteTable(Admin admin, TableName tableName) throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  protected final List<TableName> listTableNames(Admin admin) throws IOException {
    return Arrays.asList(admin.listTableNames());
  }

  protected final List<TableName> listTableNames(Admin admin, Pattern pattern) throws IOException {
    return Arrays.asList(admin.listTableNames(pattern));
  }

  protected abstract List<TableName> listTableNamesUsingDescriptors(Admin admin, Pattern pattern)
      throws IOException;
  
  protected abstract List<TableName> listTableNamesUsingDescriptors(Admin admin, 
      List<TableName> tableNames) throws IOException;
  
  protected abstract void checkTableDescriptor(Admin admin, TableName tableName)
      throws TableNotFoundException, IOException;
}
