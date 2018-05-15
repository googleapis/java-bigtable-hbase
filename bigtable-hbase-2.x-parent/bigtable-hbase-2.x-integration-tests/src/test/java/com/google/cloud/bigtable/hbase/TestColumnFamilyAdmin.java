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
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests creation and deletion of column families.
 */
public class TestColumnFamilyAdmin extends AbstractTestColumnFamilyAdmin {
  
  @Before
  public void setup() throws IOException {
    admin = getConnection().getAdmin();
    tableName = sharedTestEnv.newTestTableName();

    descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    descriptor.addFamily(new HColumnDescriptor(DELETE_COLUMN_FAMILY));
    admin.createTable(descriptor);
  }

  @After
  public void tearDown() throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }
  
  @Test
  public void testAddAndCompareColumn() throws IOException {
    HColumnDescriptor newColumn = new HColumnDescriptor("NEW_COLUMN");
    admin.addColumn(tableName, newColumn);

    HTableDescriptor expectedDescriptor = new HTableDescriptor(descriptor);
    expectedDescriptor.addFamily(newColumn);

    HTableDescriptor retrievedDescriptor = admin.getTableDescriptor(tableName);
    Assert.assertEquals(expectedDescriptor, retrievedDescriptor);
  }

  @Test
  public void testModifyColumnFamilyAsync() throws Exception {
    HColumnDescriptor newColumn = new HColumnDescriptor("MODIFY_COLUMN");
    newColumn.setMaxVersions(2);
    admin.addColumnFamilyAsync(tableName, newColumn).get();

    HTableDescriptor expectedDescriptor = new HTableDescriptor(descriptor);
    expectedDescriptor.addFamily(newColumn);

    HTableDescriptor retrievedDescriptor = admin.getTableDescriptor(tableName);
    Assert.assertEquals(expectedDescriptor, retrievedDescriptor);

    newColumn.setMaxVersions(100);
    admin.modifyColumnFamilyAsync(tableName, newColumn).get();

    expectedDescriptor = new HTableDescriptor(descriptor);
    expectedDescriptor.addFamily(newColumn);

    retrievedDescriptor = admin.getTableDescriptor(tableName);
    Assert.assertEquals(expectedDescriptor, retrievedDescriptor);

  }

  @Test
  public void testAddAndCompareColumnFamily() throws IOException {
    HColumnDescriptor newColumn = new HColumnDescriptor("NEW_COLUMN");
    admin.addColumnFamily(tableName, newColumn);

    HTableDescriptor expectedDescriptor = new HTableDescriptor(descriptor);
    expectedDescriptor.addFamily(newColumn);

    HTableDescriptor retrievedDescriptor = admin.getTableDescriptor(tableName);
    Assert.assertEquals(expectedDescriptor, retrievedDescriptor);
  }
  
  @Test
  public void testDeleteColumnFamily() throws IOException {
    //Count column before delete.
    TableDescriptor tblDesc = admin.getDescriptor(tableName);
    ColumnFamilyDescriptor colDescArr[] = tblDesc.getColumnFamilies();
    assertEquals(2, colDescArr.length);
    assertEquals(Bytes.toString(COLUMN_FAMILY), colDescArr[0].getNameAsString());
    assertEquals(Bytes.toString(DELETE_COLUMN_FAMILY), colDescArr[1].getNameAsString());
    
    admin.deleteColumnFamily(tableName, DELETE_COLUMN_FAMILY);
    
    //count column after delete.
    tblDesc = admin.getDescriptor(tableName);
    ColumnFamilyDescriptor colDescArrDel[] = tblDesc.getColumnFamilies();
    assertEquals(1, colDescArrDel.length);
    assertEquals(Bytes.toString(COLUMN_FAMILY), colDescArrDel[0].getNameAsString());
  }
  
  @Test
  public void testDeleteColumnFamilyAsync() throws Exception {
  //Count column before delete.
    TableDescriptor tblDesc = admin.getDescriptor(tableName);
    ColumnFamilyDescriptor colDescArr[] = tblDesc.getColumnFamilies();
    assertEquals(2, colDescArr.length);
    assertEquals(Bytes.toString(COLUMN_FAMILY), colDescArr[0].getNameAsString());
    assertEquals(Bytes.toString(DELETE_COLUMN_FAMILY), colDescArr[1].getNameAsString());
    
    admin.deleteColumnFamilyAsync(tableName, DELETE_COLUMN_FAMILY).get();
    
  //count column after delete.
    tblDesc = admin.getDescriptor(tableName);
    ColumnFamilyDescriptor colDescArrDel[] = tblDesc.getColumnFamilies();
    assertEquals(1, colDescArrDel.length);
    assertEquals(Bytes.toString(COLUMN_FAMILY), colDescArrDel[0].getNameAsString());
  }
  
  @Override
  protected void addColumn(String colmnName) throws Exception {
    HColumnDescriptor newColumn = new HColumnDescriptor(colmnName);
    admin.addColumn(tableName, newColumn);
  }
  
  @Override
  protected void addColumn(String colmnName, int version) throws Exception {
    HColumnDescriptor newColumn = new HColumnDescriptor(colmnName);
    newColumn.setMaxVersions(version);
    admin.addColumn(tableName, newColumn);
  }
  
  @Override
  protected void modifyColumn(String columnName, int version) throws Exception {
    HColumnDescriptor newColumn = new HColumnDescriptor(columnName);
    newColumn.setMaxVersions(version);
    admin.modifyColumn(tableName, newColumn);
  }

  @Override
  protected void deleteColumn() throws Exception {
    admin.deleteColumnFamily(tableName, DELETE_COLUMN_FAMILY);
  }
}
