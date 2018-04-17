/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

/**
 * Tests creation and deletion of column families.
 */
public abstract class AbstractTestColumnFamilyAdmin extends AbstractTest {
  protected static byte[] DELETE_COLUMN_FAMILY =
      Bytes.toBytes(Bytes.toString(COLUMN_FAMILY) + "_remove");

  protected Admin admin;
  protected TableName tableName;
  protected HTableDescriptor descriptor;
  
  @Test
  public void testCreateTableFull() throws IOException {
    HTableDescriptor retrievedDescriptor = admin.getTableDescriptor(tableName);
    Assert.assertEquals(descriptor, retrievedDescriptor);
  }
  
  @Test
  public void testAddColumn() throws Exception {
    addColumn("NEW_COLUMN");
    HTableDescriptor tblDesc = admin.getTableDescriptor(tableName);
    HColumnDescriptor colDesc = tblDesc.getFamily(Bytes.toBytes("NEW_COLUMN"));
    String retrievedColumnName = colDesc.getNameAsString();
    assertEquals("NEW_COLUMN", retrievedColumnName);
  }
  
  @Test
  public void testModifyColumnFamily() throws Exception {
    addColumn("MODIFY_COLUMN", 2);

    HTableDescriptor tblDesc = admin.getTableDescriptor(tableName);
    HColumnDescriptor colDesc = tblDesc.getFamily(Bytes.toBytes("MODIFY_COLUMN"));
    assertEquals(2, colDesc.getMaxVersions());

    modifyColumn("MODIFY_COLUMN", 100);
    
    tblDesc = admin.getTableDescriptor(tableName);
    colDesc = tblDesc.getFamily(Bytes.toBytes("MODIFY_COLUMN"));
    assertEquals(100, colDesc.getMaxVersions());

  }
  
  @Test
  public void testRemoveColumn() throws Exception {
    deleteColumn();
    HTableDescriptor retrievedDescriptor = admin.getTableDescriptor(tableName);
    HTableDescriptor expectedDescriptor = new HTableDescriptor(tableName);
    expectedDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

    Assert.assertEquals(expectedDescriptor, retrievedDescriptor);
  }
  
  protected abstract void addColumn(String columnName) throws Exception;
  protected abstract void addColumn(String columnName, int version) throws Exception;
  protected abstract void modifyColumn(String newColumn, int version) throws Exception;
  protected abstract void deleteColumn() throws Exception;
}
