/*
 * Copyright 2018 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.async;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.bigtable.hbase.AbstractTestColumnFamilyAdmin;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

public class TestAsyncColumnFamily extends AbstractTestColumnFamilyAdmin{
  private AsyncAdmin asyncAdmin;
  
  @Before
  public void setup() throws Exception {
    admin = getConnection().getAdmin();
    asyncAdmin = getAsyncAdmin();
    tableName = sharedTestEnv.newTestTableName();

    descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    descriptor.addFamily(new HColumnDescriptor(DELETE_COLUMN_FAMILY));
    asyncAdmin.createTable(descriptor).get();
  }

  @After
  public void tearDown() throws IOException {
    asyncAdmin.disableTable(tableName);
    asyncAdmin.deleteTable(tableName);
  }
  
  @Test
  public void testModifyColumnFamily() throws Exception {
    checkIfColumnCreated();
    ColumnFamilyDescriptorBuilder descBuild = 
        ColumnFamilyDescriptorBuilder.newBuilder(SharedTestEnvRule.COLUMN_FAMILY);
    
    descBuild.setMaxVersions(1);
    ColumnFamilyDescriptor colDesc = descBuild.build();
    asyncAdmin.modifyColumnFamily(tableName, colDesc).get();
    
    TableDescriptor tblDesc = asyncAdmin.getDescriptor(tableName).get();
    ColumnFamilyDescriptor colFamilyDesc = tblDesc.getColumnFamily(SharedTestEnvRule.COLUMN_FAMILY);
    assertEquals(1, colFamilyDesc.getMaxVersions());
    
    ColumnFamilyDescriptorBuilder descBuild1 = 
        ColumnFamilyDescriptorBuilder.newBuilder(SharedTestEnvRule.COLUMN_FAMILY);
    
    descBuild1.setMaxVersions(5);
    ColumnFamilyDescriptor colDesc1 = descBuild1.build();
    asyncAdmin.modifyColumnFamily(tableName, colDesc1).get();
    
    tblDesc = asyncAdmin.getDescriptor(tableName).get();
    colFamilyDesc = tblDesc.getColumnFamily(SharedTestEnvRule.COLUMN_FAMILY);
    assertEquals(5, colFamilyDesc.getMaxVersions());
    
    ColumnFamilyDescriptor colDescArr[] = tblDesc.getColumnFamilies();
    assertEquals(2, colDescArr.length);
    assertEquals(Bytes.toString(COLUMN_FAMILY), colDescArr[0].getNameAsString());
    assertEquals(Bytes.toString(DELETE_COLUMN_FAMILY), colDescArr[1].getNameAsString());
  }
  
  @Test
  public void testDeleteColumnFamily() throws Exception {
    checkIfColumnCreated();
    
    asyncAdmin.deleteColumnFamily(tableName, 
        DELETE_COLUMN_FAMILY).get();
    TableDescriptor tblDesc = asyncAdmin.getDescriptor(tableName).get();
    ColumnFamilyDescriptor colDescArr[] = tblDesc.getColumnFamilies();
    assertEquals(1, colDescArr.length);
    assertEquals(Bytes.toString(COLUMN_FAMILY), colDescArr[0].getNameAsString());
  }
  
  private void checkIfColumnCreated() throws Exception {
    TableDescriptor tblDesc = asyncAdmin.getDescriptor(tableName).get();
    ColumnFamilyDescriptor colDescArr[] = tblDesc.getColumnFamilies();
    assertEquals(2, colDescArr.length);
    assertEquals(Bytes.toString(COLUMN_FAMILY), colDescArr[0].getNameAsString());
    assertEquals(Bytes.toString(DELETE_COLUMN_FAMILY), colDescArr[1].getNameAsString());
  }
  
  private AsyncAdmin getAsyncAdmin() throws InterruptedException, ExecutionException {
    return AbstractAsyncTest.getAsyncConnection().getAdmin();
  }
  
  @Override
  protected void addColumn(String columnName) throws Exception {
    ColumnFamilyDescriptorBuilder colFamilyDescBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnName));
    asyncAdmin.addColumnFamily(tableName, colFamilyDescBuilder.build()).get();
  }
  
  @Override
  protected void addColumn(String columnName, int version) throws Exception {
    ColumnFamilyDescriptorBuilder colFamilyDescBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnName));
    colFamilyDescBuilder.setMaxVersions(version);
    asyncAdmin.addColumnFamily(tableName, colFamilyDescBuilder.build()).get();
  }
  
  @Override
  protected void modifyColumn(String columnName, int version) throws Exception {
    ColumnFamilyDescriptorBuilder colFamilyDescBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnName));
    colFamilyDescBuilder.setMaxVersions(version);
    asyncAdmin.addColumnFamily(tableName, colFamilyDescBuilder.build()).get();
  }

  @Override
  protected void deleteColumn() throws Exception {
    asyncAdmin.deleteColumnFamily(tableName, DELETE_COLUMN_FAMILY).get();
  }
  
}
