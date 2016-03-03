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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests creation and deletion of column families.
 */
public class TestColumnFamilyAdmin extends AbstractTest {

  private static byte[] DELETE_COLUMN_FAMILY =
      Bytes.toBytes(Bytes.toString(COLUMN_FAMILY) + "_remove");

  private Admin admin;
  private TableName tableName;
  private HTableDescriptor descriptor;

  @Override
  protected void setup() throws IOException {
    admin = getConnection().getAdmin();
    tableName = IntegrationTests.newTestTableName();

    descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    descriptor.addFamily(new HColumnDescriptor(DELETE_COLUMN_FAMILY));
    admin.createTable(descriptor);
  }

  @Override
  protected void tearDown() throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test
  public void testCreateTable() throws IOException {
    HTableDescriptor retrievedDescriptor = admin.getTableDescriptor(tableName);
    Assert.assertEquals(descriptor.getTableName(), retrievedDescriptor.getTableName());
  }

  @Test
  public void testCreateTableFull() throws IOException {
    HTableDescriptor retrievedDescriptor = admin.getTableDescriptor(tableName);
    Assert.assertEquals(descriptor, retrievedDescriptor);
  }

  @Test
  public void testAddColumn() throws IOException {
    HColumnDescriptor newColumn = new HColumnDescriptor("NEW_COLUMN");
    admin.addColumn(tableName, newColumn);
    
    // Make sure this call doesn't fail, but don't actually check the results yet, since Bigtable
    // backend doesn't return column families yet.
    admin.getTableDescriptor(tableName);
    
    HTableDescriptor expectedDescriptor = new HTableDescriptor(descriptor);
    expectedDescriptor.addFamily(newColumn);

    admin.getTableDescriptor(tableName);
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
  public void testRemoveColumn() throws IOException {
    admin.deleteColumn(tableName, DELETE_COLUMN_FAMILY);
    HTableDescriptor retrievedDescriptor = admin.getTableDescriptor(tableName);

    HTableDescriptor expectedDescriptor = new HTableDescriptor(tableName);
    expectedDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

    Assert.assertEquals(expectedDescriptor, retrievedDescriptor);
  }
}
