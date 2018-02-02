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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
  public void testCreate() throws IOException {
    Assert.assertEquals(descriptor, admin.getTableDescriptor(tableName));
  }

  @Test
  public void testAddColumn() throws IOException {
    HColumnDescriptor newColumn = new HColumnDescriptor("NEW_COLUMN");
    admin.addColumn(tableName, newColumn);

    Assert.assertEquals(new HTableDescriptor(descriptor).addFamily(newColumn),
      admin.getTableDescriptor(tableName));
  }

  @Test
  public void testModifyColumn() throws IOException {
    HColumnDescriptor newColumn = new HColumnDescriptor("MODIFY_COLUMN");
    newColumn.setMaxVersions(2);
    admin.addColumn(tableName, newColumn);

    Assert.assertEquals(new HTableDescriptor(descriptor).addFamily(newColumn),
      admin.getTableDescriptor(tableName));

    newColumn.setMaxVersions(100);
    admin.modifyColumn(tableName, newColumn);

    Assert.assertEquals(new HTableDescriptor(descriptor).addFamily(newColumn),
      admin.getTableDescriptor(tableName));
  }

  @Test
  public void testRemoveColumn() throws IOException {
    admin.deleteColumn(tableName, DELETE_COLUMN_FAMILY);
    HTableDescriptor expectedDescriptor = new HTableDescriptor(tableName)
        .addFamily(new HColumnDescriptor(COLUMN_FAMILY));

    Assert.assertEquals(expectedDescriptor, admin.getTableDescriptor(tableName));
  }
}
