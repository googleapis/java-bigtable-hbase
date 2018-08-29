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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public abstract class AbstractTestModifyTable extends AbstractTest {
  
  public static final byte[] COLUMN_FAMILY2 = Bytes.toBytes("test_family2");
  
  @Test
  public void testModifyTable() throws IOException {
    TableName tableName = sharedTestEnv.newTestTableName();
    sharedTestEnv.createTable(tableName);
    try(Admin admin = getConnection().getAdmin();
        Table table = getConnection().getTable(tableName)) {
      HTableDescriptor descriptor = new HTableDescriptor(tableName);
      descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY2));
      modifyTable(descriptor);
      assertTrue(admin.tableExists(tableName));
      assertTrue(admin.getTableDescriptor(tableName).hasFamily(COLUMN_FAMILY2));
      assertEquals(1, admin.getTableDescriptor(tableName).getColumnFamilies().length);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }
  
  protected abstract void modifyTable(HTableDescriptor descriptor) throws IOException; 
}
