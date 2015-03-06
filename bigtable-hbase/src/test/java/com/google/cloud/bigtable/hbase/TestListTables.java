/*
 * Copyright (c) 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.IntegrationTests.COLUMN_FAMILY;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

public class TestListTables extends AbstractTest {
  @Test
  public void testTableNames() throws IOException {
    Admin admin = connection.getAdmin();
    TableName tableName1 = TableName.valueOf("list_table1-" + UUID.randomUUID().toString());
    TableName tableName2 = TableName.valueOf("list_table2-" + UUID.randomUUID().toString());

    Assert.assertFalse(admin.tableExists(tableName1));
    Assert.assertFalse(ArrayUtils.contains(admin.listTableNames(), tableName1));

    HTableDescriptor descriptor = new HTableDescriptor(tableName1);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    admin.createTable(descriptor);

    Assert.assertTrue(admin.tableExists(tableName1));
    Assert.assertFalse(admin.tableExists(tableName2));
    Assert.assertTrue(ArrayUtils.contains(admin.listTableNames(), tableName1));
    Assert.assertFalse(ArrayUtils.contains(admin.listTableNames(), tableName2));

    descriptor = new HTableDescriptor(tableName2);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    admin.createTable(descriptor);

    Assert.assertTrue(admin.tableExists(tableName1));
    Assert.assertTrue(admin.tableExists(tableName2));
    Assert.assertTrue(ArrayUtils.contains(admin.listTableNames(), tableName1));
    Assert.assertTrue(ArrayUtils.contains(admin.listTableNames(), tableName2));

    // Clean up: disable (for hbase compatibility) and delete.
    admin.disableTable(tableName1);
    admin.deleteTable(tableName1);
    admin.disableTable(tableName2);
    admin.deleteTable(tableName2);
  }
}
