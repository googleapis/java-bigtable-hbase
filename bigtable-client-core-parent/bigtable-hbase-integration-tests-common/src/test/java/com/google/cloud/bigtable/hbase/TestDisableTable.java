/*
 * Copyright 2015 Google LLC
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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class TestDisableTable extends AbstractTest {
  @Test
  @Category(KnownGap.class)
  // TODO(sduskis): Disabled tables should throw TableNotEnabledException for gets.
  public void testDisable() throws IOException {
    Admin admin = getConnection().getAdmin();
    TableName tableName = sharedTestEnv.newTestTableName();
    sharedTestEnv.createTable(tableName);

    try (Table table = getConnection().getTable(tableName)) {
      Get get = new Get("row".getBytes());
      table.get(get);
      admin.disableTable(tableName);
      Assert.assertTrue(admin.isTableDisabled(tableName));
      try {
        table.get(get);
        Assert.fail("Expected TableNotEnabledException");
      } catch (TableNotEnabledException e) {
      }
      admin.enableTable(tableName);
      table.get(get);
    } finally {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
  }
}
