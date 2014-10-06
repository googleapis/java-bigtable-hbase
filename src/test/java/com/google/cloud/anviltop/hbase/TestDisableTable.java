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
package com.google.cloud.anviltop.hbase;

import static com.google.cloud.anviltop.hbase.IntegrationTests.TABLE_NAME;
import static com.google.cloud.anviltop.hbase.IntegrationTests.COLUMN_FAMILY;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestDisableTable extends AbstractTest {
  /**
   * Requirement 1.8 - Table names must match [\w_][\w_\-\.]*
   */
  @Test
  public void testDisable() throws IOException {
    Admin admin = connection.getAdmin();
    TableName tableName = IntegrationTests.TABLE_NAME;
    Table table = connection.getTable(TABLE_NAME);

    Get get = new Get("row".getBytes());
    table.get(get);

    admin.disableTable(tableName);
    boolean throwsException = false;
    try {
      table.get(get);

    } catch (TableNotEnabledException e) {
      throwsException = true;
    }
    Assert.assertTrue(throwsException);
    
    admin.enableTable(tableName);

    table.get(get);
  }
}
