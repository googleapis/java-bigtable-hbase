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

import static com.google.cloud.bigtable.hbase.IntegrationTests.*;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

public class TestCreateTable extends AbstractTest {
  /**
   * Requirement 1.8 - Table names must match [_a-zA-Z0-9][-_.a-zA-Z0-9]*
   */
  @Test
  @Category(KnownGap.class)
  public void testTableNames() throws IOException {
    String[] goodNames = {
        "a",
        "1",
        "_", // Really?  Yuck.
        "_x",
        "a-._5x",
        "_a-._5x",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890_-."
    };
    String[] badNames = {
        "-x",
        ".x",
        "a!",
        "a@",
        "a#",
        "a$",
        "a%",
        "a^",
        "a&",
        "a*",
        "a(",
        "a+",
        "a=",
        "a~",
        "a`",
        "a{",
        "a[",
        "a|",
        "a\\",
        "a/",
        "a<",
        "a,",
        "a?",
        "a" + RandomStringUtils.random(10, false, false)
    };

    Admin admin = connection.getAdmin();

    for (String badName : badNames) {
      boolean failed = false;
      try {
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(badName));
        descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
        admin.createTable(descriptor);
      } catch (IllegalArgumentException e) {
        failed = true;
      }
      Assert.assertTrue("Should fail as table name: '" + badName + "'", failed);
    }

    for(String goodName : goodNames) {
      TableName tableName = TableName.valueOf(goodName);
      HTableDescriptor descriptor = new HTableDescriptor(tableName);
      descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
      admin.createTable(descriptor);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }
}
