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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executors;

/**
 * These tests check various factory instantiations of the HTableInterface.
 */
public class TestGetTable extends AbstractTest {
  @Test
  public void testGetTable1() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME);
    Assert.assertEquals(TABLE_NAME, table.getName());
    table.close();
  }

  @Test
  public void testGetTable2() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME.getNameAsString());
    Assert.assertEquals(TABLE_NAME, table.getName());
    table.close();
  }

  @Test
  public void testGetTable3() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME.getName());
    Assert.assertEquals(TABLE_NAME, table.getName());
    table.close();
  }

  @Test
  public void testGetTable4() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME, Executors.newFixedThreadPool(1));
    Assert.assertEquals(TABLE_NAME, table.getName());
    table.close();
  }

  @Test
  public void testGetTable5() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME.getNameAsString(),
        Executors.newFixedThreadPool(1));
    Assert.assertEquals(TABLE_NAME, table.getName());
    table.close();
  }

  @Test
  public void testGetTable6() throws Exception {
    HTableInterface table = connection.getTable(TABLE_NAME.getName(),
        Executors.newFixedThreadPool(1));
    Assert.assertEquals(TABLE_NAME, table.getName());
    table.close();
  }
}
